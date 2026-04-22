import { Hono } from 'hono';
import { verifySignature, LineClient } from '@line-crm/line-sdk';
import type { WebhookRequestBody, WebhookEvent, TextEventMessage } from '@line-crm/line-sdk';
import {
  upsertFriend,
  updateFriendFollowStatus,
  getFriendByLineUserId,
  getScenarios,
  enrollFriendInScenario,
  getScenarioSteps,
  advanceFriendScenario,
  completeFriendScenario,
  upsertChatOnMessage,
  getLineAccounts,
  jstNow,
} from '@line-crm/db';
import { fireEvent } from '../services/event-bus.js';
import { buildMessage, expandVariables } from '../services/step-delivery.js';
import type { Env } from '../index.js';

const webhook = new Hono<Env>();

webhook.post('/webhook', async (c) => {
  const rawBody = await c.req.text();
  const signature = c.req.header('X-Line-Signature') ?? '';
  const db = c.env.DB;

  let body: WebhookRequestBody;
  try {
    body = JSON.parse(rawBody) as WebhookRequestBody;
  } catch {
    console.error('Failed to parse webhook body');
    return c.json({ status: 'ok' }, 200);
  }

  // Multi-account: resolve credentials from DB by destination (channel user ID)
  // or fall back to environment variables (default account)
  let channelSecret = c.env.LINE_CHANNEL_SECRET;
  let channelAccessToken = c.env.LINE_CHANNEL_ACCESS_TOKEN;
  let matchedAccountId: string | null = null;

  if ((body as { destination?: string }).destination) {
    const accounts = await getLineAccounts(db);
    for (const account of accounts) {
      if (!account.is_active) continue;
      const isValid = await verifySignature(account.channel_secret, rawBody, signature);
      if (isValid) {
        channelSecret = account.channel_secret;
        channelAccessToken = account.channel_access_token;
        matchedAccountId = account.id;
        break;
      }
    }
  }

  // Verify with resolved secret
  const valid = await verifySignature(channelSecret, rawBody, signature);
  if (!valid) {
    console.error('Invalid LINE signature');
    return c.json({ status: 'ok' }, 200);
  }

  const lineClient = new LineClient(channelAccessToken);

  // 非同期処理 — LINE は ~1s 以内のレスポンスを要求
  const processingPromise = (async () => {
    for (const event of body.events) {
      try {
        await handleEvent(db, lineClient, event, channelAccessToken, matchedAccountId, c.env.WORKER_URL || new URL(c.req.url).origin, c.env);
      } catch (err) {
        console.error('Error handling webhook event:', err);
      }
    }
  })();

  c.executionCtx.waitUntil(processingPromise);

  return c.json({ status: 'ok' }, 200);
});

async function handleEvent(
  db: D1Database,
  lineClient: LineClient,
  event: WebhookEvent,
  lineAccessToken: string,
  lineAccountId: string | null = null,
  workerUrl?: string,
  env?: Env['Bindings'],
): Promise<void> {
  if (event.type === 'follow') {
    const userId =
      event.source.type === 'user' ? event.source.userId : undefined;
    if (!userId) return;

    // プロフィール取得 & 友だち登録/更新
    let profile;
    try {
      profile = await lineClient.getProfile(userId);
    } catch (err) {
      console.error('Failed to get profile for', userId, err);
    }

    const friend = await upsertFriend(db, {
      lineUserId: userId,
      displayName: profile?.displayName ?? null,
      pictureUrl: profile?.pictureUrl ?? null,
      statusMessage: profile?.statusMessage ?? null,
    });

    // Set line_account_id for multi-account tracking
    if (lineAccountId) {
      await db.prepare('UPDATE friends SET line_account_id = ? WHERE id = ? AND line_account_id IS NULL')
        .bind(lineAccountId, friend.id).run();
    }

    // friend_add シナリオに登録（このアカウントのシナリオのみ）
    const scenarios = await getScenarios(db);
    for (const scenario of scenarios) {
      // Only trigger scenarios belonging to this account (or unassigned for backward compat)
      const scenarioAccountMatch = !scenario.line_account_id || !lineAccountId || scenario.line_account_id === lineAccountId;
      if (scenario.trigger_type === 'friend_add' && scenario.is_active && scenarioAccountMatch) {
        try {
          const existing = await db
            .prepare(`SELECT id FROM friend_scenarios WHERE friend_id = ? AND scenario_id = ?`)
            .bind(friend.id, scenario.id)
            .first<{ id: string }>();
          if (!existing) {
            const friendScenario = await enrollFriendInScenario(db, friend.id, scenario.id);

            // Immediate delivery: if the first step has delay=0, send it now via replyMessage (free)
            const steps = await getScenarioSteps(db, scenario.id);
            const firstStep = steps[0];
            if (firstStep && firstStep.delay_minutes === 0 && friendScenario.status === 'active') {
              try {
                const expandedContent = expandVariables(firstStep.message_content, friend as { id: string; display_name: string | null; user_id: string | null });
                const message = buildMessage(firstStep.message_type, expandedContent);
                await lineClient.replyMessage(event.replyToken, [message]);
                console.log(`Immediate delivery: sent step ${firstStep.id} to ${userId}`);

                // Log outgoing message (replyMessage = 無料)
                const logId = crypto.randomUUID();
                await db
                  .prepare(
                    `INSERT INTO messages_log (id, friend_id, direction, message_type, content, broadcast_id, scenario_step_id, delivery_type, created_at)
                     VALUES (?, ?, 'outgoing', ?, ?, NULL, ?, 'reply', ?)`,
                  )
                  .bind(logId, friend.id, firstStep.message_type, firstStep.message_content, firstStep.id, jstNow())
                  .run();

                // Advance or complete the friend_scenario
                const secondStep = steps[1] ?? null;
                if (secondStep) {
                  const nextDeliveryDate = new Date(Date.now() + 9 * 60 * 60_000);
                  nextDeliveryDate.setMinutes(nextDeliveryDate.getMinutes() + secondStep.delay_minutes);
                  // Enforce 9:00-21:00 JST delivery window
                  const h = nextDeliveryDate.getUTCHours();
                  if (h < 9 || h >= 21) {
                    if (h >= 21) nextDeliveryDate.setUTCDate(nextDeliveryDate.getUTCDate() + 1);
                    nextDeliveryDate.setUTCHours(9, 0, 0, 0);
                  }
                  await advanceFriendScenario(db, friendScenario.id, firstStep.step_order, nextDeliveryDate.toISOString().slice(0, -1) + '+09:00');
                } else {
                  await completeFriendScenario(db, friendScenario.id);
                }
              } catch (err) {
                console.error('Failed immediate delivery for scenario', scenario.id, err);
              }
            }
          }
        } catch (err) {
          console.error('Failed to enroll friend in scenario', scenario.id, err);
        }
      }
    }

    // イベントバス発火: friend_add（replyToken は Step 0 で使用済みの可能性あり）
    await fireEvent(db, 'friend_add', { friendId: friend.id, eventData: { displayName: friend.display_name } }, lineAccessToken, lineAccountId);
    return;
  }

  if (event.type === 'unfollow') {
    const userId =
      event.source.type === 'user' ? event.source.userId : undefined;
    if (!userId) return;

    await updateFriendFollowStatus(db, userId, false);
    return;
  }

  // 予約キャンセル用postback処理
  if ((event as { type: string }).type === 'postback') {
    const postbackEvent = event as unknown as {
      replyToken: string;
      source: { type: string; userId?: string };
      postback: { data: string };
    };
    const userId = postbackEvent.source.type === 'user' ? postbackEvent.source.userId : undefined;
    if (!userId) return;

    let friend = await getFriendByLineUserId(db, userId);
    if (!friend) {
      try {
        const profile = await lineClient.getProfile(userId);
        friend = await upsertFriend(db, {
          lineUserId: userId,
          displayName: profile?.displayName ?? null,
          pictureUrl: profile?.pictureUrl ?? null,
          statusMessage: profile?.statusMessage ?? null,
        });
      } catch (err) {
        console.error('postback: auto-register friend failed', err);
        return;
      }
    }

    const data = postbackEvent.postback.data;
    if (data.startsWith('cancel_reservation:')) {
      const submissionId = data.slice('cancel_reservation:'.length);
      try {
        const row = await db
          .prepare('SELECT id, friend_id, data FROM form_submissions WHERE id = ?')
          .bind(submissionId)
          .first<{ id: string; friend_id: string; data: string }>();

        if (!row || row.friend_id !== friend.id) {
          await lineClient.replyMessage(postbackEvent.replyToken, [
            buildMessage('text', '予約が見つかりませんでした。'),
          ]);
          return;
        }

        const subData = JSON.parse(row.data || '{}') as Record<string, unknown>;
        if (subData._cancelled_at) {
          await lineClient.replyMessage(postbackEvent.replyToken, [
            buildMessage('text', 'この予約はすでにキャンセル済みです。'),
          ]);
          return;
        }

        const eventIds = Array.isArray(subData._calendar_event_ids)
          ? (subData._calendar_event_ids as string[])
          : [];

        // Google Calendarから削除
        let deletedCount = 0;
        if (eventIds.length > 0 && env?.GOOGLE_SERVICE_ACCOUNT_KEY && env?.GOOGLE_CALENDAR_ID) {
          try {
            const { getGoogleAccessToken } = await import('../services/google-auth.js');
            const { GoogleCalendarClient } = await import('../services/google-calendar.js');
            const accessToken = await getGoogleAccessToken(env.GOOGLE_SERVICE_ACCOUNT_KEY);
            const calClient = new GoogleCalendarClient({
              calendarId: env.GOOGLE_CALENDAR_ID,
              accessToken,
            });
            for (const eid of eventIds) {
              try {
                await calClient.deleteEvent(eid);
                deletedCount++;
              } catch (err) {
                console.warn(`Google Calendar event ${eid} delete failed:`, err);
              }
            }
          } catch (err) {
            console.error('Google Calendar auth/setup failed for cancel:', err);
          }
        }

        // submission data に _cancelled_at を記録
        const cancelledData = { ...subData, _cancelled_at: jstNow() };
        await db
          .prepare('UPDATE form_submissions SET data = ? WHERE id = ?')
          .bind(JSON.stringify(cancelledData), submissionId)
          .run();

        const visitDate = String(subData.visit_date ?? '');
        const rawTime = subData.visit_time;
        const timeStr = Array.isArray(rawTime) ? rawTime.join(', ') : String(rawTime ?? '');

        await lineClient.replyMessage(postbackEvent.replyToken, [
          buildMessage('flex', JSON.stringify({
            type: 'bubble', size: 'mega',
            header: {
              type: 'box', layout: 'vertical',
              contents: [
                { type: 'text', text: 'キャンセル完了', size: 'lg', weight: 'bold', color: '#1e293b' },
              ],
              paddingAll: '16px', backgroundColor: '#fef2f2',
            },
            body: {
              type: 'box', layout: 'vertical', spacing: 'sm',
              contents: [
                { type: 'text', text: `${visitDate} のご予約をキャンセルしました。`, size: 'sm', color: '#1e293b', wrap: true },
                { type: 'text', text: timeStr, size: 'xs', color: '#64748b', margin: 'sm' },
                { type: 'separator', margin: 'lg' },
                { type: 'text', text: `Googleカレンダーから${deletedCount}件のイベントを削除しました。`, size: 'xxs', color: '#64748b', margin: 'lg', wrap: true },
                { type: 'text', text: 'またのご予約をお待ちしております🙏', size: 'xs', color: '#1e293b', margin: 'md', wrap: true },
              ],
              paddingAll: '16px',
            },
          }), 'キャンセル完了'),
        ]);
      } catch (err) {
        console.error('予約キャンセル処理エラー:', err);
        try {
          await lineClient.replyMessage(postbackEvent.replyToken, [
            buildMessage('text', 'キャンセル処理中にエラーが発生しました。お手数ですが直接ご連絡ください。'),
          ]);
        } catch { /* silent */ }
      }
    }
    return;
  }

  if (event.type === 'message' && event.message.type === 'text') {
    const textMessage = event.message as TextEventMessage;
    const userId =
      event.source.type === 'user' ? event.source.userId : undefined;
    if (!userId) return;

    let friend = await getFriendByLineUserId(db, userId);
    if (!friend) {
      // 未登録ならプロフィール取得して自動登録（Webhook設定前から友だちだった人の救済）
      try {
        const profile = await lineClient.getProfile(userId);
        friend = await upsertFriend(db, {
          lineUserId: userId,
          displayName: profile?.displayName ?? null,
          pictureUrl: profile?.pictureUrl ?? null,
          statusMessage: profile?.statusMessage ?? null,
        });
        if (lineAccountId) {
          await db.prepare('UPDATE friends SET line_account_id = ? WHERE id = ? AND line_account_id IS NULL')
            .bind(lineAccountId, friend.id).run();
        }
      } catch (err) {
        console.error('Failed to auto-register friend from message event:', err);
        return;
      }
    }

    // 予約をキャンセル: キャンセル対象の予約一覧をFlex Messageで表示
    if ((event.message as TextEventMessage).text === '予約をキャンセル') {
      try {
        const today = new Date();
        const todayStr = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`;
        const rows = await db
          .prepare(
            `SELECT id, data, created_at FROM form_submissions
             WHERE friend_id = ?
               AND (json_extract(data, '$._cancelled_at') IS NULL)
               AND (json_extract(data, '$.visit_date') >= ?)
             ORDER BY json_extract(data, '$.visit_date') ASC, created_at DESC
             LIMIT 10`,
          )
          .bind(friend.id, todayStr)
          .all<{ id: string; data: string; created_at: string }>();

        const submissions = rows.results.map((r) => {
          const data = JSON.parse(r.data || '{}') as Record<string, unknown>;
          return { id: r.id, data };
        });

        if (submissions.length === 0) {
          await lineClient.replyMessage(event.replyToken, [
            buildMessage('text', 'キャンセル可能な予約が見つかりません。\n(今後の日程で未キャンセルの予約のみ表示されます)'),
          ]);
          return;
        }

        // Flex Carousel で予約一覧表示、各予約にキャンセルボタン
        const bubbles = submissions.map((sub) => {
          const d = sub.data;
          const visitDate = String(d.visit_date ?? '');
          const rawTime = d.visit_time;
          const timeStr = Array.isArray(rawTime) ? rawTime.join(', ') : String(rawTime ?? '');
          const company = String(d.company_name ?? '');
          const contact = String(d.contact_name ?? '');
          const numVisitors = String(d.num_visitors ?? '');
          return {
            type: 'bubble',
            size: 'mega',
            header: {
              type: 'box', layout: 'vertical',
              contents: [
                { type: 'text', text: visitDate, size: 'lg', weight: 'bold', color: '#1e293b' },
                { type: 'text', text: timeStr, size: 'sm', color: '#64748b', margin: 'sm', wrap: true },
              ],
              paddingAll: '16px', backgroundColor: '#e0f2fe',
            },
            body: {
              type: 'box', layout: 'vertical', spacing: 'sm',
              contents: [
                { type: 'box', layout: 'baseline', contents: [
                  { type: 'text', text: '会社', size: 'xs', color: '#64748b', flex: 2 },
                  { type: 'text', text: company || '-', size: 'sm', color: '#1e293b', flex: 5, wrap: true },
                ]},
                { type: 'box', layout: 'baseline', contents: [
                  { type: 'text', text: '担当', size: 'xs', color: '#64748b', flex: 2 },
                  { type: 'text', text: contact || '-', size: 'sm', color: '#1e293b', flex: 5, wrap: true },
                ]},
                { type: 'box', layout: 'baseline', contents: [
                  { type: 'text', text: '人数', size: 'xs', color: '#64748b', flex: 2 },
                  { type: 'text', text: numVisitors || '-', size: 'sm', color: '#1e293b', flex: 5 },
                ]},
              ],
              paddingAll: '16px',
            },
            footer: {
              type: 'box', layout: 'vertical', paddingAll: '12px',
              contents: [
                { type: 'button',
                  action: { type: 'postback', label: 'この予約をキャンセル', data: `cancel_reservation:${sub.id}`, displayText: `${visitDate} の予約をキャンセル` },
                  style: 'primary', color: '#dc2626' },
              ],
            },
          };
        });

        const carousel = { type: 'carousel', contents: bubbles };
        await lineClient.replyMessage(event.replyToken, [
          buildMessage('flex', JSON.stringify(carousel), 'キャンセル可能な予約一覧'),
        ]);
      } catch (err) {
        console.error('予約キャンセル一覧表示エラー:', err);
      }
      return;
    }

    const incomingText = textMessage.text;
    const now = jstNow();
    const logId = crypto.randomUUID();

    // 受信メッセージをログに記録
    await db
      .prepare(
        `INSERT INTO messages_log (id, friend_id, direction, message_type, content, broadcast_id, scenario_step_id, created_at)
         VALUES (?, ?, 'incoming', 'text', ?, NULL, NULL, ?)`,
      )
      .bind(logId, friend.id, incomingText, now)
      .run();

    // チャットを作成/更新（ユーザーの自発的メッセージのみ unread にする）
    // ボタンタップ等の自動応答キーワードは除外
    const autoKeywords = ['料金', '機能', 'API', 'フォーム', 'ヘルプ', 'UUID', 'UUID連携について教えて', 'UUID連携を確認', '配信時間', '導入支援を希望します', 'アカウント連携を見る', '体験を完了する', 'BAN対策を見る', '連携確認'];
    const isAutoKeyword = autoKeywords.some(k => incomingText === k);
    const isTimeCommand = /(?:配信時間|配信|届けて|通知)[はを]?\s*\d{1,2}\s*時/.test(incomingText);
    if (!isAutoKeyword && !isTimeCommand) {
      await upsertChatOnMessage(db, friend.id);
    }

    // 配信時間設定: 「配信時間は○時」「○時に届けて」等のパターンを検出
    const timeMatch = incomingText.match(/(?:配信時間|配信|届けて|通知)[はを]?\s*(\d{1,2})\s*時/);
    if (timeMatch) {
      const hour = parseInt(timeMatch[1], 10);
      if (hour >= 6 && hour <= 22) {
        // Save preferred_hour to friend metadata
        const existing = await db.prepare('SELECT metadata FROM friends WHERE id = ?').bind(friend.id).first<{ metadata: string }>();
        const meta = JSON.parse(existing?.metadata || '{}');
        meta.preferred_hour = hour;
        await db.prepare('UPDATE friends SET metadata = ?, updated_at = ? WHERE id = ?')
          .bind(JSON.stringify(meta), jstNow(), friend.id).run();

        // Reply with confirmation
        try {
          const period = hour < 12 ? '午前' : '午後';
          const displayHour = hour <= 12 ? hour : hour - 12;
          await lineClient.replyMessage(event.replyToken, [
            buildMessage('flex', JSON.stringify({
              type: 'bubble',
              body: { type: 'box', layout: 'vertical', contents: [
                { type: 'text', text: '配信時間を設定しました', size: 'lg', weight: 'bold', color: '#1e293b' },
                { type: 'box', layout: 'vertical', contents: [
                  { type: 'text', text: `${period} ${displayHour}:00`, size: 'xxl', weight: 'bold', color: '#f59e0b', align: 'center' },
                  { type: 'text', text: `（${hour}:00〜）`, size: 'sm', color: '#64748b', align: 'center', margin: 'sm' },
                ], backgroundColor: '#fffbeb', cornerRadius: 'md', paddingAll: '20px', margin: 'lg' },
                { type: 'text', text: '今後のステップ配信はこの時間以降にお届けします。', size: 'xs', color: '#64748b', wrap: true, margin: 'lg' },
              ], paddingAll: '20px' },
            })),
          ]);
        } catch (err) {
          console.error('Failed to reply for time setting', err);
        }
        return;
      }
    }

    // Cross-account trigger: send message from another account via UUID
    if (incomingText === '体験を完了する' && lineAccountId) {
      try {
        const friendRecord = await db.prepare('SELECT user_id FROM friends WHERE id = ?').bind(friend.id).first<{ user_id: string | null }>();
        if (friendRecord?.user_id) {
          // Find the same user on other accounts
          const otherFriends = await db.prepare(
            'SELECT f.line_user_id, la.channel_access_token FROM friends f INNER JOIN line_accounts la ON la.id = f.line_account_id WHERE f.user_id = ? AND f.line_account_id != ? AND f.is_following = 1'
          ).bind(friendRecord.user_id, lineAccountId).all<{ line_user_id: string; channel_access_token: string }>();

          for (const other of otherFriends.results) {
            const otherClient = new LineClient(other.channel_access_token);
            const { buildMessage: bm } = await import('../services/step-delivery.js');
            await otherClient.pushMessage(other.line_user_id, [bm('flex', JSON.stringify({
              type: 'bubble', size: 'giga',
              header: { type: 'box', layout: 'vertical', paddingAll: '20px', backgroundColor: '#fffbeb',
                contents: [{ type: 'text', text: `${friend.display_name || ''}さんへ`, size: 'lg', weight: 'bold', color: '#1e293b' }],
              },
              body: { type: 'box', layout: 'vertical', paddingAll: '20px',
                contents: [
                  { type: 'text', text: '別アカウントからのアクションを検知しました。', size: 'sm', color: '#06C755', weight: 'bold', wrap: true },
                  { type: 'text', text: 'アカウント連携が正常に動作しています。体験ありがとうございました。', size: 'sm', color: '#1e293b', wrap: true, margin: 'md' },
                  { type: 'separator', margin: 'lg' },
                  { type: 'text', text: 'ステップ配信・フォーム即返信・アカウント連携・リッチメニュー・自動返信 — 全て無料、全てOSS。', size: 'xs', color: '#64748b', wrap: true, margin: 'lg' },
                ],
              },
              footer: { type: 'box', layout: 'vertical', paddingAll: '16px',
                contents: [
                  { type: 'button', action: { type: 'message', label: '導入について相談する', text: '導入支援を希望します' }, style: 'primary', color: '#06C755' },
                  ...(c.env.LIFF_URL ? [{ type: 'button', action: { type: 'uri', label: 'フィードバックを送る', uri: `${c.env.LIFF_URL}?page=form` }, style: 'secondary', margin: 'sm' }] : []),
                ],
              },
            }))]);
          }

          // Reply on Account ② confirming
          await lineClient.replyMessage(event.replyToken, [buildMessage('flex', JSON.stringify({
            type: 'bubble',
            body: { type: 'box', layout: 'vertical', paddingAll: '20px',
              contents: [
                { type: 'text', text: 'Account ① にメッセージを送りました', size: 'sm', color: '#06C755', weight: 'bold', align: 'center' },
                { type: 'text', text: 'Account ① のトーク画面を確認してください', size: 'xs', color: '#64748b', align: 'center', margin: 'md' },
              ],
            },
          }))]);
          return;
        }
      } catch (err) {
        console.error('Cross-account trigger error:', err);
      }
    }

    // 自動返信チェック（このアカウントのルール + グローバルルールのみ）
    // NOTE: Auto-replies use replyMessage (free, no quota) instead of pushMessage
    // The replyToken is only valid for ~1 minute after the message event
    const autoReplyQuery = lineAccountId
      ? `SELECT * FROM auto_replies WHERE is_active = 1 AND (line_account_id IS NULL OR line_account_id = ?) ORDER BY created_at ASC`
      : `SELECT * FROM auto_replies WHERE is_active = 1 AND line_account_id IS NULL ORDER BY created_at ASC`;
    const autoReplyStmt = db.prepare(autoReplyQuery);
    const autoReplies = await (lineAccountId ? autoReplyStmt.bind(lineAccountId) : autoReplyStmt)
      .all<{
        id: string;
        keyword: string;
        match_type: 'exact' | 'contains';
        response_type: string;
        response_content: string;
        is_active: number;
        created_at: string;
      }>();

    let matched = false;
    let replyTokenConsumed = false;
    for (const rule of autoReplies.results) {
      const isMatch =
        rule.match_type === 'exact'
          ? incomingText === rule.keyword
          : incomingText.includes(rule.keyword);

      if (isMatch) {
        try {
          // Expand template variables ({{name}}, {{uid}}, {{auth_url:CHANNEL_ID}})
          const expandedContent = expandVariables(rule.response_content, friend as { id: string; display_name: string | null; user_id: string | null }, workerUrl);
          const replyMsg = buildMessage(rule.response_type, expandedContent);
          await lineClient.replyMessage(event.replyToken, [replyMsg]);
          replyTokenConsumed = true;

          // 送信ログ（replyMessage = 無料）
          const outLogId = crypto.randomUUID();
          await db
            .prepare(
              `INSERT INTO messages_log (id, friend_id, direction, message_type, content, broadcast_id, scenario_step_id, delivery_type, created_at)
               VALUES (?, ?, 'outgoing', ?, ?, NULL, NULL, 'reply', ?)`,
            )
            .bind(outLogId, friend.id, rule.response_type, rule.response_content, jstNow())
            .run();
        } catch (err) {
          console.error('Failed to send auto-reply', err);
          // replyToken may still be unused if replyMessage threw before LINE accepted it
        }

        matched = true;
        break;
      }
    }

    // イベントバス発火: message_received
    // Pass replyToken only when auto_reply didn't actually consume it
    await fireEvent(db, 'message_received', {
      friendId: friend.id,
      eventData: { text: incomingText, matched },
      replyToken: replyTokenConsumed ? undefined : event.replyToken,
    }, lineAccessToken, lineAccountId);

    return;
  }
}

export { webhook };
