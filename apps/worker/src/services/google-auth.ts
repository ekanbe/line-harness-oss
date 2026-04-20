// Google service account authentication (JWT → access token)

interface ServiceAccountKey {
  client_email: string;
  private_key: string;
  token_uri?: string;
}

const TOKEN_URI = 'https://oauth2.googleapis.com/token';
const SCOPE = 'https://www.googleapis.com/auth/calendar';

// Simple in-memory token cache (reset per Worker isolate).
let cachedToken: { token: string; expiresAt: number } | null = null;

function base64urlEncode(input: ArrayBuffer | string): string {
  const bytes =
    typeof input === 'string'
      ? new TextEncoder().encode(input)
      : new Uint8Array(input);
  let bin = '';
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function pemToArrayBuffer(pem: string): ArrayBuffer {
  // Extract content between BEGIN/END markers if present
  const match = pem.match(/-----BEGIN [^-]+-----([\s\S]*?)-----END [^-]+-----/);
  const body = match ? match[1] : pem;
  // Keep only valid base64 characters (handles \n, \r, literal \\n, spaces, etc.)
  const cleaned = body.replace(/[^A-Za-z0-9+/=]/g, '');
  if (!cleaned) throw new Error('Invalid PEM: no base64 content found');
  const binary = atob(cleaned);
  const buf = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) buf[i] = binary.charCodeAt(i);
  return buf.buffer;
}

async function signJWT(account: ServiceAccountKey): Promise<string> {
  const header = { alg: 'RS256', typ: 'JWT' };
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    iss: account.client_email,
    scope: SCOPE,
    aud: account.token_uri ?? TOKEN_URI,
    exp: now + 3600,
    iat: now,
  };

  const signingInput =
    `${base64urlEncode(JSON.stringify(header))}.${base64urlEncode(JSON.stringify(payload))}`;

  const keyBuffer = pemToArrayBuffer(account.private_key);
  const key = await crypto.subtle.importKey(
    'pkcs8',
    keyBuffer,
    { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
    false,
    ['sign'],
  );

  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    key,
    new TextEncoder().encode(signingInput),
  );

  return `${signingInput}.${base64urlEncode(signature)}`;
}

export async function getGoogleAccessToken(serviceAccountJson: string): Promise<string> {
  const nowMs = Date.now();
  if (cachedToken && cachedToken.expiresAt - 60_000 > nowMs) {
    return cachedToken.token;
  }

  const account = JSON.parse(serviceAccountJson) as ServiceAccountKey;
  const jwt = await signJWT(account);

  const res = await fetch(account.token_uri ?? TOKEN_URI, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: jwt,
    }).toString(),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`Google token request failed ${res.status}: ${text}`);
  }

  const data = (await res.json()) as { access_token: string; expires_in: number };
  cachedToken = {
    token: data.access_token,
    expiresAt: nowMs + data.expires_in * 1000,
  };
  return data.access_token;
}
