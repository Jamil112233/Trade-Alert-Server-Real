/**
 * Trade Alert — Combined Server (Bandwidth-Optimized)
 * Runs 24/7 on Render (Web Service, port required).
 *
 * Replaces:  price-bridge.js  +  Cloudflare Worker
 *
 * Flow:
 *   1. Capital.com WebSocket  → live XAU/XAG prices + OHLC candles (only if XAU/XAG alert active)
 *   2. Gate.io WebSocket      → live crypto prices (only subscribed pairs that have active alerts)
 *   3. Yahoo Finance polling  → indices + forex every 20s (only pairs with active alerts, market hours only)
 *   4. RTDB listener          → watches alerts node, maintains live alert cache
 *                               triggers Gate.io resubscribe when alert set changes
 *   5. Price checker          → every second, checks all cached alerts vs live prices
 *   6. On trigger             → FCM → delete RTDB alert → update Firestore
 *   7. Indicator engine       → EMA alerts (Section 12): history once per pair+timeframe, then 1 tiny
 *                               candle fetch per close; Touch checked every second from memory
 *
 * Bandwidth optimizations:
 *   - Gate.io WS: subscribes only to pairs that have at least one active alert
 *   - Yahoo: polls only symbols with active alerts, skips closed markets by session hours
 *   - Yahoo: 20s interval instead of 10s
 *   - Gate.io candle REST: only fetches pairs with active candle-close alerts
 *   - Weekend mode: forex/indices markets are closed Sat/Sun — skip Yahoo entirely
 *   - Capital.com WS: already limited to GOLD + SILVER only
 *
 * Environment variables (set in Render dashboard):
 *   CAP_EMAIL, CAP_PASSWORD, CAP_API_KEY         — Capital.com credentials
 *   FIREBASE_URL                                  — RTDB URL (asia-southeast1)
 *   FIREBASE_SECRET                               — RTDB database secret
 *   FIREBASE_PROJECT_ID                           — e.g. tradealert-2602c
 *   FIREBASE_SERVICE_ACCOUNT                      — full service account JSON string
 *   PORT                                          — set by Render automatically
 */

'use strict';

const https     = require('https');
const http      = require('http');
const WebSocket = require('ws');

// ── Environment ──────────────────────────────────────────────────────────────
const CAP_EMAIL    = process.env.CAP_EMAIL;
const CAP_PASSWORD = process.env.CAP_PASSWORD;
const CAP_API_KEY  = process.env.CAP_API_KEY;
// Strip trailing slash(es) — every call site below does `${FIREBASE_URL}/path`,
// so a trailing slash in the env var silently produces "//path" which Firebase
// treats as a different (empty) path. This caused a real production bug where
// RTDB connected successfully (200) but returned zero alerts.
const FIREBASE_URL = (process.env.FIREBASE_URL || '').replace(/\/+$/, '');       // RTDB base URL
const FIREBASE_SECRET = process.env.FIREBASE_SECRET; // RTDB secret for writes
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID;
const SERVICE_ACCOUNT_JSON = process.env.FIREBASE_SERVICE_ACCOUNT;

const CAP_REST_URL  = 'https://api-capital.backend-capital.com';
const CAP_WS_URL    = 'wss://api-streaming-capital.backend-capital.com/connect';

// ── Brevo SMTP alert emails ───────────────────────────────────────────────────
// Set these in Render dashboard → Environment
const BREVO_USER    = process.env.BREVO_USER;
const BREVO_KEY     = process.env.BREVO_KEY;
const ALERT_TO      = process.env.ALERT_TO      || 'dev.dreamlabs.org@gmail.com';
const ALERT_FROM    = process.env.ALERT_FROM    || 'support.dreamlabs@gmail.com';

const DEV_EMAIL = 'dev.dreamlabs.org@gmail.com';

// ── Logging ──────────────────────────────────────────────────────────────────
function log(msg)  { console.log(`[${new Date().toISOString()}] ${msg}`); }
function warn(msg) { console.warn(`[${new Date().toISOString()}] ⚠️  ${msg}`); }
function err(msg)  { console.error(`[${new Date().toISOString()}] ❌ ${msg}`); }

// ── Server alert emails (Brevo SMTP) ─────────────────────────────────────────
// Cooldown: each issue key can only send 1 email per 30 minutes to prevent spam
// during retry loops or repeated failures.
const emailCooldowns = {};
const EMAIL_COOLDOWN_MS = 30 * 60 * 1000; // 30 minutes

function sendAlertEmail(issueKey, subject, body) {
  if (!BREVO_KEY) return; // env var not set — skip silently
  const now = Date.now();
  if (emailCooldowns[issueKey] && now - emailCooldowns[issueKey] < EMAIL_COOLDOWN_MS) return;
  emailCooldowns[issueKey] = now;

  const ts       = new Date().toISOString();
  const fullBody = `${body}\n\nTime: ${ts}\nServer: TradeAlert Render`;

  // Use Brevo HTTP API (port 443) — SMTP ports 587/465 are blocked on Render free plan
  const payload = JSON.stringify({
    sender:   { name: 'TradeAlert Server', email: ALERT_FROM },
    to:       [{ email: ALERT_TO }],
    subject:  `[TradeAlert] ${subject}`,
    textContent: fullBody
  });

  const req = https.request({
    hostname: 'api.brevo.com',
    port:     443,
    path:     '/v3/smtp/email',
    method:   'POST',
    headers:  {
      'api-key':       BREVO_KEY,
      'Content-Type':  'application/json',
      'Content-Length': Buffer.byteLength(payload)
    }
  }, res => {
    let d = '';
    res.on('data', c => d += c);
    res.on('end', () => {
      if (res.statusCode >= 200 && res.statusCode < 300) {
        log(`Alert email sent: ${subject}`);
      } else {
        warn(`Alert email API error ${res.statusCode}: ${d}`);
      }
    });
  });
  req.on('error', e => warn(`Alert email request error: ${e.message}`));
  req.write(payload);
  req.end();
}


// Updated continuously by WebSocket handlers and Yahoo polling
const livePrice = {
  // Metals (from Capital.com WebSocket)
  XAU: 0, XAG: 0,
  // Crypto (from MEXC WebSocket)
  BTC:0, ETH:0, BNB:0, SOL:0, XRP:0, ADA:0, DOGE:0, AVAX:0,
  DOT:0, MATIC:0, LINK:0, UNI:0, ATOM:0, LTC:0, BCH:0, NEAR:0,
  ARB:0, OP:0, SHIB:0, TRX:0,
  SUI:0, APT:0, PEPE:0, ICP:0, INJ:0, FIL:0, HBAR:0,
  // Indices + Forex (from Yahoo polling)
  SPX500:0, US30:0, US100:0, DXY:0, NIF50:0,
  EURUSD:0, GBPUSD:0, USDJPY:0, GBPJPY:0, AUDUSD:0, USDGBP:0, // USDGBP derived as 1/GBPUSD
  USDCAD:0, USDCHF:0, NZDUSD:0, EURJPY:0,
};

// M1 OHLC for miss-hit detection (previous closed candle high/low)
// Updated every minute from WebSocket data or candle builds
const m1Candle = {}; // { [sym]: { high, low, close, openTime } }

// Current open M1 candles being built from ticks
const openCandle = {}; // { [sym]: { open, high, low, startMin } }

// ── Alert cache (from RTDB listener) ─────────────────────────────────────────
// Key = alertId, Value = alert object
const activeAlerts = {};

// Track recently triggered to prevent double-fire
const recentlyTriggered = new Set();

// Repeating PRICE alerts: after a restart/deploy the side stored in RTDB can be stale (crossings
// consumed by cooldown were never persisted, app edits overwrite the node without it), which made
// old alerts fire on boot. Alerts created before this process started therefore re-learn their
// side silently on their first evaluation. REP_SEEN = ids already evaluated in this process.
const SERVER_BOOT_MS = Date.now();
const REP_SEEN = new Set();

// Yahoo consecutive fail counter (for email alert)
let yahooFailCount = 0;

// Gate.io WS disconnect counter (for email alert)
let gateWsDisconnects = 0;

// ── Active pair tracking ──────────────────────────────────────────────────────
// Derived from activeAlerts — updated whenever RTDB pushes alert changes.
// Gate.io WS subscribes ONLY to pairs in this set; Yahoo polls ONLY these.
// Populated after GATE_SYMBOLS / YAHOO_SYMBOLS are defined (below Section 5/6).
const GATE_SYMBOLS_SET   = new Set(); // filled at startup
const YAHOO_SYMBOLS_KEYS = new Set(); // filled at startup

function getActiveCryptoPairs() {
  const pairs = new Set();
  for (const alert of Object.values(activeAlerts)) {
    if (GATE_SYMBOLS_SET.has(alert.pairSymbol)) pairs.add(alert.pairSymbol);
  }
  return pairs;
}

function getActiveYahooPairs() {
  const pairs = new Set();
  for (const alert of Object.values(activeAlerts)) {
    if (YAHOO_SYMBOLS_KEYS.has(alert.pairSymbol)) pairs.add(alert.pairSymbol);
  }
  return pairs;
}

// Crypto pairs that need candle REST fetches at minute boundary for a given timeframe
function getActiveCandleCloseCryptoPairs(tf) {
  const pairs = new Set();
  for (const alert of Object.values(activeAlerts)) {
    if (alert.candleClose && alert.alertKind !== 'indicator' && alert.timeframe === tf && GATE_SYMBOLS_SET.has(alert.pairSymbol)) {
      pairs.add(alert.pairSymbol);
    }
  }
  return pairs;
}

// ── Market hours helpers ──────────────────────────────────────────────────────
// All times in UTC. Returns true if the market for that symbol is currently open.

const INDEX_HOURS_UTC = {
  // NYSE/NASDAQ: Mon-Fri 13:30–20:00 UTC
  SPX500: { days: [1,2,3,4,5], open: 13*60+30, close: 20*60 },
  US30:   { days: [1,2,3,4,5], open: 13*60+30, close: 20*60 },
  US100:  { days: [1,2,3,4,5], open: 13*60+30, close: 20*60 },
  // DXY: Mon-Fri 00:00–21:00 UTC (ICE)
  DXY:    { days: [1,2,3,4,5], open: 0,         close: 21*60 },
  // NSE India: Mon-Fri 03:45–10:00 UTC
  NIF50:  { days: [1,2,3,4,5], open: 3*60+45,  close: 10*60 },
};

const FOREX_PAIRS = new Set(['EURUSD','GBPUSD','USDJPY','GBPJPY','AUDUSD','USDGBP',
  'USDCAD','USDCHF','NZDUSD','EURJPY']);

function isWeekend() {
  const day = new Date().getUTCDay(); // 0=Sun, 6=Sat
  return day === 0 || day === 6;
}

// Forex/metals weekly session: closed Sat all day, closed Sun until 20:00 UTC
// (2hr earlier than the standard 22:00 UTC open — errs early so we never miss
// the volatile open when brokers shift by 1hr for DST), open Mon-Thu all day,
// closed Fri from 21:00 UTC onward.
function isForexOpen() {
  const now  = new Date();
  const day  = now.getUTCDay(); // 0=Sun ... 6=Sat
  const mins = now.getUTCHours() * 60 + now.getUTCMinutes();

  if (day === 6) return false;                 // Saturday — fully closed
  if (day === 0) return mins >= 20*60;          // Sunday — closed until 20:00 UTC
  if (day === 5 && mins >= 21*60) return false; // Friday after 21:00 UTC — closed
  return true;                                  // Mon-Thu, and Fri before 21:00 UTC
}

// XAU/XAG follow the same weekly trading session as forex (Capital.com).
function isMetalsOpen() {
  return isForexOpen();
}

function isIndexOpen(sym) {
  if (isWeekend()) return false;
  const h = INDEX_HOURS_UTC[sym];
  if (!h) return false;
  const now  = new Date();
  const day  = now.getUTCDay();
  const mins = now.getUTCHours() * 60 + now.getUTCMinutes();
  return h.days.includes(day) && mins >= h.open && mins < h.close;
}

function isYahooSymbolOpen(sym) {
  if (FOREX_PAIRS.has(sym)) return isForexOpen();
  return isIndexOpen(sym);
}

// ── OAuth token cache ────────────────────────────────────────────────────────
let _oauthToken = null;
let _tokenExpiry = 0;

// ════════════════════════════════════════════════════════════════════════════
// SECTION 1 — FIREBASE / GOOGLE AUTH
// ════════════════════════════════════════════════════════════════════════════

async function getAccessToken() {
  if (_oauthToken && Date.now() < _tokenExpiry - 60000) return _oauthToken;
  const sa  = JSON.parse(SERVICE_ACCOUNT_JSON);
  const now = Math.floor(Date.now() / 1000);
  const b64u = s => Buffer.from(s).toString('base64')
    .replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
  const header  = b64u(JSON.stringify({ alg:'RS256', typ:'JWT' }));
  const payload = b64u(JSON.stringify({
    iss:   sa.client_email,
    scope: 'https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/firebase',
    aud:   'https://oauth2.googleapis.com/token',
    exp:   now + 3600, iat: now
  }));
  const msg = `${header}.${payload}`;
  const { createSign } = require('crypto');
  const sign = createSign('RSA-SHA256');
  sign.update(msg);
  const sig = sign.sign(sa.private_key, 'base64')
    .replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
  const jwt = `${msg}.${sig}`;
  const res = await fetchJson('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`
  });
  _oauthToken  = res.access_token;
  _tokenExpiry = Date.now() + (res.expires_in * 1000);
  log('OAuth token refreshed');
  return _oauthToken;
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 2 — HTTP HELPERS
// ════════════════════════════════════════════════════════════════════════════

function fetchJson(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === 'https:' ? https : http;
    const body = opts.body || null;
    const req = lib.request({
      hostname: u.hostname,
      port:     u.port || (u.protocol === 'https:' ? 443 : 80),
      path:     u.pathname + u.search,
      method:   opts.method || 'GET',
      headers:  { 'Content-Type': 'application/json', ...(opts.headers || {}),
                  ...(body ? { 'Content-Length': Buffer.byteLength(body) } : {}) }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch { resolve(data); }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

// RTDB REST helper — uses database secret for auth
async function rtdbGet(path) {
  const url = `${FIREBASE_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
  return fetchJson(url);
}

async function rtdbSet(path, data) {
  const url = `${FIREBASE_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
  return fetchJson(url, {
    method: 'PUT',
    body: JSON.stringify(data)
  });
}

async function rtdbDelete(path) {
  const url = `${FIREBASE_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
  return fetchJson(url, { method: 'DELETE' });
}

/**
 * Ghost-node guard. A server write to alerts/{uid}/{id} (a child like lastSide, or the whole alert)
 * can land AFTER the user deleted that alert from the app and re-create the node in RTDB while it is
 * gone from Firestore. Call this right after such a write: if the alert is no longer in our live
 * cache (the app deleted it) the node is removed again. Never throws.
 */
async function rtdbRemoveIfDeleted(userId, alertId) {
  try {
    if (activeAlerts[alertId]) return false;                 // still a live alert → keep
    await rtdbDelete(`alerts/${userId}/${alertId}`);
    log(`🧹 Removed ghost RTDB node (alert deleted during a server write): alerts/${userId}/${alertId}`);
    return true;
  } catch (e) {
    warn(`rtdbRemoveIfDeleted failed for ${alertId}: ${e.message}`);
    return false;
  }
}

// ── Bulletproof write helper for the trigger-processing pipeline ───────────
// attemptFn does a write AND reads the value back from the server to confirm
// it actually landed (not just that the HTTP call didn't throw) — same idea
// as the app's verifyBothLocationsOnServer() for alert creation. Retries the
// whole write+verify cycle a couple of times before giving up.
async function withVerifiedRetry(label, attemptFn, retries = 2, delayMs = 1200) {
  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      const ok = await attemptFn();
      if (ok) {
        if (attempt > 1) log(`  ✅ ${label} confirmed on retry ${attempt}`);
        return true;
      }
      warn(`  ${label}: verify mismatch on attempt ${attempt}/${retries + 1}`);
    } catch (e) {
      warn(`  ${label}: attempt ${attempt}/${retries + 1} threw — ${e.message}`);
    }
    if (attempt <= retries) await new Promise(r => setTimeout(r, delayMs));
  }
  err(`  ❌ ${label} FAILED after ${retries + 1} attempts — giving up`);
  return false;
}

// Pakistan-time formatting — must byte-match TimeUtils.formatPakistanDateOnly()
// / formatPakistanTimeWithSeconds() in the Android app (3-letter months, not
// Intl's 4-letter "Sept"), since the admin panel groups everything by this
// exact date-string doc ID.
const PK_MONTHS_3 = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function pkDateOnly(ms) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Karachi', day: '2-digit', month: 'numeric', year: 'numeric',
  }).formatToParts(new Date(ms));
  const get = (t) => parts.find((p) => p.type === t).value;
  const monthIdx = parseInt(get('month'), 10) - 1;
  return `${get('day')} ${PK_MONTHS_3[monthIdx]} ${get('year')}`;
}
function pkTimeFull(ms) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Karachi', day: '2-digit', month: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true,
  }).formatToParts(new Date(ms));
  const get = (t) => parts.find((p) => p.type === t).value;
  const monthIdx = parseInt(get('month'), 10) - 1;
  const ampm = (parts.find((p) => p.type === 'dayPeriod')?.value || '').toUpperCase();
  return `${get('day')} ${PK_MONTHS_3[monthIdx]} ${get('year')} ${get('hour')}:${get('minute')}:${get('second')} ${ampm}`;
}

// Firestore REST helper
async function firestoreGet(path) {
  const token = await getAccessToken();
  const BASE  = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents`;
  return fetchJson(`${BASE}/${path}`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
}

async function firestorePatch(path, fields) {
  const token    = await getAccessToken();
  const docPath  = `projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/${path}`;
  const fieldPaths = Object.keys(fields);
  return fetchJson(
    `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`,
    {
      method:  'POST',
      headers: { 'Authorization': `Bearer ${token}` },
      body:    JSON.stringify({
        writes: [{
          update:     { name: docPath, fields },
          updateMask: { fieldPaths }
        }]
      })
    }
  );
}

async function firestoreDelete(path) {
  const token = await getAccessToken();
  const BASE  = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents`;
  return fetchJson(`${BASE}/${path}`, {
    method:  'DELETE',
    headers: { 'Authorization': `Bearer ${token}` }
  });
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 3 — RTDB ALERT LISTENER
// ════════════════════════════════════════════════════════════════════════════

/**
 * RTDB Server-Sent Events listener.
 * Firebase RTDB supports SSE at /.json?auth=...&stream=true
 * This gives us real-time add/remove/change events without polling.
 */
function startRtdbListener() {
  const rtdbUrl = FIREBASE_URL || 'NOT SET';
  log(`Starting RTDB alerts listener on: ${rtdbUrl}/alerts`);

  const url = `${FIREBASE_URL}/alerts.json?auth=${FIREBASE_SECRET}&stream=true`;
  const u   = new URL(url);

  // ── Connection guard ──────────────────────────────────────────────────────
  // Only ONE connection attempt may be in-flight at a time.
  // Without this, rapid ECONNRESET storms spawn dozens of parallel connections,
  // exhaust Firebase RTDB's 100-connection free-tier limit, and cause an
  // infinite 402 reconnect loop that burns all bandwidth.
  let connecting   = false;  // true while an attempt is in-flight
  let reconnTimer  = null;   // handle to the pending setTimeout
  let failCount    = 0;      // consecutive failures for exponential backoff

  function scheduleReconnect(delayMs) {
    if (connecting) return;   // already connecting — don't pile on
    if (reconnTimer) return;  // already scheduled
    reconnTimer = setTimeout(() => {
      reconnTimer = null;
      connect();
    }, delayMs);
  }

  function connect() {
    if (connecting) {
      warn('RTDB SSE: connect() called while already connecting — skipped');
      return;
    }
    connecting = true;

    const req = https.request({
      hostname: u.hostname,
      port:     443,
      path:     u.pathname + u.search,
      method:   'GET',
      headers:  { 'Accept': 'text/event-stream', 'Cache-Control': 'no-cache' }
    }, res => {
      log(`RTDB SSE connected: ${res.statusCode}`);

      // 402 = RTDB connection limit hit (free plan: 100 max).
      // Destroy immediately and wait a long time before retrying.
      if (res.statusCode === 402) {
        warn('RTDB SSE: 402 connection limit — waiting 60s before retry');
        sendAlertEmail('rtdb_402', 'RTDB connection limit (402)', 'Firebase RTDB returned 402 — too many connections.\nWaiting 60s before retry.\nAlert monitoring is paused until reconnected.');
        res.destroy();
        connecting = false;
        failCount++;
        scheduleReconnect(60000);
        return;
      }

      if (res.statusCode !== 200) {
        warn(`RTDB SSE: unexpected status ${res.statusCode} — retrying in 10s`);
        if (failCount >= 3) sendAlertEmail('rtdb_error', `RTDB connection failing (${res.statusCode})`, `Firebase RTDB SSE returned status ${res.statusCode} repeatedly.\nFail count: ${failCount}\nAlert monitoring may be affected.`);
        res.destroy();
        connecting = false;
        failCount++;
        scheduleReconnect(10000);
        return;
      }

      // Successful connection
      failCount = 0;
      connecting = false; // connected — not "connecting" anymore, stream is live

      let buf = '';

      res.on('data', chunk => {
        buf += chunk.toString();
        const lines = buf.split('\n');
        buf = lines.pop();

        let event = null;
        let data  = null;

        for (const line of lines) {
          if (line.startsWith('event:')) {
            event = line.slice(6).trim();
          } else if (line.startsWith('data:')) {
            data = line.slice(5).trim();
          } else if (line === '' && event && data) {
            handleRtdbEvent(event, data);
            event = null;
            data  = null;
          }
        }
      });

      res.on('end', () => {
        warn('RTDB SSE stream ended — reconnecting in 5s');
        scheduleReconnect(5000);
      });

      res.on('error', e => {
        warn(`RTDB SSE error: ${e.message} — reconnecting in 5s`);
        scheduleReconnect(5000);
      });
    });

    req.on('error', e => {
      warn(`RTDB SSE request error: ${e.message} — reconnecting in 10s`);
      connecting = false;
      failCount++;
      if (failCount >= 3) sendAlertEmail('rtdb_req_error', 'RTDB connection request failing', `Firebase RTDB SSE request error (attempt ${failCount}).\nError: ${e.message}\nAlert monitoring may be affected.`);
      // Exponential backoff: 10s, 20s, 40s, max 60s
      const delay = Math.min(10000 * Math.pow(2, Math.min(failCount - 1, 3)), 60000);
      scheduleReconnect(delay);
    });

    req.setTimeout(150000, () => {
      warn('RTDB SSE: request timeout (150s) — aborting');
      req.destroy();
      connecting = false;
      failCount++;
      scheduleReconnect(5000);
    });

    req.end();
  }

  connect();
}

// ── REST polling fallback for alerts ──────────────────────────────────────────
// The SSE stream above (startRtdbListener) is the primary, low-latency path.
// It has proven unreliable in production for reasons still under investigation
// (connects with 200 but sometimes never delivers the initial data dump).
// This poller is a safety net: plain REST GET of /alerts.json, proven reliable,
// runs independently on its own interval and self-heals activeAlerts regardless
// of whether the SSE stream is working. Slightly higher latency (up to pollMs)
// but guarantees alerts are never silently stuck at 0.
// Keep in-memory-only state when a cached alert object is replaced by a fresh copy from
// RTDB (hourly fallback poll, alert updates): the side tracker for repeating alerts and the
// newest lastTriggeredAt. Side tracker is dropped if the target price was edited.
function carryRuntimeState(oldAlert, newAlert) {
  if (!oldAlert || !newAlert) return newAlert;

  const wasEnabled = oldAlert.enabled !== false;
  const isEnabled  = newAlert.enabled !== false;

  if (!wasEnabled && isEnabled) {
    // Alert was paused and has just been resumed. Any crossing that happened
    // while it was off is invisible to us and must NOT be treated as "the"
    // crossing the instant it comes back on — force a silent re-baseline on
    // the next check instead of firing immediately off stale movement.
    newAlert._justReenabled = true; // _lastSide deliberately NOT carried over
  } else if (oldAlert._lastSide && parseFloat(oldAlert.targetPrice) === parseFloat(newAlert.targetPrice)) {
    newAlert._lastSide = oldAlert._lastSide;
  }

  if ((oldAlert.lastTriggeredAt || 0) > (newAlert.lastTriggeredAt || 0)) {
    newAlert.lastTriggeredAt = oldAlert.lastTriggeredAt;
  }

  // EMA alerts: keep the in-memory side tracker unless the indicator setup was edited
  if (oldAlert._emaSide && newAlert.alertKind === 'indicator'
      && indSetupSig(oldAlert) === indSetupSig(newAlert)) {
    newAlert._emaSide = oldAlert._emaSide;
    newAlert._emaWait = oldAlert._emaWait;
    if (!newAlert.lastSide && oldAlert.lastSide) newAlert.lastSide = oldAlert.lastSide;
  }
  return newAlert;
}

async function pollAlertsFallback() {
  try {
    const value = await rtdbGet('alerts');
    const rebuilt = {};
    if (value && typeof value === 'object') {
      for (const userId of Object.keys(value)) {
        const userAlerts = value[userId];
        if (userAlerts && typeof userAlerts === 'object') {
          for (const [alertId, alert] of Object.entries(userAlerts)) {
            if (alert && typeof alert === 'object') {
              rebuilt[alertId] = carryRuntimeState(activeAlerts[alertId], { ...alert, userId });
            }
          }
        }
      }
    }

    const before = Object.keys(activeAlerts).length;
    const after  = Object.keys(rebuilt).length;

    // Replace activeAlerts contents in place (keep same object reference,
    // other code closes over `activeAlerts` directly)
    Object.keys(activeAlerts).forEach(k => delete activeAlerts[k]);
    Object.assign(activeAlerts, rebuilt);

    if (before !== after) {
      log(`Alerts fallback poll: ${after} active alerts (was ${before})`);
      scheduleGateResubscribe();
    }
  } catch (e) {
    warn(`Alerts fallback poll error: ${e.message}`);
  }
}

// Track whether we've done the first full load — reconnects skip the clear
let rtdbHasLoaded = false;

function handleRtdbEvent(event, data) {
  if (event === 'cancel' || event === 'auth_revoked') {
    warn(`RTDB SSE event: ${event}`);
    return;
  }
  if (event !== 'put' && event !== 'patch') return;

  try {
    const parsed = JSON.parse(data);
    const path   = parsed.path;
    const value  = parsed.data;

    if (path === '/' || path === '') {
      // Firebase sends a full data dump on every (re)connect.
      // On first connect: clear and reload everything.
      // On reconnect: merge instead of clearing — we already have the data
      // and clearing+reloading wastes bandwidth and resets the alert cache.
      if (!rtdbHasLoaded) {
        // First load — clear and populate
        Object.keys(activeAlerts).forEach(k => delete activeAlerts[k]);
        if (value && typeof value === 'object') {
          for (const userId of Object.keys(value)) {
            const userAlerts = value[userId];
            if (userAlerts && typeof userAlerts === 'object') {
              for (const [alertId, alert] of Object.entries(userAlerts)) {
                if (alert && typeof alert === 'object') {
                  activeAlerts[alertId] = { ...alert, userId };
                }
              }
            }
          }
        }
        rtdbHasLoaded = true;
        log(`RTDB initial load: ${Object.keys(activeAlerts).length} active alerts`);
        scheduleGateResubscribe();
      } else {
        // Reconnect — merge: add any alerts in Firebase not in memory,
        // remove any in memory that are no longer in Firebase
        const firestoreIds = new Set();
        if (value && typeof value === 'object') {
          for (const userId of Object.keys(value)) {
            const userAlerts = value[userId];
            if (userAlerts && typeof userAlerts === 'object') {
              for (const [alertId, alert] of Object.entries(userAlerts)) {
                firestoreIds.add(alertId);
                if (!activeAlerts[alertId] && alert && typeof alert === 'object') {
                  activeAlerts[alertId] = { ...alert, userId };
                }
              }
            }
          }
        }
        // Remove any alerts no longer in RTDB
        for (const id of Object.keys(activeAlerts)) {
          if (!firestoreIds.has(id)) delete activeAlerts[id];
        }
        log(`RTDB reconnect sync: ${Object.keys(activeAlerts).length} active alerts (no full reload)`);
      }
      return;
    }

    // Path format: /userId/alertId  or  /userId  or  /userId/alertId/field
    const parts = path.replace(/^\//, '').split('/');

    if (parts.length === 1) {
      // Whole user's alerts changed
      const userId = parts[0];
      // Snapshot the old per-alert state for this user BEFORE removing, so
      // carryRuntimeState below can still compare old vs new per alert.
      const oldAlertsForUser = {};
      for (const id of Object.keys(activeAlerts)) {
        if (activeAlerts[id].userId === userId) oldAlertsForUser[id] = activeAlerts[id];
      }
      // Remove all alerts for this user first
      for (const id of Object.keys(activeAlerts)) {
        if (activeAlerts[id].userId === userId) delete activeAlerts[id];
      }
      // Add new ones. Same 'put' vs 'patch' distinction as the single-alert
      // branch below — a 'patch' here would only list the alertIds that
      // changed, not this user's whole alert set, so a patch must be merged
      // per-alert onto what's cached rather than treated as the full list
      // (which the 'remove all for this user first' step above would then
      // wrongly leave empty for every alert NOT included in the patch).
      if (value && typeof value === 'object') {
        for (const [alertId, alert] of Object.entries(value)) {
          if (alert && typeof alert === 'object') {
            const base   = event === 'patch' ? (oldAlertsForUser[alertId] || {}) : {};
            const merged = { ...base, ...alert, userId };
            activeAlerts[alertId] = carryRuntimeState(oldAlertsForUser[alertId], merged);
          }
        }
        if (event === 'patch') {
          // Restore every alert NOT touched by this patch — they were removed
          // above but a patch never implied they were deleted.
          for (const [alertId, old] of Object.entries(oldAlertsForUser)) {
            if (!(alertId in value) && !activeAlerts[alertId]) activeAlerts[alertId] = old;
          }
        }
      }
      log(`RTDB user ${userId} alerts reloaded: ${Object.keys(activeAlerts).filter(k => activeAlerts[k].userId === userId).length} alerts`);
      scheduleGateResubscribe();

    } else if (parts.length === 2) {
      const [userId, alertId] = parts;
      if (value === null) {
        // Alert deleted — log with stack trace context
        const known = activeAlerts[alertId];
        log(`RTDB alert removed: ${alertId}${known ? ` (${known.pairSymbol} ${known.direction} ${known.targetPrice})` : ' (not in cache)'}`);
        delete activeAlerts[alertId];
        scheduleGateResubscribe();
      } else {
        // Alert added or updated. A 'put' event's `value` is the FULL alert
        // node; a 'patch' event's `value` is ONLY the fields that changed —
        // every write this server makes is a full PUT (see processRepeatingTrigger),
        // but this merge is kept as a safety net: if a 'patch' event ever does
        // arrive, treating its partial value as the whole alert would wipe
        // targetPrice/pairSymbol/direction/etc from the cache and silently
        // stop this alert from ever being checked again.
        const base   = event === 'patch' ? (activeAlerts[alertId] || {}) : {};
        const merged = { ...base, ...value, userId };
        activeAlerts[alertId] = carryRuntimeState(activeAlerts[alertId], merged);
        log(`RTDB alert ${event === 'patch' ? 'patched' : 'added'}: ${alertId} (${merged.pairSymbol} ${merged.direction} ${merged.targetPrice})`);
        scheduleGateResubscribe();
      }
    }

  } catch(e) {
    warn(`RTDB SSE parse error: ${e.message}`);
  }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 4 — CAPITAL.COM WEBSOCKET (XAU / XAG)
// ════════════════════════════════════════════════════════════════════════════

let capCst = null;
let capToken = null;
let capWs = null;

// Tracks the last time a live price tick was received from Capital.com WS per symbol.
// Used by the staleness watchdog to detect silent stalls (connected but no data flowing).
const lastTickAt = { XAU: 0, XAG: 0 };

// Snapshot of livePrice taken at the exact minute boundary (first 500ms of new minute).
// Used as the candle close price for XAU/XAG candle-close alerts — matches the chart close exactly.
// Key = tf ("M1","M5","M15","H1"), value = price at that boundary
const metalCloseSnapshot = { XAU: {}, XAG: {} };

async function createCapSession() {
  // Retry loop — Capital.com returns 429 (too-many-requests) on session create,
  // especially after a reconnect. Keep retrying with increasing delays until success.
  // Delays: 10s, 20s, 30s, 30s, 30s... (caps at 30s)
  let attempt = 0;
  while (true) {
    attempt++;
    log(`Creating Capital.com session (attempt ${attempt})...`);
    try {
      await new Promise((resolve, reject) => {
        const u    = new URL(`${CAP_REST_URL}/api/v1/session`);
        const body = JSON.stringify({ identifier: CAP_EMAIL, password: CAP_PASSWORD, encryptedPassword: false });
        const req  = https.request({
          hostname: u.hostname,
          port:     443,
          path:     u.pathname,
          method:   'POST',
          headers:  {
            'X-CAP-API-KEY':  CAP_API_KEY,
            'Content-Type':   'application/json',
            'Content-Length': Buffer.byteLength(body)
          }
        }, res => {
          capCst   = res.headers['cst'];
          capToken = res.headers['x-security-token'];
          let d = '';
          res.on('data', c => d += c);
          res.on('end', () => {
            if (res.statusCode === 200 && capCst && capToken) {
              log('Capital.com session created');
              resolve();
            } else {
              reject(new Error(`${res.statusCode} ${d}`));
            }
          });
        });
        req.on('error', reject);
        req.write(body);
        req.end();
      });
      return; // success — exit loop
    } catch (e) {
      const is429 = e.message.startsWith('429');
      const delayS = Math.min(10 * attempt, 30);
      if (is429) {
        warn(`Capital.com session 429 too-many-requests (attempt ${attempt}) — retrying in ${delayS}s`);
        if (attempt === 3) sendAlertEmail('cap_session_429', 'Capital.com session 429 too-many-requests', `Capital.com is rejecting session creation with 429.\nAttempt: ${attempt}\nWill keep retrying every ${delayS}s automatically.`);
      } else {
        warn(`Capital.com session failed: ${e.message} (attempt ${attempt}) — retrying in ${delayS}s`);
        if (attempt === 2) sendAlertEmail('cap_session_fail', 'Capital.com session failed', `Capital.com session creation failed.\nError: ${e.message}\nAttempt: ${attempt}\nWill keep retrying every ${delayS}s automatically.`);
      }
      await new Promise(r => setTimeout(r, delayS * 1000));
    }
  }
}

async function pingCapSession() {
  if (!capCst || !capToken) return;
  try {
    const u   = new URL(`${CAP_REST_URL}/api/v1/ping`);
    await new Promise((resolve) => {
      const req = https.request({
        hostname: u.hostname, port: 443, path: u.pathname, method: 'GET',
        headers: { 'CST': capCst, 'X-SECURITY-TOKEN': capToken }
      }, res => {
        if (res.statusCode !== 200) {
          warn(`Cap ping failed ${res.statusCode} — recreating session`);
          createCapSession().then(() => reconnectCapWs()).catch(e => err(`Cap session recreate: ${e.message}`));
        }
        res.resume();
        resolve();
      });
      req.on('error', e => {
        warn(`Cap ping error: ${e.message}`);
        resolve();
      });
      req.end();
    });
  } catch(e) { warn(`pingCapSession: ${e.message}`); }
}

// Watchdog: checks every 60s whether Capital.com WS is still delivering ticks.
// If XAU hasn't had a tick for 2 minutes during market hours, the stream has
// silently stalled — force-reconnect immediately so users aren't stuck on a stale price.
function startCapWsWatchdog() {
  setInterval(() => {
    if (!isMetalsOpen()) {
      // Market closed — reset tick timestamps so watchdog doesn't fire
      // immediately at next market open (no ticks expected while closed).
      lastTickAt.XAU = 0;
      lastTickAt.XAG = 0;
      return;
    }
    if (capWsReconnecting) return; // already reconnecting — don't pile on
    const now     = Date.now();
    const staleMs = 2 * 60 * 1000; // 2 minutes
    const xauAge  = now - lastTickAt.XAU;
    const xagAge  = now - lastTickAt.XAG;
    if (lastTickAt.XAU > 0 && xauAge > staleMs) {
      warn(`Capital.com WS stale — XAU last tick ${Math.round(xauAge/1000)}s ago — force reconnecting`);
      sendAlertEmail('cap_ws_stale', 'Capital.com WS price feed stalled', `Gold (XAU) price feed stopped updating.\nLast tick: ${Math.round(xauAge/1000)}s ago.\nAuto-reconnecting now — prices may have been stale for up to 2 minutes.`);
      lastTickAt.XAU = Date.now(); // reset so we don't re-trigger next minute during reconnect
      lastTickAt.XAG = Date.now();
      reconnectCapWs();
    } else if (lastTickAt.XAG > 0 && xagAge > staleMs) {
      warn(`Capital.com WS stale — XAG last tick ${Math.round(xagAge/1000)}s ago — force reconnecting`);
      sendAlertEmail('cap_ws_stale', 'Capital.com WS price feed stalled', `Silver (XAG) price feed stopped updating.\nLast tick: ${Math.round(xagAge/1000)}s ago.\nAuto-reconnecting now.`);
      lastTickAt.XAU = Date.now();
      lastTickAt.XAG = Date.now();
      reconnectCapWs();
    }
  }, 60 * 1000);
}

function connectCapWs() {
  log('Connecting Capital.com WebSocket...');
  capWs = new WebSocket(CAP_WS_URL);

  capWs.on('open', () => {
    log('Capital.com WebSocket connected');

    capWs.send(JSON.stringify({
      destination: 'marketData.subscribe', correlationId: '1',
      cst: capCst, securityToken: capToken,
      payload: { epics: ['GOLD', 'SILVER'] }
    }));

    capWs.send(JSON.stringify({
      destination: 'OHLCMarketData.subscribe', correlationId: '2',
      cst: capCst, securityToken: capToken,
      payload: {
        epics: ['GOLD', 'SILVER'],
        resolutions: ['MINUTE', 'MINUTE_5', 'MINUTE_15', 'HOUR'],
        type: 'classic'
      }
    }));

    // Keep WebSocket alive with ping every 30s
    if (capWs._ping) clearInterval(capWs._ping);
    capWs._ping = setInterval(() => {
      if (capWs && capWs.readyState === WebSocket.OPEN) {
        capWs.send(JSON.stringify({
          destination: 'ping', correlationId: `p-${Date.now()}`,
          cst: capCst, securityToken: capToken
        }));
      }
    }, 30000);
  });

  capWs.on('message', data => {
    try {
      const msg  = JSON.parse(data.toString());
      const epic = msg.payload?.epic;
      const sym  = epic === 'GOLD' ? 'XAU' : epic === 'SILVER' ? 'XAG' : null;

      // Live tick
      if (msg.destination === 'quote' && sym) {
        const bid = msg.payload.bid || 0;
        const ask = msg.payload.ofr || 0;
        if (bid > 0) {
          livePrice[sym] = bid;
          lastTickAt[sym] = Date.now();
          updateOpenCandle(sym, bid);
        }
      }

      // OHLC candle updates — kept for m1Candle high/low tracking (miss-hit detection)
      if (msg.destination === 'ohlc.event' && sym) {
        const res = msg.payload.resolution;
        const c   = msg.payload.c;
        const h   = msg.payload.h;
        const l   = msg.payload.l;
        if (c && h && l && res === 'MINUTE') {
          const existingByTf = m1Candle[sym]?.byTf;
          m1Candle[sym] = { high: h, low: l, close: c, byTf: existingByTf || {} };
        }
      }

    } catch(e) { /* ignore parse errors */ }
  });

  capWs.on('close', (code, reason) => {
    if (capWs._ping) { clearInterval(capWs._ping); capWs._ping = null; }
    // Only reschedule if this close was NOT triggered by our own terminate() call.
    // reconnectCapWs sets capWs=null before terminate, so if capWs is null here
    // it means we intentionally closed it — don't spawn another reconnect.
    if (capWsReconnecting) return; // reconnect already scheduled/running
    warn(`Capital.com WS closed ${code} — reconnecting in 5s`);
    capWsReconnecting = true;
    setTimeout(() => { capWsReconnecting = false; reconnectCapWs(); }, 5000);
  });

  capWs.on('error', e => warn(`Capital.com WS error: ${e.message}`));
}

let capWsReconnecting = false;

function reconnectCapWs() {
  if (capWsReconnecting) return; // already in progress — don't stack
  capWsReconnecting = true;
  if (capWs) {
    capWs.removeAllListeners(); // prevent close handler from spawning another reconnect
    try { capWs.terminate(); } catch {}
    capWs = null;
  }
  // Small delay before connecting to let Capital.com breathe
  setTimeout(() => {
    capWsReconnecting = false;
    connectCapWs();
  }, 1000);
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 5 — GATE.IO REST POLLING (Crypto prices)
// ════════════════════════════════════════════════════════════════════════════

const GATE_SYMBOLS = {
  BTC:'BTC_USDT', ETH:'ETH_USDT', BNB:'BNB_USDT', SOL:'SOL_USDT',
  XRP:'XRP_USDT', ADA:'ADA_USDT', DOGE:'DOGE_USDT', AVAX:'AVAX_USDT',
  DOT:'DOT_USDT', MATIC:'POL_USDT', LINK:'LINK_USDT', UNI:'UNI_USDT',
  ATOM:'ATOM_USDT', LTC:'LTC_USDT', BCH:'BCH_USDT', NEAR:'NEAR_USDT',
  ARB:'ARB_USDT', OP:'OP_USDT', SHIB:'SHIB_USDT', TRX:'TRX_USDT',
  SUI:'SUI_USDT', APT:'APT_USDT', PEPE:'PEPE_USDT',
  ICP:'ICP_USDT', INJ:'INJ_USDT', FIL:'FIL_USDT', HBAR:'HBAR_USDT'
};

const GATE_REVERSE = {};
for (const [sym, gs] of Object.entries(GATE_SYMBOLS)) {
  GATE_REVERSE[gs] = sym;
  GATE_SYMBOLS_SET.add(sym); // populate the active-pair helper set
}

const MEXC_SYMBOLS = {
  BTC:'BTCUSDT', ETH:'ETHUSDT', BNB:'BNBUSDT', SOL:'SOLUSDT',
  XRP:'XRPUSDT', ADA:'ADAUSDT', DOGE:'DOGEUSDT', AVAX:'AVAXUSDT',
  DOT:'DOTUSDT', MATIC:'MATICUSDT', LINK:'LINKUSDT', UNI:'UNIUSDT',
  ATOM:'ATOMUSDT', LTC:'LTCUSDT', BCH:'BCHUSDT', NEAR:'NEARUSDT',
  ARB:'ARBUSDT', OP:'OPUSDT', SHIB:'SHIBUSDT', TRX:'TRXUSDT',
  SUI:'SUIUSDT', APT:'APTUSDT', PEPE:'PEPEUSDT',
  ICP:'ICPUSDT', INJ:'INJUSDT', FIL:'FILUSDT', HBAR:'HBARUSDT'
};

let gateWs = null;
let gateWsConnected = false;

// Tracks which Gate pairs are currently subscribed on the open WS
let gateSubscribedPairs = new Set();

// Debounce resubscribe so rapid alert add/remove (e.g. batch load) only triggers once
let _gateResubTimer = null;
function scheduleGateResubscribe() {
  if (_gateResubTimer) clearTimeout(_gateResubTimer);
  _gateResubTimer = setTimeout(() => {
    _gateResubTimer = null;
    syncGateSubscriptions();
  }, 2000); // 2s debounce
}

/**
 * Compares currently-subscribed Gate pairs vs pairs that have active alerts.
 * Sends unsubscribe for pairs no longer needed, subscribe for new ones.
 * Much cheaper than reconnecting the whole WebSocket.
 */
function syncGateSubscriptions() {
  if (!gateWs || gateWs.readyState !== WebSocket.OPEN) return;

  const needed = getActiveCryptoPairs(); // Set of syms like 'BTC', 'ETH'

  // Unsubscribe pairs no longer needed
  const toUnsub = [...gateSubscribedPairs].filter(s => !needed.has(s));
  if (toUnsub.length > 0) {
    const tickers = toUnsub.map(s => GATE_SYMBOLS[s]);
    gateWs.send(JSON.stringify({
      time:    Math.floor(Date.now() / 1000),
      channel: 'spot.tickers',
      event:   'unsubscribe',
      payload: tickers
    }));
    toUnsub.forEach(s => gateSubscribedPairs.delete(s));
    log(`Gate.io WS unsubscribed ${toUnsub.length} pairs: ${toUnsub.join(', ')}`);
  }

  // Subscribe to new needed pairs
  const toSub = [...needed].filter(s => !gateSubscribedPairs.has(s));
  if (toSub.length > 0) {
    const tickers = toSub.map(s => GATE_SYMBOLS[s]);
    gateWs.send(JSON.stringify({
      time:    Math.floor(Date.now() / 1000),
      channel: 'spot.tickers',
      event:   'subscribe',
      payload: tickers
    }));
    toSub.forEach(s => gateSubscribedPairs.add(s));
    log(`Gate.io WS subscribed ${toSub.length} pairs: ${toSub.join(', ')}`);
  }

  if (toUnsub.length === 0 && toSub.length === 0) {
    log(`Gate.io WS subscriptions unchanged (${gateSubscribedPairs.size} pairs)`);
  }
}

function connectGateWs() {
  log('Connecting Gate.io WebSocket...');
  gateWs = new WebSocket('wss://api.gateio.ws/ws/v4/');

  gateWs.on('open', () => {
    log('Gate.io WebSocket connected');
    gateWsConnected  = true;
    gateSubscribedPairs = new Set();

    // Subscribe only to pairs that currently have active alerts
    const needed = getActiveCryptoPairs();
    if (needed.size > 0) {
      const tickers = [...needed].map(s => GATE_SYMBOLS[s]);
      gateWs.send(JSON.stringify({
        time:    Math.floor(Date.now() / 1000),
        channel: 'spot.tickers',
        event:   'subscribe',
        payload: tickers
      }));
      needed.forEach(s => gateSubscribedPairs.add(s));
      log(`Gate.io WS initial subscribe: ${tickers.length} pairs (${[...needed].join(', ')})`);
    } else {
      log('Gate.io WS connected — no active crypto alerts, not subscribing yet');
    }

    if (gateWs._ping) clearInterval(gateWs._ping);
    gateWs._ping = setInterval(() => {
      if (gateWs && gateWs.readyState === WebSocket.OPEN) {
        gateWs.send(JSON.stringify({ time: Math.floor(Date.now()/1000), channel: 'spot.ping' }));
      }
    }, 30000);
  });

  gateWs.on('message', data => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.event === 'subscribe' || msg.event === 'unsubscribe') {
        if (msg.error) warn(`Gate.io WS ${msg.event} error: ${JSON.stringify(msg.error)}`);
        return;
      }
      if (msg.channel === 'spot.pong') return;
      if (msg.channel === 'spot.tickers' && msg.event === 'update') {
        const result = msg.result;
        const sym    = GATE_REVERSE[result?.currency_pair];
        const price  = parseFloat(result?.last) || 0;
        if (sym && price > 0) {
          livePrice[sym] = price;
          updateOpenCandle(sym, price);
        }
      }
    } catch(e) { /* ignore */ }
  });

  gateWs.on('close', (code) => {
    gateWsConnected = false;
    gateSubscribedPairs = new Set();
    if (gateWs._ping) { clearInterval(gateWs._ping); gateWs._ping = null; }
    gateWsDisconnects = (gateWsDisconnects || 0) + 1;
    warn(`Gate.io WS closed ${code} — reconnecting in 5s (disconnect #${gateWsDisconnects})`);
    if (gateWsDisconnects % 5 === 0) sendAlertEmail('gate_ws_disconnect', 'Gate.io WS repeatedly disconnecting', `Gate.io WebSocket has disconnected ${gateWsDisconnects} times.\nLast close code: ${code}\nCrypto prices may be intermittently unavailable.`);
    setTimeout(connectGateWs, 5000);
  });

  gateWs.on('error', e => warn(`Gate.io WS error: ${e.message}`));
}

// Fallback REST polling — only runs if WebSocket is not connected
async function pollGateio() {
  if (gateWsConnected) return;
  const needed = getActiveCryptoPairs();
  if (needed.size === 0) return;
  try {
    const results = await Promise.all(
      [...needed].map(async sym => {
        const gSym = GATE_SYMBOLS[sym];
        try {
          const res    = await fetchJson(`https://api.gateio.ws/api/v4/spot/tickers?currency_pair=${gSym}`);
          const ticker = Array.isArray(res) ? res[0] : res;
          const price  = parseFloat(ticker?.last) || 0;
          return { sym, price };
        } catch(e) { return { sym, price: 0 }; }
      })
    );
    let updated = 0;
    for (const { sym, price } of results) {
      if (price > 0) { livePrice[sym] = price; updateOpenCandle(sym, price); updated++; }
    }
    if (updated > 0) log(`Gate.io REST fallback: ${updated}/${needed.size} prices. BTC=${livePrice.BTC}`);
  } catch(e) { warn(`Gate.io REST fallback error: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 6 — YAHOO FINANCE POLLING (Indices + Forex)
// ════════════════════════════════════════════════════════════════════════════

const YAHOO_SYMBOLS = {
  SPX500: '%5EGSPC', US30: '%5EDJI', US100: '%5ENDX',
  DXY: 'DX-Y.NYB', NIF50: '%5ENSEI',
  EURUSD: 'EURUSD=X', GBPUSD: 'GBPUSD=X', USDJPY: 'USDJPY=X',
  GBPJPY: 'GBPJPY=X', AUDUSD: 'AUDUSD=X',
  USDCAD: 'USDCAD=X', USDCHF: 'USDCHF=X', NZDUSD: 'NZDUSD=X', EURJPY: 'EURJPY=X'
  // USDGBP is derived from GBPUSD (1/GBPUSD) — not a separate Yahoo fetch
};

// Populate helper set for active pair tracking (USDGBP excluded — derived from GBPUSD)
for (const sym of Object.keys(YAHOO_SYMBOLS)) YAHOO_SYMBOLS_KEYS.add(sym);
YAHOO_SYMBOLS_KEYS.delete('USDGBP');

async function pollYahoo() {
  // Skip entirely on Saturday, or Sunday before 20:00 UTC — nothing is open yet
  // (forex/metals open Sun 20:00 UTC at the earliest, indices never open on weekends)
  {
    const now  = new Date();
    const day  = now.getUTCDay();
    const mins = now.getUTCHours() * 60 + now.getUTCMinutes();
    if (day === 6 || (day === 0 && mins < 20*60)) {
      log('Yahoo: skipping — weekend, markets closed');
      return;
    }
  }

  // Fix 2: Only poll pairs that have at least one active alert AND whose market is open
  const activeYahoo = getActiveYahooPairs();
  if (activeYahoo.size === 0) {
    log('Yahoo: no active forex/index alerts — skipping');
    return;
  }

  const toFetch = [...activeYahoo].filter(sym => isYahooSymbolOpen(sym));
  if (toFetch.length === 0) {
    log(`Yahoo: ${activeYahoo.size} active alerts but all markets currently closed — skipping`);
    return;
  }

  let updated = 0;
  for (const sym of toFetch) {
    const yahooSym = YAHOO_SYMBOLS[sym];
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${yahooSym}?interval=1m&range=2m`;
      const res = await fetchJson(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'application/json',
          'Accept-Language': 'en-US,en;q=0.9'
        }
      });
      const meta  = res?.chart?.result?.[0]?.meta;
      const price = parseFloat(meta?.regularMarketPrice) || 0;
      if (price > 0) {
        livePrice[sym] = price;
        if (sym === 'GBPUSD' && price > 0) livePrice.USDGBP = parseFloat((1 / price).toFixed(6));
        updated++;
      } else {
        if (sym === 'EURUSD') {
          warn(`Yahoo EURUSD failed — response: ${JSON.stringify(res).slice(0, 200)}`);
        }
      }
    } catch(e) {
      if (sym === 'EURUSD') warn(`Yahoo EURUSD error: ${e.message}`);
    }
  }
  log(`Yahoo: ${updated}/${toFetch.length} updated (${toFetch.join(', ')}). EURUSD=${livePrice.EURUSD} SPX500=${livePrice.SPX500}`);
  if (updated === 0 && toFetch.length > 0) {
    yahooFailCount = (yahooFailCount || 0) + 1;
    if (yahooFailCount === 5) sendAlertEmail('yahoo_fail', 'Yahoo Finance polling failing', `Yahoo Finance returned no prices for ${toFetch.length} symbols (${toFetch.join(', ')}) for 5 consecutive polls.\nForex and index alerts may not be triggering correctly.`);
  } else {
    yahooFailCount = 0;
  }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 7 — CANDLE BUILDER (tick-based M1 for candle close alerts)
// ════════════════════════════════════════════════════════════════════════════

function updateOpenCandle(sym, price) {
  const nowMin = Math.floor(Date.now() / 60000);
  if (!openCandle[sym]) {
    openCandle[sym] = { open: price, high: price, low: price, startMin: nowMin };
    return;
  }
  const oc = openCandle[sym];
  if (oc.startMin !== nowMin) {
    const existingByTf = m1Candle[sym]?.byTf;
    m1Candle[sym] = { high: oc.high, low: oc.low, close: oc.open, byTf: existingByTf || {} };
    openCandle[sym] = { open: price, high: price, low: price, startMin: nowMin };
  } else {
    if (price > oc.high) oc.high = price;
    if (price < oc.low)  oc.low  = price;
  }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 8 — PRICE ALERT CHECKER (instant alerts every second)
// ════════════════════════════════════════════════════════════════════════════

let serverStopped = false;

async function checkServerStopFlag() {
  try {
    const val = await rtdbGet('worker_control/stop');
    const wasStopped = serverStopped;
    serverStopped = val === true || val === 'true';
    if (serverStopped) {
      log('Server stop flag is TRUE — only dev alerts will be checked');
      if (!wasStopped) sendAlertEmail('server_stopped', 'TradeAlert server STOPPED', 'The server stop flag has been set to TRUE in Firebase.\nAll user alerts are paused — only dev alerts are being checked.\nSet worker_control/stop = false in RTDB to resume.');
    }
  } catch(e) { /* keep current state */ }
}

/** "320 total (290 price, 20 candle close, 10 indicator)" — price = instant, candle close = price candle-close, indicator = all indicator alerts. */
function alertCountsText(list) {
  let ind = 0, candle = 0, price = 0;
  for (const a of list) {
    if (a.alertKind === 'indicator') ind++;
    else if (a.candleClose) candle++;
    else price++;
  }
  return `${list.length} total (${price} price, ${candle} candle close, ${ind} indicator)`;
}

let _checkCount = 0;
function checkAlerts() {
  _checkCount++;
  const alertList = Object.values(activeAlerts);

  if (_checkCount % 30 === 0) {
    log(`Checker alive — ${alertCountsText(alertList)}, BTC=${livePrice.BTC} XAU=${livePrice.XAU} EURUSD=${livePrice.EURUSD}`);
    // Show all active instant alerts for debugging
    alertList.filter(a => !a.candleClose && a.alertKind !== 'indicator').forEach(a => {
      const p   = livePrice[a.pairSymbol];
      const mkt = (p && p > 0) ? '' : (isYahooSymbolOpen(a.pairSymbol) === false ? ' [market closed]' : ' [no price yet]');
      log(`  active: ${a.pairSymbol} ${a.direction} ${a.targetPrice} price=${p || 0}${mkt}`);
    });
  }
  if (!alertList.length) return;

  try { checkIndicatorTouchAlerts(); } catch (e) { warn(`checkIndicatorTouchAlerts error: ${e.message}`); } // EMA "Touch" — in-memory only

  const nowMs = Date.now();
  for (const alert of alertList) {
    try {
      const mode = alert.alertMode === 'repeating' ? 'repeating' : 'once';
      if (mode === 'once' && recentlyTriggered.has(alert.id)) continue;
      if (alert.enabled === false) continue; // paused by the user (repeating alerts only)
      if (serverStopped && alert.userEmail !== DEV_EMAIL) continue;
      if (alert.alertKind === 'indicator') continue; // EMA alerts → Section 12 (indicator engine)
      if (alert.candleClose) continue; // handled by checkCandleCloseAlerts at minute boundary

      // Skip when this symbol's market is currently closed — livePrice may hold a
      // stale pre-close value (WS stays connected on weekends, just not updating).
      const sym = alert.pairSymbol;
      if ((sym === 'XAU' || sym === 'XAG') && !isMetalsOpen()) continue;
      if (FOREX_PAIRS.has(sym) && !isForexOpen()) continue;

      const price = livePrice[alert.pairSymbol];
      if (!price || price <= 0) continue;

      const target = parseFloat(alert.targetPrice);
      if (!target) continue;

      if (mode === 'repeating') {
        checkRepeatingCrossing(alert, price);
        continue;
      }

      let hit = false;
      if (alert.direction === 'above' && price >= target) hit = true;
      if (alert.direction === 'below' && price <= target) hit = true;

      if (hit) {
        log(`🎯 Alert triggered: ${alert.pairSymbol} ${alert.direction} ${target} (price=${price}) user=${alert.userId}`);
        recentlyTriggered.add(alert.id);
        processTriggeredAlert(alert, price).catch(e => {
          err(`processTriggeredAlert error: ${e.message}`);
          recentlyTriggered.delete(alert.id);
        });
      }
    } catch(e) { warn(`checkAlerts error for ${alert.id}: ${e.message}`); }
  }
}

// ════════════════════════════════════════════════════════════════════════════
// REPEATING ALERTS — crossing detection + cooldown
//
// Unlike "once" alerts (fixed direction, delete on hit), a repeating alert
// fires every time the price CROSSES the target line — in either direction —
// subject to a per-alert cooldown so a price flickering around the target
// doesn't spam FCMs.
//
// We track, per alert, which side of the target the price was on at the last
// check (alert._lastSide). A crossing = the side flipped since last check.
// The baseline side is the side the price was on when the alert was created
// (or the side persisted at the last trigger), so a candle close that is already
// across the target on the very first check correctly triggers.
// ════════════════════════════════════════════════════════════════════════════

function checkRepeatingCrossing(alert, value) {
  const target = parseFloat(alert.targetPrice);
  if (!target) return;
  const currentSide = value >= target ? 'above' : 'below';

  if (alert._justReenabled) {
    // Just resumed after being paused — silently establish a fresh baseline
    // from wherever price is right now and wait for the NEXT real crossing.
    // Ignores whatever price did while the alert was disabled.
    alert._justReenabled = false;
    alert._lastSide = currentSide;
    log(`  🔁 repeating alert ${alert.id} resumed — baseline set to '${currentSide}', no trigger`);
    return;
  }

  // First evaluation since this server process started, for an alert that already existed before
  // the start (restart / deploy): never trust the stored side — adopt where the price is NOW and
  // wait for the next real crossing. Alerts created while the server is running skip this and keep
  // the creation-side baseline below, so their first real crossing still fires.
  if (!REP_SEEN.has(alert.id)) {
    REP_SEEN.add(alert.id);
    if (alert._lastSide == null && (parseInt(alert.createdAt, 10) || 0) < SERVER_BOOT_MS - 60000) {
      alert._lastSide = currentSide;
      log(`  🔁 repeating alert ${alert.id} (${alert.pairSymbol} ${target}) loaded after restart — baseline '${currentSide}' (value=${value}), no trigger`);
      return;
    }
  }

  // First evaluation of this alert in this process: baseline = the side the price was
  // on when the alert was CREATED (alert.direction is "above" when the target sat above
  // the creation price, i.e. creation side = "below"), or the side persisted at the last
  // trigger. Using the creation side (not "whatever the first check sees") means the very
  // first candle close / tick that is already across the target counts as a crossing.
  if (alert._lastSide == null) {
    if (alert.lastSide === 'above' || alert.lastSide === 'below') {
      alert._lastSide = alert.lastSide;               // persisted at the last trigger
    } else if ((alert.lastTriggeredAt || 0) > 0) {
      // Already fired before, but its side was never persisted (fired on an older server
      // version) — the creation side would be wrong now. Learn the side from the current
      // value and wait for the NEXT real crossing instead of firing a false alert.
      alert._lastSide = currentSide;
      return;
    } else {
      alert._lastSide = (alert.direction === 'above' ? 'below' : 'above'); // creation side
    }
  }
  if (alert._lastSide === currentSide) return; // no crossing since last check

  const crossSide = currentSide;
  alert._lastSide = currentSide; // update immediately — avoids re-detecting the same crossing

  const cooldownMs = Math.max(1, parseInt(alert.cooldownMinutes, 10) || 5) * 60000;
  const lastFired   = alert.lastTriggeredAt || 0;
  const now         = Date.now();

  if (now - lastFired < cooldownMs) {
    const secsLeft = Math.ceil((cooldownMs - (now - lastFired)) / 1000);
    log(`  🔁 repeating alert ${alert.id} crossed target (${crossSide}) but in cooldown — ${secsLeft}s left`);
    return;
  }

  log(`🎯🔁 Repeating alert triggered: ${alert.pairSymbol} crossed ${target} (${crossSide}, value=${value}) user=${alert.userId}`);
  processRepeatingTrigger(alert, value, crossSide).catch(e => {
    err(`processRepeatingTrigger error: ${e.message}`);
  });
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 9 — MINUTE BOUNDARY CANDLE CLOSE CHECKER
// ════════════════════════════════════════════════════════════════════════════

let lastCandleMinute = -1;

async function onMinuteClose() {
  const nowMin = Math.floor(Date.now() / 60000);
  if (nowMin === lastCandleMinute) return;
  lastCandleMinute = nowMin;

  const closedM5  = nowMin % 5  === 0;
  const closedM15 = nowMin % 15 === 0;
  const closedH1  = nowMin % 60 === 0;

  const closedTfs = ['M1'];
  if (closedM5)  closedTfs.push('M5');
  if (closedM15) closedTfs.push('M15');
  if (closedH1)  closedTfs.push('H1');

  log(`── Minute ${nowMin} closed [${closedTfs.join(' ')}] ──`);

  const gateIntervals = { M1:'1m', M5:'5m', M15:'15m', H1:'1h' };

  // Fix 5: Only fetch candle data for pairs that have active candle-close alerts for this TF
  for (const tf of closedTfs) {
    const interval     = gateIntervals[tf];
    const activePairs  = getActiveCandleCloseCryptoPairs(tf);

    if (activePairs.size === 0) {
      log(`  No active candle-close alerts for ${tf} — skipping Gate.io candle fetch`);
      continue;
    }

    for (const sym of activePairs) {
      const gSym = GATE_SYMBOLS[sym];
      try {
        const url = `https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair=${gSym}&interval=${interval}&limit=2`;
        const res = await fetchJson(url);
        if (Array.isArray(res) && res.length >= 1) {
          const prev  = res[res.length - 1];
          const close = parseFloat(prev[2]) || 0;
          if (!m1Candle[sym])       m1Candle[sym]       = {};
          if (!m1Candle[sym].byTf)  m1Candle[sym].byTf  = {};
          m1Candle[sym].byTf[tf] = { high: parseFloat(prev[3])||0, low: parseFloat(prev[4])||0, close };
          if (tf === 'M1') m1Candle[sym].close = close;
          if (sym === 'BTC' && close > 0) log(`  BTC ${tf} close: ${close}`);
          if (sym === 'BTC' && !close)    warn(`  BTC ${tf} close missing! raw=${JSON.stringify(prev)}`);
        } else if (sym === 'BTC') {
          warn(`  BTC ${tf} candle empty: ${JSON.stringify(res).slice(0,100)}`);
        }
      } catch(e) {
        if (sym === 'BTC') warn(`  Gate.io BTC ${tf} error: ${e.message}`);
      }
    }
  }

  // Yahoo candle close for indices + forex — only active pairs, market hours only.
  // isYahooSymbolOpen() per-symbol filter below correctly handles weekend/session hours
  // for both forex (Sun 20:00 UTC open) and indices, so no outer weekend gate needed.
  {
    const activeYahoo = getActiveYahooPairs();
    const toFetch     = [...activeYahoo].filter(sym => isYahooSymbolOpen(sym));

    const yahooIntervalMap = { M1: '1m', M5: '5m', M15: '15m', H1: '60m' };
    const yahooRangeMap    = { M1: '5m', M5: '30m', M15: '2h', H1: '2d' };

    for (const sym of toFetch) {
      const yahooSym = YAHOO_SYMBOLS[sym];

      for (const tf of closedTfs) {
        // Only fetch if there's at least one candle-close alert for this sym+tf
        // (instant alerts don't need candle data)
        const hasCandleAlert = Object.values(activeAlerts).some(
          a => a.candleClose && a.alertKind !== 'indicator' && a.timeframe === tf && a.pairSymbol === sym
        );
        if (!hasCandleAlert) continue;
        const interval = yahooIntervalMap[tf];
        const range    = yahooRangeMap[tf];
        try {
          const url = `https://query1.finance.yahoo.com/v8/finance/chart/${yahooSym}?interval=${interval}&range=${range}`;
          const res = await fetchJson(url, {
            headers: {
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
              'Accept': 'application/json'
            }
          });
          const quotes = res?.chart?.result?.[0]?.indicators?.quote?.[0];
          const times  = res?.chart?.result?.[0]?.timestamp;
          if (quotes && times && times.length >= 1) {
            let close = 0, closeIdx = -1;
            for (let i = quotes.close.length - 1; i >= 0; i--) {
              if (quotes.close[i] != null && quotes.close[i] > 0) {
                close = quotes.close[i]; closeIdx = i; break;
              }
            }
            if (close > 0) {
              if (!m1Candle[sym])      m1Candle[sym]      = {};
              if (!m1Candle[sym].byTf) m1Candle[sym].byTf = {};
              m1Candle[sym].byTf[tf] = {
                close,
                high: quotes.high?.[closeIdx] || 0,
                low:  quotes.low?.[closeIdx]  || 0
              };
              if (tf === 'M1') m1Candle[sym].close = close;
              log(`  ${sym} ${tf} Yahoo candle close: ${close}`);
            } else {
              warn(`  ${sym} ${tf} Yahoo candle: all closes null`);
            }
          } else {
            warn(`  ${sym} ${tf} Yahoo candle: no data. times=${times?.length}`);
          }
        } catch(e) {
          warn(`  ${sym} ${tf} Yahoo candle error: ${e.message}`);
        }
      }
    }
  }

  // Metals — metalCloseSnapshot was already taken at boundary in startMinuteBoundaryChecker.
  // No REST call needed; just check alerts directly.
  checkCandleCloseAlerts(closedTfs);
}

function checkCandleCloseAlerts(closedTfs) {
  const all       = Object.values(activeAlerts);
  const alertList = all.filter(a => a.candleClose && a.alertKind !== 'indicator' && closedTfs.includes(a.timeframe));

  log(`  checkCandleCloseAlerts: ${alertCountsText(all)}, ${alertList.length} match [${closedTfs.join(',')}]`);
  all.filter(a => a.candleClose && a.alertKind !== 'indicator').forEach(a =>
    log(`    alert: ${a.pairSymbol} ${a.direction} ${a.targetPrice} tf=${a.timeframe} candleClose=${a.candleClose}`)
  );

  const nowMs = Date.now();
  for (const alert of alertList) {
    try {
      const mode = alert.alertMode === 'repeating' ? 'repeating' : 'once';
      if (mode === 'once' && recentlyTriggered.has(alert.id)) {
        log(`    SKIP ${alert.id}: recentlyTriggered`); continue;
      }
      if (alert.enabled === false) { log(`    SKIP ${alert.id}: disabled by user`); continue; }
      if (serverStopped && alert.userEmail !== DEV_EMAIL) continue;

      // Skip when this symbol's market is currently closed — avoids firing on
      // a stale weekend price snapshot/candle.
      const sym = alert.pairSymbol;
      if ((sym === 'XAU' || sym === 'XAG') && !isMetalsOpen()) continue;
      if (FOREX_PAIRS.has(sym) && !isForexOpen()) continue;

      const close = getCandleClose(alert.pairSymbol, alert.timeframe);
      log(`    checking ${alert.pairSymbol} ${alert.timeframe} close=${close} target=${alert.targetPrice} dir=${alert.direction} mode=${mode}`);
      if (!close || close <= 0) continue;

      const target = parseFloat(alert.targetPrice);

      if (mode === 'repeating') {
        checkRepeatingCrossing(alert, close);
        continue;
      }

      let hit = false;
      if (alert.direction === 'above' && close >= target) hit = true;
      if (alert.direction === 'below' && close <= target) hit = true;

      if (hit) {
        log(`🎯 Candle close alert triggered: ${alert.pairSymbol} ${alert.direction} ${target} (close=${close}) user=${alert.userId}`);
        recentlyTriggered.add(alert.id);
        processTriggeredAlert(alert, close).catch(e => {
          err(`processTriggeredAlert error: ${e.message}`);
          recentlyTriggered.delete(alert.id);
        });
      }
    } catch(e) { warn(`checkCandleCloseAlerts error: ${e.message}`); }
  }
}

function getCandleClose(sym, tf) {
  if (sym === 'XAU' || sym === 'XAG') {
    // Use live price snapshot taken at the exact minute boundary — matches chart candle close.
    // Falls back to current live price if snapshot missing (e.g. server just started).
    return metalCloseSnapshot[sym]?.[tf] || livePrice[sym] || 0;
  }
  return m1Candle[sym]?.byTf?.[tf]?.close || 0;
}


function startMinuteBoundaryChecker() {
  let lastCloseMin = -1;
  setInterval(() => {
    const now    = Date.now();
    const secMs  = now % 60000;
    const minNow = Math.floor(now / 60000);
    // Fire within first 500ms of the new minute — snapshot live prices right at the boundary
    // so metalCloseSnapshot holds the closest possible value to the actual candle close.
    // No delay needed since we no longer rely on Capital.com WebSocket candle rotation.
    if (secMs < 500 && lastCloseMin !== minNow) {
      lastCloseMin = minNow;
      // Snapshot XAU/XAG live price at this exact moment for all timeframes closing now
      const closedM5  = minNow % 5  === 0;
      const closedM15 = minNow % 15 === 0;
      const closedH1  = minNow % 60 === 0;
      for (const sym of ['XAU', 'XAG']) {
        const price = livePrice[sym];
        if (price > 0) {
          metalCloseSnapshot[sym]['M1'] = price;
          if (closedM5)  metalCloseSnapshot[sym]['M5']  = price;
          if (closedM15) metalCloseSnapshot[sym]['M15'] = price;
          if (closedH1)  metalCloseSnapshot[sym]['H1']  = price;
        }
      }
      onMinuteClose().catch(e => warn(`onMinuteClose error: ${e.message}`));
      try { onIndicatorBoundary(minNow); } catch (e) { warn(`onIndicatorBoundary error: ${e.message}`); }
    }
  }, 500);
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 10 — TRIGGER PROCESSING
// ════════════════════════════════════════════════════════════════════════════

// ── Layout name lookup (admin panel trigger log only) ───────────────────────
// Alerts store a layout ID ("main" or "lay_xxxxxxxx"); the readable name lives in
// users/{uid}.layouts = [{id, name}]. Cached per user for 5 minutes so a burst of
// triggers costs at most one read. Any failure falls back to the ID — a trigger
// must never fail because of a name lookup.
const layoutNameCache = new Map(); // userId -> { at, names: { id: name } }
async function getLayoutName(userId, layoutId) {
  if (!layoutId || layoutId === 'main') return 'Main';
  try {
    let entry = layoutNameCache.get(userId);
    if (!entry || Date.now() - entry.at > 5 * 60 * 1000) {
      const token = await getAccessToken();
      const doc = await fetchJson(
        `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/users/${userId}`,
        { headers: { 'Authorization': `Bearer ${token}` } }
      );
      const names = {};
      const vals = doc?.fields?.layouts?.arrayValue?.values || [];
      for (const v of vals) {
        const f = v?.mapValue?.fields;
        if (f?.id?.stringValue && f?.name?.stringValue) names[f.id.stringValue] = f.name.stringValue;
      }
      entry = { at: Date.now(), names };
      layoutNameCache.set(userId, entry);
    }
    return entry.names[layoutId] || layoutId;
  } catch (e) {
    return layoutId;
  }
}

async function processTriggeredAlert(alert, hitPrice) {
  const alertId = alert.id;
  const userId  = alert.userId;
  const hitTime = Date.now();

  delete activeAlerts[alertId];

  // 1. Delete from RTDB
  try {
    await rtdbDelete(`alerts/${userId}/${alertId}`);
    log(`  RTDB deleted: ${alertId}`);
  } catch(e) { warn(`  RTDB delete failed: ${e.message}`); }

  // 2. Delete from Firestore active_alerts field
  // Strategy: read the full document, remove the field locally, write entire fields map back.
  // This is the only 100% reliable way via REST — the commit/updateMask approach
  // silently succeeds (returns writeResults) but sometimes doesn't delete the field.
  try {
    const token   = await getAccessToken();
    const docUrl  = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/alerts/${userId}/active_alerts/alerts`;
    const authHdr = { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' };

    // Read current document
    const readRes = await fetchJson(docUrl, { headers: authHdr });
    if (readRes?.error) {
      warn(`  Firestore active_alerts read error: ${JSON.stringify(readRes.error)}`);
    } else {
      // Remove the target field from the fields map
      const fields = { ...(readRes?.fields || {}) };
      if (fields[alertId]) {
        delete fields[alertId];
        // Write back the full fields map — this overwrites the entire document
        // so the deleted field is guaranteed gone
        const writeRes = await fetchJson(docUrl, {
          method:  'PATCH',
          headers: authHdr,
          body:    JSON.stringify({ fields })
        });
        if (writeRes?.error) {
          warn(`  Firestore active_alerts write-back error: ${JSON.stringify(writeRes.error)}`);
        } else {
          // Verify field is actually gone
          if (writeRes?.fields?.[alertId]) {
            warn(`  Firestore active_alerts delete STILL present after write-back: ${alertId}`);
          } else {
            log(`  Firestore active_alerts field deleted: ${alertId}`);
          }
        }
      } else {
        log(`  Firestore active_alerts field already absent: ${alertId}`);
      }
    }
  } catch(e) { warn(`  Firestore active_alerts delete failed: ${e.message}`); }

  // 3. Write to Firestore history field
  try {
    const token    = await getAccessToken();
    // layout (folder) is kept on the history entry; alerts created before layouts existed → 'main'
    const hitAlert = { ...alert, triggered: true, hitAt: hitTime, layout: alert.layout || 'main' };
    delete hitAlert._emaSide;
    delete hitAlert._emaWait;
    const docPath  = `projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/alerts/${userId}/history/history`;
    const res3 = await fetchJson(
      `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`,
      {
        method:  'POST',
        headers: { 'Authorization': `Bearer ${token}` },
        body:    JSON.stringify({
          writes: [{
            update: {
              name:   docPath,
              fields: { [alertId]: { stringValue: JSON.stringify(hitAlert) } }
            },
            updateMask: { fieldPaths: ['`' + alertId + '`'] }
          }]
        })
      }
    );
    if (res3?.error) warn(`  Firestore history write error: ${JSON.stringify(res3.error)}`);
    else log(`  Firestore history written: ${alertId}`);
  } catch(e) { warn(`  Firestore history write failed: ${e.message}`); }

  // 3b. Log the trigger into the admin panel's daily tree — same date-doc
  // convention as the app's own admin_panel writes (Pakistan calendar day),
  // so the dashboard's "Alerts Triggered Today" count and per-user activity
  // popup pick this up automatically. admin_panel records are native
  // Firestore maps (the app writes them via a plain Map<String,Object> with
  // SetOptions.merge()) — NOT the JSON-string-per-field convention used by
  // active_alerts/history, so this needs typed Firestore field values.
  try {
    const token   = await getAccessToken();
    const dateDoc = pkDateOnly(hitTime);
    const docPath = `projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/admin_panel/${dateDoc}/alerts/alerts_trigger`;
    const triggerMapFields = {
      userId:      { stringValue: String(alert.userId || userId || '') },
      pair:        { stringValue: String(alert.pairSymbol || alert.pair || '') },
      targetPrice: { doubleValue: Number(alert.targetPrice) || 0 },
      candleClose: { booleanValue: !!alert.candleClose },
      alertKind:   { stringValue: alert.alertKind === 'indicator' ? 'indicator' : 'price' },
      layoutId:    { stringValue: alert.layout || 'main' },
      layout:      { stringValue: await getLayoutName(userId, alert.layout || 'main') },
      triggerTime: { stringValue: pkTimeFull(hitTime) },
    };
    const res4 = await fetchJson(
      `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`,
      {
        method:  'POST',
        headers: { 'Authorization': `Bearer ${token}` },
        body:    JSON.stringify({
          writes: [{
            update: {
              name:   docPath,
              fields: { [alertId]: { mapValue: { fields: triggerMapFields } } }
            },
            updateMask: { fieldPaths: ['`' + alertId + '`'] }
          }]
        })
      }
    );
    if (res4?.error) warn(`  Firestore admin_panel trigger-log write error: ${JSON.stringify(res4.error)}`);
    else log(`  Firestore admin_panel trigger-log written: ${alertId}`);
  } catch(e) { warn(`  Firestore admin_panel trigger-log write failed: ${e.message}`); }

  // 4. (history_index removed — app reads history doc directly on login)

  // 5. Send FCM
  try {
    await sendFCM(userId, alert, hitPrice);
  } catch(e) { warn(`  FCM failed: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 10b — REPEATING ALERT TRIGGER PROCESSING
//
// Differs from processTriggeredAlert() in the important way: the alert is
// NEVER deleted from RTDB/Firestore active_alerts — it keeps monitoring.
// Each firing instead gets its own uniquely-keyed history entry
// ("<alertId>_<hitTime>"), and lastTriggeredAt is bumped everywhere (memory,
// RTDB, Firestore) so the cooldown survives a server restart.
// ════════════════════════════════════════════════════════════════════════════

async function processRepeatingTrigger(alert, hitPrice, crossSide, hitTarget) {
  // hitTarget (optional): the level that was hit — the EMA value for indicator alerts. Shown in
  // history / FCM / admin log; the live alert itself keeps targetPrice 0.
  const alertId    = alert.id;
  const userId     = alert.userId;
  const hitTime    = Date.now();
  const newCount   = (parseInt(alert.triggeredCount, 10) || 0) + 1;
  const historyId  = `${alertId}_rep`; // ONE stable history card per repeating alert

  // Update in-memory cache immediately — this IS the cached object (by
  // reference), so cooldown + count are correct for every check tick from here.
  alert.lastTriggeredAt = hitTime;
  alert.lastSide        = crossSide;
  alert.triggeredCount  = newCount;

  // 1. RTDB — write the WHOLE alert object back (not a partial PATCH of just
  // 3 fields). A PATCH only delivers the changed fields in its own SSE event,
  // and our own listener would otherwise merge that partial payload in as if
  // it were the full alert — silently losing targetPrice/pairSymbol/direction/
  // etc from the in-memory cache and making this alert go permanently silent.
  // A full PUT of the canonical in-memory object (runtime-only fields and
  // userId stripped — userId is implied by the path, not stored under it)
  // sidesteps that risk entirely: same shape as every other RTDB write in
  // this file, so there's nothing special to get wrong here.
  const rtdbOk = await withVerifiedRetry(`RTDB update (${alertId})`, async () => {
    const { _lastSide, _justReenabled, _emaSide, _emaWait, userId: _uid, ...persistable } = alert;
    await rtdbSet(`alerts/${userId}/${alertId}`, persistable);
    if (await rtdbRemoveIfDeleted(userId, alertId)) return true;   // user deleted it meanwhile → don't resurrect
    const check = await rtdbGet(`alerts/${userId}/${alertId}`);
    return !!check && check.lastTriggeredAt === hitTime && check.triggeredCount === newCount;
  });
  if (rtdbOk) log(`  RTDB confirmed (repeating): ${alertId} ×${newCount}`);

  // 2. Firestore active_alerts field — merge into the stored JSON, confirmed
  // by a read-back inside updateActiveAlertFirestoreField itself.
  const fsActiveOk = await withVerifiedRetry(`Firestore active_alerts update (${alertId})`,
    () => updateActiveAlertFirestoreField(userId, alertId,
        { lastTriggeredAt: hitTime, lastSide: crossSide, triggeredCount: newCount }));

  // 3. Firestore history — ONE stable card per repeating alert. Every firing
  // REPLACES this same field (new hit time + count) instead of adding a new
  // one, so the card just jumps back to the top of History on each trigger.
  const fsHistoryOk = await withVerifiedRetry(`Firestore history update (${historyId})`, async () => {
    const token    = await getAccessToken();
    // id deliberately differs from the live alert's id — the app keys its
    // history list off this "id" field, and reusing the live id would
    // collide with the active alert when the app syncs.
    const hitAlert = { ...alert, id: historyId, direction: crossSide, triggered: true,
                        hitAt: hitTime, active: false, triggeredCount: newCount,
                        layout: alert.layout || 'main' };
    if (hitTarget != null) hitAlert.targetPrice = hitTarget;
    delete hitAlert._lastSide;
    delete hitAlert._emaSide;
    delete hitAlert._emaWait;
    const docPath  = `projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/alerts/${userId}/history/history`;
    const commitUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`;
    const authHdr   = { 'Authorization': `Bearer ${token}` };

    const writeRes = await fetchJson(commitUrl, {
      method:  'POST',
      headers: authHdr,
      body:    JSON.stringify({
        writes: [{
          update: { name: docPath, fields: { [historyId]: { stringValue: JSON.stringify(hitAlert) } } },
          updateMask: { fieldPaths: ['`' + historyId + '`'] }
        }]
      })
    });
    if (writeRes?.error) { warn(`  Firestore history write error: ${JSON.stringify(writeRes.error)}`); return false; }

    // Read back to confirm — not just that Firestore accepted the commit,
    // but that THIS hit's time + count are what's actually stored now.
    const readUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/alerts/${userId}/history/history`;
    const readRes = await fetchJson(readUrl, { headers: authHdr });
    const raw = readRes?.fields?.[historyId]?.stringValue;
    if (!raw) return false;
    try {
      const parsed = JSON.parse(raw);
      return parsed.hitAt === hitTime && parsed.triggeredCount === newCount;
    } catch { return false; }
  });
  if (fsHistoryOk) log(`  Firestore history confirmed: ${historyId} ×${newCount}`);

  // 3b. Admin panel trigger log — unique key per firing (every trigger is its
  // own event for the admin stats), best-effort with one retry.
  await withVerifiedRetry(`Admin panel trigger log (${alertId})`, async () => {
    const token    = await getAccessToken();
    const dateDoc  = pkDateOnly(hitTime);
    const key      = `${alertId}_${hitTime}`;
    const docPath  = `projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/admin_panel/${dateDoc}/alerts/alerts_trigger`;
    const triggerMapFields = {
      userId:          { stringValue: String(alert.userId || userId || '') },
      pair:            { stringValue: String(alert.pairSymbol || '') },
      targetPrice:     { doubleValue: Number(hitTarget != null ? hitTarget : alert.targetPrice) || 0 },
      candleClose:     { booleanValue: !!alert.candleClose },
      alertKind:       { stringValue: alert.alertKind === 'indicator' ? 'indicator' : 'price' },
      repeating:       { booleanValue: true },
      triggeredCount:  { integerValue: String(newCount) },
      cooldownMinutes: { integerValue: String(parseInt(alert.cooldownMinutes, 10) || 5) },
      layoutId:        { stringValue: alert.layout || 'main' },
      layout:          { stringValue: await getLayoutName(userId, alert.layout || 'main') },
      triggerTime:     { stringValue: pkTimeFull(hitTime) },
    };
    const res = await fetchJson(
      `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`,
      {
        method:  'POST',
        headers: { 'Authorization': `Bearer ${token}` },
        body:    JSON.stringify({
          writes: [{
            update: { name: docPath, fields: { [key]: { mapValue: { fields: triggerMapFields } } } },
            updateMask: { fieldPaths: ['`' + key + '`'] }
          }]
        })
      }
    );
    return !res?.error;
  }, 1);

  // 4. Send FCM — pass the actual crossing direction + running count so the
  // notification/local history show the right thing for THIS firing.
  // Retrying is safe: the app's duplicate-guard compares hitTime against the
  // last hit it already recorded, so a resent copy of the same hitTime is a
  // no-op on the client even if the first one actually did arrive.
  const fcmOk = await withVerifiedRetry(`FCM send (${alertId})`, async () => {
    try {
      await sendFCM(userId, { ...alert, direction: crossSide, triggeredCount: newCount,
                              ...(hitTarget != null ? { targetPrice: hitTarget } : {}) }, hitPrice);
      return true;
    } catch (e) { warn(`  sendFCM threw: ${e.message}`); return false; }
  }, 2);

  if (!rtdbOk || !fsActiveOk || !fsHistoryOk || !fcmOk) {
    err(`⚠️  Repeating trigger for ${alertId} completed WITH FAILURES — `
      + `rtdb=${rtdbOk} fsActive=${fsActiveOk} fsHistory=${fsHistoryOk} fcm=${fcmOk}`);
  } else {
    log(`✅ Repeating trigger for ${alertId} fully confirmed (×${newCount})`);
  }
}

/**
 * Read-modify-write a single field of the active_alerts/alerts doc, merging
 * `updates` into the JSON object stored in that field's stringValue (the doc
 * stores one JSON-string field per alert, not native Firestore sub-fields),
 * then reads the field back to confirm the merged values actually landed.
 * Returns true only once that confirmation succeeds.
 */
async function updateActiveAlertFirestoreField(userId, alertId, updates) {
  const token   = await getAccessToken();
  const docUrl  = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/alerts/${userId}/active_alerts/alerts`;
  const authHdr = { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' };

  const readRes = await fetchJson(docUrl, { headers: authHdr });
  const raw = readRes?.fields?.[alertId]?.stringValue;
  if (!raw) { warn(`  updateActiveAlertFirestoreField: field missing for ${alertId}`); return false; }

  let obj;
  try { obj = JSON.parse(raw); } catch(e) { warn(`  updateActiveAlertFirestoreField parse failed: ${e.message}`); return false; }
  Object.assign(obj, updates);

  const writeRes = await fetchJson(
    `${docUrl}?updateMask.fieldPaths=${encodeURIComponent('`' + alertId + '`')}`,
    {
      method:  'PATCH',
      headers: authHdr,
      body:    JSON.stringify({ fields: { [alertId]: { stringValue: JSON.stringify(obj) } } })
    }
  );
  if (writeRes?.error) { warn(`  updateActiveAlertFirestoreField write error: ${JSON.stringify(writeRes.error)}`); return false; }

  // Confirm — re-read and check every updated key matches
  const verifyRes = await fetchJson(docUrl, { headers: authHdr });
  const verifyRaw = verifyRes?.fields?.[alertId]?.stringValue;
  if (!verifyRaw) return false;
  try {
    const verifyObj = JSON.parse(verifyRaw);
    for (const k of Object.keys(updates)) {
      if (verifyObj[k] !== updates[k]) return false;
    }
    log(`  Firestore active_alerts field confirmed (repeating): ${alertId}`);
    return true;
  } catch { return false; }
}



async function sendFCM(userId, alert, hitPrice) {
  const token   = await getAccessToken();
  const BASE    = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents`;
  const userDoc = await fetchJson(`${BASE}/users/${userId}`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const fcmToken = userDoc?.fields?.fcmToken?.stringValue;
  if (!fcmToken) { warn(`No FCM token for user ${userId}`); return; }

  const isAlarm  = alert.alarm === true;
  const isIndicator = alert.alertKind === 'indicator';
  const hitType  = isIndicator
    ? `${alert.candleClose ? 'Candle Confirmation' : 'Touch'} · ${alert.indicatorType === 'rsi' ? `RSI ${alert.indicatorPeriod} = ${alert.indicatorTarget}` : alert.indicatorType === 'vwap' ? 'VWAP' : alert.indicatorMode === 'crossover' ? `${String(alert.indicatorType || 'ema').toUpperCase()} ${alert.indicatorPeriod} × ${alert.indicatorPeriod2} Cross` : `${String(alert.indicatorType || 'ema').toUpperCase()} ${alert.indicatorPeriod}`} · ${alert.timeframe}`
    : (alert.candleClose ? `Candle Close · ${alert.timeframe}` : 'Instant Hit');
  const priceStr = formatPrice(hitPrice, alert.pairSymbol);
  const dirLabel = alert.direction === 'above' ? '📈 Above' : '📉 Below';

  // Layout (folder) the alert lives in — id + display name for notification / alarm screen
  const layoutId = (alert.layout && String(alert.layout).trim()) ? String(alert.layout) : 'main';
  let layoutName = 'Main';
  if (layoutId !== 'main') {
    layoutName = 'Main'; // unknown / deleted layout falls back to Main (same as the app)
    const lays = userDoc?.fields?.layouts?.arrayValue?.values || [];
    for (const l of lays) {
      const f = l?.mapValue?.fields;
      if (f?.id?.stringValue === layoutId && f?.name?.stringValue) { layoutName = f.name.stringValue; break; }
    }
  }

  const message = {
    message: {
      token: fcmToken,
      data: {
        type:         'PRICE_ALERT',
        alertId:      alert.id,
        pairSymbol:   alert.pairSymbol,
        pairName:     alert.pairName,
        pairEmoji:    alert.pairEmoji || '',
        targetPrice:  String(alert.targetPrice),
        currentPrice: String(hitPrice),
        direction:    alert.direction,
        isAlarm:      String(isAlarm),
        hitType,
        hitTime:      String(Date.now()),
        isVibration:    String(alert.vibrationEnabled !== false),
        isSoundEnabled: String(alert.soundEnabled !== false),
        alertMode:       alert.alertMode === 'repeating' ? 'repeating' : 'once',
        cooldownMinutes: String(alert.cooldownMinutes || 5),
        triggeredCount:  String(alert.triggeredCount || 0),
        alertKind:       isIndicator ? 'indicator' : 'price',
        indicatorType:   String(alert.indicatorType || ''),
        indicatorMode:   String(alert.indicatorMode || ''),
        indicatorPeriod: String(alert.indicatorPeriod || 0),
        indicatorPeriod2: String(alert.indicatorPeriod2 || 0),
        indicatorTarget: String(alert.indicatorTarget || 0),
        timeframe:       String(alert.timeframe || ''),
        layout:          layoutId,
        layoutName:      layoutName,
      },
      // notification field intentionally omitted for ALL message types.
      // Sending a notification field causes Android to show a system-generated
      // notification (white box icon, no large icon, wrong channel) BEFORE
      // onMessageReceived runs. Data-only FCM = app handles everything correctly.
      // isAlarm=true  → AlarmRingingActivity full-screen alarm
      // isAlarm=false → AlarmQueue.showNotificationOnly with correct style + large icon
      android: { priority: 'high', direct_boot_ok: true }
    }
  };

  const res = await fetchJson(
    `https://fcm.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/messages:send`,
    {
      method:  'POST',
      headers: { 'Authorization': `Bearer ${token}` },
      body:    JSON.stringify(message)
    }
  );
  if (res?.name) log(`  FCM sent to ${userId}: ${res.name}`);
  else warn(`  FCM response: ${JSON.stringify(res)}`);
}

function formatPrice(price, sym) {
  if (!price) return '—';
  if (sym === 'XAU' || sym === 'XAG' || YAHOO_SYMBOLS[sym])
    return price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (price < 0.01) return price.toFixed(8);
  if (price < 1)    return price.toFixed(4);
  if (price < 100)  return price.toFixed(2);
  return price.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 11 — RTDB PRICE UPDATES (for mobile app display)
// ════════════════════════════════════════════════════════════════════════════

async function updateRtdbPrices() {
  if (!livePrice.XAU && !livePrice.XAG) return;
  // Skip on weekends — market closed, price is stale, no need to push to RTDB
  // (saves bandwidth/usage; mobile app simply keeps showing the last value it has)
  if (!isMetalsOpen()) return;
  try {
    await rtdbSet('prices', {
      xau: { current: livePrice.XAU || 0, updatedAt: Date.now() },
      xag: { current: livePrice.XAG || 0, updatedAt: Date.now() },
    });
  } catch(e) { warn(`RTDB price update failed: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 12 — HEALTH SERVER
// ════════════════════════════════════════════════════════════════════════════

function startHealthServer() {
  const port = process.env.PORT || 3000;
  http.createServer((req, res) => {
    if (voiceHandleRequest(req, res)) return;   // VOICE-FEATURE
    // Test email endpoint — hit this URL in browser to verify email alerts work
    if (req.url === '/test-email') {
      sendAlertEmail('test', 'TradeAlert server email test', 'This is a test email from your TradeAlert server.\nIf you received this, email alerts are working correctly.');
      // Reset cooldown immediately so a real alert can still fire
      delete emailCooldowns['test'];
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('Test email sent to ' + ALERT_TO + ' — check your inbox.');
      return;
    }

    // EMA engine state — secret-gated; compare these EMA values with TradingView to verify accuracy.
    if (req.url === '/admin/indicators') {
      const secret = req.headers['x-admin-secret'];
      if (!process.env.ADMIN_PANEL_SECRET || secret !== process.env.ADMIN_PANEL_SECRET) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'unauthorized' }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ...indDebugSnapshot(), updatedAt: Date.now() }));
      return;
    }

    // Full per-alert list, for the admin panel's Active Alerts browser page.
    // Same auth as /admin/active-alerts above; also free — this data is
    // already sitting in memory, this just serializes the fields the admin
    // panel actually needs (skips internal-only fields like sound/vibration
    // flags, alarm type, etc — those aren't shown there).
    if (req.url === '/admin/active-alerts-list') {
      const secret = req.headers['x-admin-secret'];
      if (!process.env.ADMIN_PANEL_SECRET || secret !== process.env.ADMIN_PANEL_SECRET) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'unauthorized' }));
        return;
      }
      const list = Object.entries(activeAlerts).map(([alertId, a]) => ({
        alertId,
        userId:      a.userId || '',
        userEmail:   a.userEmail || '',
        pairSymbol:  a.pairSymbol || '',
        pairName:    a.pairName || '',
        pairEmoji:   a.pairEmoji || '',
        targetPrice: a.targetPrice,
        direction:   a.direction || '',
        candleClose: !!a.candleClose,
        timeframe:   a.timeframe || '',
        label:       a.label || '',
        layout:      a.layout || 'main',
        createdAt:   a.createdAt || 0,
        alertMode:       a.alertMode === 'repeating' ? 'repeating' : 'once',
        cooldownMinutes: a.alertMode === 'repeating' ? (a.cooldownMinutes || 0) : 0,
        enabled:         a.enabled !== false, // false = repeating alert paused by the user
        alertKind:       a.alertKind === 'indicator' ? 'indicator' : 'price',
        indicatorType:   a.indicatorType || '',
        indicatorMode:   a.indicatorMode || '',
        indicatorPeriod: a.indicatorPeriod || 0,
        indicatorPeriod2: a.indicatorPeriod2 || 0,
        indicatorTarget: a.indicatorTarget || 0,
      }));
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ total: list.length, alerts: list, updatedAt: Date.now() }));
      return;
    }

    // Dedicated, secret-gated endpoint for the admin panel Worker — separate
    // from the public "/" health check below, which stays unauthenticated
    // on purpose for uptime monitors. Costs nothing extra: activeAlerts is
    // already held in memory for the price-monitoring loop, so this just
    // reads it, no new Firebase calls.
    if (req.url === '/admin/active-alerts') {
      const secret = req.headers['x-admin-secret'];
      if (!process.env.ADMIN_PANEL_SECRET || secret !== process.env.ADMIN_PANEL_SECRET) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'unauthorized' }));
        return;
      }
      const list = Object.values(activeAlerts);
      const breakdown = { instant: 0, candleClose: 0 };
      for (const a of list) (a.candleClose ? breakdown.candleClose++ : breakdown.instant++);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        total: list.length,
        breakdown,
        serverUptimeSeconds: Math.floor(process.uptime()),
        updatedAt: Date.now(),
      }));
      return;
    }

    const status = {
      ok:          true,
      alerts:      Object.keys(activeAlerts).length,
      prices:      `${Object.values(livePrice).filter(p => p > 0).length}/${Object.keys(livePrice).length}`,
      stopped:     serverStopped,
      gateWs:      gateWsConnected,
      gatePairs:   gateSubscribedPairs.size,
      weekend:     isWeekend(),
      xau:         livePrice.XAU,
      xag:         livePrice.XAG,
      btc:         livePrice.BTC,
      uptime:      Math.floor(process.uptime()),
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(status));
  }).listen(port, () => log(`Health server on port ${port}`));
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 12 — INDICATOR ENGINE (EMA alerts — Trade Alert Pro)
//
// Alerts with alertKind = "indicator" (indicatorType "ema", indicatorMode "single")
// carry NO fixed target price (targetPrice = 0). Instead the level is the EMA:
//
//     k = 2 / (N + 1)        EMA = close × k + previousEMA × (1 − k)
//
// ONE state per (pair, timeframe, period), shared by every user's alert with that
// combination — cost scales with distinct combinations, never with user count.
//
// History:  fetched ONCE per (pair, timeframe) when the first alert appears (and again
//           after a restart/deploy — it lives only in memory). After that, each candle
//           close fetches only the NEWEST candle(s) and updates the EMA recursively.
// Sources:  crypto → Gate.io · forex/indices → Yahoo · XAU/XAG → Capital.com REST history
//           (bootstrap only; after that metals use the same minute-boundary price
//           snapshot the existing candle-close alerts already use).
//
// Hit logic (side = which side of the EMA the price was on when the alert was created):
//   Touch               price reaches the EMA from its creation side. NOTE: the EMA line
//                       moves with the forming candle, but price − liveEMA always has the
//                       same sign as price − lastClosedEMA, so "price meets the live line"
//                       is exactly "price meets the last closed EMA value" — no extra math.
//   Candle Confirmation a candle CLOSES on the opposite side of the EMA (close vs the EMA
//                       value that includes that candle).
//   Once      → fires, alert removed.
//   Repeating → fires, side flips, waits for the opposite side again (+ cooldown), exactly
//               like price alerts. The side is persisted in RTDB (`lastSide`).
// ════════════════════════════════════════════════════════════════════════════

const IND_TF_MS = { M1: 60000, M5: 300000, M15: 900000, H1: 3600000 };
const IND_MIN_PERIOD = 3;
const IND_MAX_PERIOD = 500;
const IND_WARMUP_FACTOR = 5;      // history candles ≈ 5 × period (error from the SMA seed → ~0.005%)
const IND_MAX_PAGES = 8;          // safety cap on paged history requests
const IND_MAX_PARALLEL_BOOTS = 3; // never bootstrap more than this many groups at once
const IND_UNUSED_GRACE_MS = 10 * 60 * 1000; // keep a state this long after its last alert is gone

const GATE_INTERVAL    = { M1: '1m', M5: '5m', M15: '15m', H1: '1h' };
const YAHOO_INTERVAL   = { M1: '1m', M5: '5m', M15: '15m', H1: '60m' };
const CAP_RESOLUTION   = { M1: 'MINUTE', M5: 'MINUTE_5', M15: 'MINUTE_15', H1: 'HOUR' };
// Yahoo only keeps limited history per interval
const YAHOO_MAX_SPAN_MS = { M1: 6.5 * 86400000, M5: 58 * 86400000, M15: 58 * 86400000, H1: 700 * 86400000 };
const YAHOO_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'application/json'
};

// key `${sym}|${tf}` → group { sym, tf, tfMs, source, periods: Map(period → state), lastOpen, ready, ... }
const indGroups = new Map();
let indBootsRunning = 0;

// ── helpers ──────────────────────────────────────────────────────────────────

function indSource(sym) {
  if (GATE_SYMBOLS_SET.has(sym)) return 'gate';
  if (sym === 'XAU' || sym === 'XAG') return 'capital';
  if (YAHOO_SYMBOLS_KEYS.has(sym)) return 'yahoo';
  return null; // e.g. USDGBP (derived from GBPUSD) — not supported for indicators yet
}

function indPeriodOf(a) {
  const p = parseInt(a.indicatorPeriod, 10);
  return Number.isFinite(p) ? p : 0;
}

/** Crossover: second EMA period (indicatorPeriod = EMA 1, indicatorPeriod2 = EMA 2). */
function indPeriod2Of(a) {
  const p = parseInt(a.indicatorPeriod2, 10);
  return Number.isFinite(p) ? p : 0;
}
function indIsCross(a) { return a.indicatorMode === 'crossover'; }
function indTargetOf(a) { const t = parseFloat(a.indicatorTarget); return Number.isFinite(t) ? t : 0; }
function indTypeOf(a) { return IND_TYPES.has(a.indicatorType) ? a.indicatorType : 'ema'; }
/** State keys (type:period) this alert needs. */
function indKeysOf(a) { return indPeriodsOf(a).map(p => indStateKey(a.indicatorType, p)); }
/** Every EMA period this alert needs. */
function indPeriodsOf(a) { return indIsCross(a) ? [indPeriodOf(a), indPeriod2Of(a)] : [indPeriodOf(a)]; }
/** Short label for logs, e.g. "EMA50" or "EMA9×EMA21". */
function indLabel(a) { const T = String(a.indicatorType || 'ema').toUpperCase(); if (T === 'VWAP') return 'VWAP'; return indIsCross(a) ? `${T}${indPeriodOf(a)}×${T}${indPeriod2Of(a)}` : `${T}${indPeriodOf(a)}`; }
/** Signature of the indicator setup — if it changes (edit), the stored side must be forgotten. */
function indSetupSig(a) { return `${a.indicatorType}|${a.indicatorMode}|${indTargetOf(a)}|${indPeriodOf(a)}|${indIsCross(a) ? indPeriod2Of(a) : 0}|${a.timeframe}|${a.candleClose ? 1 : 0}`; }

/** True when this alert is an EMA alert (single or crossover) this engine can monitor. */
function indIsValid(a) {
  if (!a || a.alertKind !== 'indicator' || !IND_TYPES.has(a.indicatorType)) return false;
  if (a.indicatorMode !== 'single' && a.indicatorMode !== 'crossover') return false;
  if (!IND_TF_MS[a.timeframe] || !indSource(a.pairSymbol)) return false;
  if (a.indicatorType === 'vwap') return a.indicatorMode === 'single' && indSource(a.pairSymbol) === 'gate';   // VWAP: no inputs, crypto only (real volume)
  if (a.indicatorType === 'rsi') {                                         // RSI: single line, length 2–100, target 1–100, Touch only
    const n = indPeriodOf(a), t = indTargetOf(a);
    return a.indicatorMode === 'single' && !a.candleClose
        && n >= IND_RSI_MIN_LEN && n <= IND_RSI_MAX_LEN && t >= 1 && t <= 100;
  }
  const ps = indPeriodsOf(a);
  if (!ps.every(p => p >= IND_MIN_PERIOD && p <= IND_MAX_PERIOD)) return false;
  if (indIsCross(a) && ps[0] === ps[1]) return false;                    // two identical EMAs can never cross
  return true;
}

function indKey(sym, tf) { return `${sym}|${tf}`; }

function indMarketOpen(sym) {
  if (sym === 'XAU' || sym === 'XAG') return isMetalsOpen();
  if (YAHOO_SYMBOLS_KEYS.has(sym))    return isYahooSymbolOpen(sym);
  return true; // crypto trades 24/7
}

function indEmaFromCloses(closes, period) {
  const k = 2 / (period + 1);
  let ema = 0;
  for (let i = 0; i < period; i++) ema += closes[i];
  ema /= period;                                   // seed = SMA of the first N closes
  for (let i = period; i < closes.length; i++) ema = closes[i] * k + ema * (1 - k);
  return ema;
}

// ── indicator math ───────────────────────────────────────────────────────────
// Every state keeps:  val   = indicator value on the last CLOSED candle (candle-confirmation, display)
//                     level = price at which "price == live indicator" (Touch: price vs level has the
//                             same sign as price vs the live, forming-candle indicator value)
//   EMA : level = ema                       (price − liveEMA = (1−k)(price − ema))
//   SMA : level = (sum − oldest)/(N−1)      (price − liveSMA = (N−1)/N · (price − level))
//   DEMA: level = ((2−k)e1 − e2)/(1−k)      (price − liveDEMA = (1−k)² · (price − level))

const IND_TYPES = new Set(['ema', 'sma', 'dema', 'rsi', 'vwap']);
const IND_DAY_MS = 86400000;                      // VWAP session = UTC day (TradingView's crypto session)
const indSessionStart = t => Math.floor(t / IND_DAY_MS) * IND_DAY_MS;
const IND_RSI_MIN_LEN = 2, IND_RSI_MAX_LEN = 100;   // RSI length range (target range is 1–100)
const indStateKey = (type, p) => `${type}:${p}`;

/** Closed candles needed for the first calculation (EMA/DEMA converge, SMA is exact). */
function indNeedFor(type, p, tfMs) {
  if (type === 'vwap') return Math.max(3, Math.ceil((Date.now() - indSessionStart(Date.now())) / tfMs) + 2);   // today's candles only
  if (type === 'sma')  return p + 2;
  if (type === 'dema') return Math.max(p * 6, p * 2 + 50);
  if (type === 'rsi')  return Math.max(p * 14, p + 100);        // Wilder smoothing: (1−1/N)^k decays slowly
  return Math.max(p * IND_WARMUP_FACTOR, p + 50);
}
/** Fewest candles that give any value at all. */
function indMinFor(type, p) { return type === 'vwap' ? 1 : type === 'dema' ? 2 * p - 1 : (type === 'rsi' ? p + 1 : p); }

/** Wilder RSI from average gain / loss (same edge rules as TradingView ta.rsi). */
function indRsiFrom(ag, al) {
  if (al === 0) return 100;
  if (ag === 0) return 0;
  return 100 - 100 / (1 + ag / al);
}

/**
 * RSI "Touch" level: the price at which the LIVE RSI (forming candle) equals `target`.
 * Live RSI rises monotonically with price, so  RSI(price) >= target  ⇔  price >= level.
 * That lets RSI reuse the exact same price-vs-level Touch logic as every other indicator
 * (zero network, O(1) per second). Derivation: RS = T/(100−T); with change ch = price − lastClose
 *   ch >= 0 :  ag' = (ag(N−1)+ch)/N, al' = al(N−1)/N   →  ch = (N−1)(RS·al − ag)
 *   ch <  0 :  ag' = ag(N−1)/N, al' = (al(N−1)−ch)/N   →  ch = (N−1)(al − ag/RS)
 */
function indRsiLevel(st, target) {
  const T  = Math.min(Math.max(target, 0.0001), 99.9999);   // 100 would need RS = ∞
  const RS = T / (100 - T), N = st.period;
  const ch = (RS * st.al >= st.ag) ? (N - 1) * (RS * st.al - st.ag)
                                   : (N - 1) * (st.al - st.ag / RS);
  return st.lc + ch;
}

function indRefreshLevel(st) {
  if (st.type === 'vwap') {
    st.val = st.vv > 0 ? st.pv / st.vv : st.lastTp;          // no volume yet → typical price of the last candle
    st.level = st.val;                                       // Touch: price vs the VWAP of the last closed candle
  } else if (st.type === 'rsi') {
    st.val = indRsiFrom(st.ag, st.al);
    st.level = NaN;                                   // per-alert (depends on the alert's target) → indRsiLevel()
  } else if (st.type === 'sma') {
    st.val   = st.sum / st.period;
    st.level = (st.sum - st.buf[0]) / (st.period - 1);
  } else if (st.type === 'dema') {
    st.val   = 2 * st.e1 - st.e2;
    st.level = ((2 - st.k) * st.e1 - st.e2) / (1 - st.k);
  } else {
    st.val = st.level = st.e1;
  }
}

/** Builds the state values from ascending closes. Caller guarantees closes.length >= indMinFor(). */
function indInitState(st, closes, candles) {
  const n = st.period, k = st.k;
  if (st.type === 'vwap') {
    // Session-anchored VWAP, source hlc3: sum(hlc3·volume) / sum(volume) over today's (UTC) candles
    const last = candles[candles.length - 1];
    st.day = indSessionStart(last.t); st.pv = 0; st.vv = 0; st.lastTp = last.c;
    for (const c of candles) {
      if (c.t < st.day) continue;
      const tp = (c.h + c.l + c.c) / 3, v = c.v > 0 ? c.v : 0;
      st.pv += tp * v; st.vv += v; st.lastTp = tp;
    }
  } else if (st.type === 'rsi') {
    let ag = 0, al = 0;
    for (let i = 1; i <= n; i++) { const d = closes[i] - closes[i - 1]; if (d > 0) ag += d; else al -= d; }
    ag /= n; al /= n;                                                          // seed = simple average of first N changes
    for (let i = n + 1; i < closes.length; i++) {
      const d = closes[i] - closes[i - 1];
      ag = (ag * (n - 1) + (d > 0 ? d : 0)) / n;
      al = (al * (n - 1) + (d < 0 ? -d : 0)) / n;
    }
    st.ag = ag; st.al = al; st.lc = closes[closes.length - 1];
  } else if (st.type === 'sma') {
    st.buf = closes.slice(-n);
    st.sum = st.buf.reduce((a, b) => a + b, 0);
  } else if (st.type === 'dema') {
    let e1 = 0; for (let i = 0; i < n; i++) e1 += closes[i]; e1 /= n;         // e1 seed = SMA of first N closes
    const e1s = [e1];
    for (let i = n; i < closes.length; i++) { e1 = closes[i] * k + e1 * (1 - k); e1s.push(e1); }
    let e2 = 0; for (let i = 0; i < n; i++) e2 += e1s[i]; e2 /= n;             // e2 seed = SMA of first N e1 values
    for (let i = n; i < e1s.length; i++) e2 = e1s[i] * k + e2 * (1 - k);
    st.e1 = e1; st.e2 = e2;
  } else {
    st.e1 = indEmaFromCloses(closes, n);
  }
  indRefreshLevel(st);
}

/** One more closed candle. */
function indStepState(st, c, candle) {
  if (st.type === 'vwap') {
    const day = indSessionStart(candle.t);
    if (day !== st.day) { st.day = day; st.pv = 0; st.vv = 0; st.reset = true; }   // new session → VWAP starts over
    const tp = (candle.h + candle.l + candle.c) / 3, v = candle.v > 0 ? candle.v : 0;
    st.pv += tp * v; st.vv += v; st.lastTp = tp;
  } else if (st.type === 'rsi') {
    const d = c - st.lc;
    st.ag = (st.ag * (st.period - 1) + (d > 0 ? d : 0)) / st.period;
    st.al = (st.al * (st.period - 1) + (d < 0 ? -d : 0)) / st.period;
    st.lc = c;
  } else if (st.type === 'sma') {
    st.buf.push(c);
    if (st.buf.length > st.period) st.buf.shift();
    st.sum = st.buf.reduce((x, y) => x + y, 0);          // recomputed (N ≤ 500) → no float drift
  } else if (st.type === 'dema') {
    st.e1 = c * st.k + st.e1 * (1 - st.k);
    st.e2 = st.e1 * st.k + st.e2 * (1 - st.k);
  } else {
    st.e1 = c * st.k + st.e1 * (1 - st.k);
  }
  indRefreshLevel(st);
}

/** Indicator value if the forming candle closed at `price` right now (live, like TradingView). Zero network. */
function indLiveValue(st, price) {
  if (st.type === 'vwap') return st.val;                // forming-candle volume is unknown → last closed VWAP
  if (st.type === 'rsi') {
    const d = price - st.lc;
    return indRsiFrom((st.ag * (st.period - 1) + (d > 0 ? d : 0)) / st.period,
                      (st.al * (st.period - 1) + (d < 0 ? -d : 0)) / st.period);
  }
  if (st.type === 'sma')  return (st.sum - st.buf[0] + price) / st.period;
  if (st.type === 'dema') {
    const e1 = price * st.k + st.e1 * (1 - st.k);
    const e2 = e1 * st.k + st.e2 * (1 - st.k);
    return 2 * e1 - e2;
  }
  return price * st.k + st.e1 * (1 - st.k);
}

// ── candle fetchers (each returns ascending [{t: openTimeMs, c: close}], may include forming candle) ──

async function indFetchGate(sym, tf, fromMs, toMs) {
  const tfMs = IND_TF_MS[tf];
  const gSym = GATE_SYMBOLS[sym];
  const out  = [];
  let start  = Math.ceil(fromMs / tfMs) * tfMs;
  let pages  = 0;
  while (start <= toMs) {
    if (++pages > IND_MAX_PAGES) throw new Error('gap too large (pages)');
    const end = Math.min(toMs, start + 999 * tfMs);         // ≤ 1000 points per request
    const url = `https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair=${gSym}`
              + `&interval=${GATE_INTERVAL[tf]}&from=${Math.floor(start / 1000)}&to=${Math.floor(end / 1000)}`;
    const res = await fetchJson(url);
    if (!Array.isArray(res)) throw new Error(`Gate candles bad response: ${JSON.stringify(res).slice(0, 120)}`);
    for (const r of res) {                                   // [t, quoteVol, close, high, low, open, baseVol, closed]
      const t = parseInt(r[0], 10) * 1000;
      const c = parseFloat(r[2]);
      if (t >= fromMs && c > 0) out.push({ t, c, h: parseFloat(r[3]) || c, l: parseFloat(r[4]) || c, v: parseFloat(r[6]) || 0 });   // r[6] = base-currency volume
    }
    start = end + tfMs;
  }
  return out;
}

async function indFetchYahoo(sym, tf, fromMs, toMs) {
  if (toMs - fromMs > YAHOO_MAX_SPAN_MS[tf]) throw new Error('gap too large (yahoo span)');
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${YAHOO_SYMBOLS[sym]}`
            + `?interval=${YAHOO_INTERVAL[tf]}&period1=${Math.floor(fromMs / 1000)}&period2=${Math.ceil(toMs / 1000) + 60}`;
  const res    = await fetchJson(url, { headers: YAHOO_HEADERS });
  const result = res?.chart?.result?.[0];
  const times  = result?.timestamp;
  const closes = result?.indicators?.quote?.[0]?.close;
  if (!result) {
    // Yahoo answers {chart:{result:null,error:{...}}} when the window holds no candles — not a failure
    if (res?.chart?.error && /no data|not found/i.test(JSON.stringify(res.chart.error))) return [];
    throw new Error(`Yahoo candles bad response: ${JSON.stringify(res).slice(0, 120)}`);
  }
  const out = [];
  if (Array.isArray(times) && Array.isArray(closes)) {
    for (let i = 0; i < times.length; i++) {
      const c = closes[i];
      if (c != null && c > 0) out.push({ t: times[i] * 1000, c });
    }
  }
  return out;
}

async function indFetchCapital(sym, tf, fromMs, toMs) {
  if (!capCst || !capToken) throw new Error('Capital.com session not ready');
  const tfMs  = IND_TF_MS[tf];
  const epic  = sym === 'XAU' ? 'GOLD' : 'SILVER';
  const iso   = ms => new Date(ms).toISOString().slice(0, 19);
  const out   = [];
  let start   = Math.ceil(fromMs / tfMs) * tfMs;
  let pages   = 0;
  while (start <= toMs) {
    if (++pages > IND_MAX_PAGES) throw new Error('gap too large (pages)');
    const end = Math.min(toMs, start + 999 * tfMs);
    const url = `${CAP_REST_URL}/api/v1/prices/${epic}?resolution=${CAP_RESOLUTION[tf]}&max=1000`
              + `&from=${iso(start)}&to=${iso(end)}`;
    const res = await fetchJson(url, { headers: { 'CST': capCst, 'X-SECURITY-TOKEN': capToken } });
    if (!res || !Array.isArray(res.prices)) {
      throw new Error(`Capital candles bad response: ${JSON.stringify(res).slice(0, 140)}`);
    }
    for (const p of res.prices) {
      const ts = String(p.snapshotTimeUTC || '');
      const t  = Date.parse(/Z$|[+-]\d\d:?\d\d$/.test(ts) ? ts : ts + 'Z');
      const c  = parseFloat(p.closePrice?.bid);              // live metal price is the bid too
      if (Number.isFinite(t) && c > 0 && t >= fromMs) out.push({ t, c });
    }
    start = end + tfMs;
  }
  return out;
}

/** Closed candles only (forming candle dropped), ascending, de-duplicated. */
async function indFetchCandles(sym, tf, fromMs, toMs) {
  const src = indSource(sym);
  let raw;
  if (src === 'gate')         raw = await indFetchGate(sym, tf, fromMs, toMs);
  else if (src === 'yahoo')   raw = await indFetchYahoo(sym, tf, fromMs, toMs);
  else if (src === 'capital') raw = await indFetchCapital(sym, tf, fromMs, toMs);
  else throw new Error(`no candle source for ${sym}`);

  const tfMs = IND_TF_MS[tf];
  const now  = Date.now();
  const seen = new Set();
  return raw
    .filter(c => c.t + tfMs <= now && !seen.has(c.t) && seen.add(c.t))
    .sort((a, b) => a.t - b.t);
}

/** History for the first calculation: ~5 × period closed candles ending now. */
async function indFetchHistory(sym, tf, need) {
  const tfMs = IND_TF_MS[tf];
  const now  = Date.now();
  if (indSource(sym) === 'yahoo') {
    // One request. Forex trades ~5/7 of the week; indices only a few hours a day.
    const calFactor = FOREX_PAIRS.has(sym) ? 1.5 : 5.5;
    const span = Math.min(need * tfMs * calFactor, YAHOO_MAX_SPAN_MS[tf]);
    return (await indFetchCandles(sym, tf, now - span, now)).slice(-need);
  }
  let out = [];
  let to  = now;
  for (let page = 0; page < IND_MAX_PAGES && out.length < need; page++) {
    const from = to - 999 * tfMs;
    const part = await indFetchCandles(sym, tf, from, to);
    out = part.concat(out);
    to  = from - tfMs;
  }
  return out.slice(-need);
}

// ── readable logs (every indicator type) ─────────────────────────────────────

const indTypeName = a => String(a.indicatorType || 'ema').toUpperCase();

/** "RSI 14 → 70", "EMA 50", "SMA 9 × 21", "VWAP" */
function indWhat(a) {
  const T = indTypeName(a);
  if (T === 'VWAP') return 'VWAP';
  if (T === 'RSI')  return `RSI ${indPeriodOf(a)} → ${indTargetOf(a)}`;
  if (indIsCross(a)) return `${T} ${indPeriodOf(a)} × ${indPeriod2Of(a)}`;
  return `${T} ${indPeriodOf(a)}`;
}
/** "Touch" / "Candle Confirmation" (RSI is always Touch) */
const indHitName = a => (a.candleClose && a.indicatorType !== 'rsi') ? 'Candle Confirmation' : 'Touch';
/** "once" / "repeating (5m cooldown)" */
const indModeName = a => a.alertMode === 'repeating' ? `repeating (${a.cooldownMinutes || 0}m cooldown)` : 'once';

/** Current indicator value(s) from memory, e.g. "RSI=52.31" / "EMA9=101.2 EMA21=100.8" — null while history is loading. */
function indCurrentText(a) {
  const g = indGroups.get(indKey(a.pairSymbol, a.timeframe));
  if (!g || !g.ready) return null;
  const sts = indKeysOf(a).map(k => g.periods.get(k));
  if (sts.some(st => !st || !st.ready)) return null;
  const T = indTypeName(a);
  const fmt = v => (Math.abs(v) >= 100 ? v.toFixed(2) : v.toFixed(5));
  let txt;
  if (T === 'VWAP') txt = `VWAP=${fmt(sts[0].val)}`;
  else if (T === 'RSI') txt = `RSI=${sts[0].val.toFixed(2)}`;
  else txt = sts.map(st => `${T}${st.period}=${fmt(st.val)}`).join(' ');
  const px = livePrice[a.pairSymbol];
  return px > 0 ? `${txt} · price=${px}` : txt;
}

const indLoggedAlerts = new Map();   // alert id → setup signature we already logged

/** One clear line per new / edited indicator alert, written as soon as its values are known. */
function indLogNewAlerts() {
  const alive = new Set();
  for (const a of Object.values(activeAlerts)) {
    if (!indIsValid(a)) continue;
    alive.add(a.id);
    const sig = indSetupSig(a);
    const prev = indLoggedAlerts.get(a.id);
    if (prev === sig) continue;
    const cur = indCurrentText(a);
    if (!cur) continue;                                        // history still loading → logged on a later sync
    indLoggedAlerts.set(a.id, sig);
    log(`🆕 ${indTypeName(a)} alert ${prev ? 'updated' : 'created'}: ${a.pairSymbol} · ${indWhat(a)} · ${a.timeframe} · ${indHitName(a)} · ${indModeName(a)} · current ${cur} · user=${a.userId} id=${a.id}`);
  }
  for (const id of indLoggedAlerts.keys()) if (!alive.has(id)) indLoggedAlerts.delete(id);
}

// ── group lifecycle ──────────────────────────────────────────────────────────

/** Runs every 5s (pure in-memory): creates states for new alerts, drops unused ones, starts bootstraps. */
function syncIndicatorStates() {
  const wanted = new Map(); // key → Set(periods)
  for (const a of Object.values(activeAlerts)) {
    if (!indIsValid(a)) continue;
    const key = indKey(a.pairSymbol, a.timeframe);
    if (!wanted.has(key)) wanted.set(key, new Set());
    for (const k of indKeysOf(a)) wanted.get(key).add(k);
  }
  const now = Date.now();

  for (const [key, periods] of wanted) {
    let g = indGroups.get(key);
    if (!g) {
      const [sym, tf] = key.split('|');
      g = { key, sym, tf, tfMs: IND_TF_MS[tf], source: indSource(sym), periods: new Map(),
            lastOpen: 0, ready: false, loading: false, updating: false, dirty: false,
            fails: 0, nextTryAt: 0 };
      indGroups.set(key, g);
      log(`📈 Indicator group created: ${key} (${g.source})`);
    }
    for (const sk of periods) {
      let st = g.periods.get(sk);
      if (!st) {
        const [type, ps] = sk.split(':'); const p = parseInt(ps, 10);
        st = { type, period: p, k: 2 / (p + 1), val: 0, level: 0, ready: false, lastWantedAt: now };
        g.periods.set(sk, st);
        g.dirty = true;                       // a new period needs (at most) one history reload
      }
      st.lastWantedAt = now;
    }
    if ((!g.ready || g.dirty) && !g.loading && now >= g.nextTryAt && indBootsRunning < IND_MAX_PARALLEL_BOOTS) {
      indBootstrapGroup(g).catch(e => warn(`Indicator bootstrap crash ${g.key}: ${e.message}`));
    }
  }

  // Cleanup — states/groups nobody uses any more
  for (const [key, g] of indGroups) {
    for (const [sk, st] of g.periods) {
      if (now - st.lastWantedAt > IND_UNUSED_GRACE_MS) g.periods.delete(sk);
    }
    if (g.periods.size === 0) { indGroups.delete(key); log(`📈 Indicator group removed: ${key}`); }
  }
  indLogNewAlerts();
}

async function indBootstrapGroup(g) {
  g.loading = true;
  indBootsRunning++;
  try {
    const need = Math.max(...[...g.periods.values()].map(st => indNeedFor(st.type, st.period, g.tfMs)));
    const candles = await indFetchHistory(g.sym, g.tf, need);
    if (candles.length === 0) throw new Error('no candles returned');

    const closes = candles.map(c => c.c);
    const notes  = [];
    for (const st of g.periods.values()) {
      if (closes.length < indMinFor(st.type, st.period)) {
        st.ready = false;
        notes.push(`${st.type.toUpperCase()}${st.period}: only ${closes.length} candles available — not ready`);
        continue;
      }
      indInitState(st, closes, candles);
      st.ready = true;
      st.warm  = closes.length;
    }
    g.lastOpen = candles[candles.length - 1].t;
    g.lastCandle = candles[candles.length - 1];
    g.ready = true; g.dirty = false; g.fails = 0;
    const summary = [...g.periods.values()].map(s => `${s.type.toUpperCase()}${s.period}=${s.ready ? s.val.toFixed(5) : 'n/a'}`).join(' ');
    log(`📈 Indicator bootstrap ${g.key}: ${candles.length} candles (need ${need}) lastOpen=${new Date(g.lastOpen).toISOString()} ${summary}`);
    notes.forEach(n => warn(`  ${g.key} ${n}`));
  } catch (e) {
    g.fails++;
    g.nextTryAt = Date.now() + Math.min(30000 * Math.pow(2, g.fails - 1), 5 * 60000);
    warn(`Indicator bootstrap failed ${g.key} (attempt ${g.fails}): ${e.message} — retry in ${Math.round((g.nextTryAt - Date.now()) / 1000)}s`);
    if (g.fails === 5) {
      sendAlertEmail(`ind_boot_${g.key}`, `Indicator history failing for ${g.key}`,
        `The indicator engine could not load candle history for ${g.key} after 5 attempts.\nLast error: ${e.message}\nIndicator alerts on this pair/timeframe cannot trigger until it succeeds.`);
    }
  } finally {
    g.loading = false;
    indBootsRunning--;
  }
  // Candles that closed while history was loading → one small catch-up fetch
  // (only when a candle really closed since the history snapshot — otherwise it's a wasted request)
  if (g.ready && g.source !== 'capital' && Date.now() >= g.lastOpen + 2 * g.tfMs) indUpdateGroup(g).catch(() => {});
}

// ── candle-close updates ─────────────────────────────────────────────────────

/** VWAP just started a new session: re-measure every VWAP alert's side from the live price, so the reset itself never counts as a crossing. */
function indRebaseVwap(g, st) {
  const price = livePrice[g.sym];
  for (const alert of Object.values(activeAlerts)) {
    if (alert.indicatorType !== 'vwap' || alert.pairSymbol !== g.sym || alert.timeframe !== g.tf || !indIsValid(alert)) continue;
    if (!(price > 0)) { alert._emaSide = null; continue; }                // re-baselined on the next check
    alert._emaSide = price >= st.val ? 'above' : 'below';
    alert._emaWait = false;
    alert.lastSide = alert._emaSide;
    if (activeAlerts[alert.id]) {
      rtdbSet(`alerts/${alert.userId}/${alert.id}/lastSide`, alert._emaSide)
        .then(() => rtdbRemoveIfDeleted(alert.userId, alert.id)).catch(() => {});
    }
  }
  log(`  🔄 VWAP ${g.key} new session (UTC day) — alert sides re-measured`);
}

function indApplyCandle(g, candle) {
  for (const st of g.periods.values()) {
    if (!st.ready) continue;
    indStepState(st, candle.c, candle);
    if (st.reset) { st.reset = false; indRebaseVwap(g, st); }
  }
  g.lastOpen   = candle.t;
  g.lastCandle = candle;
}

/** Gate / Yahoo: fetch only candles newer than the last one we hold, update every period, evaluate. */
async function indUpdateGroup(g) {
  if (!g.ready || g.loading || g.updating) return false;
  g.updating = true;
  try {
    const candles = await indFetchCandles(g.sym, g.tf, g.lastOpen + g.tfMs, Date.now());
    const fresh = candles.filter(c => c.t > g.lastOpen);
    if (!fresh.length) return false;
    for (const c of fresh) indApplyCandle(g, c);
    indEvaluateCandleConfirm(g, g.lastCandle);
    return true;
  } catch (e) {
    warn(`EMA update failed ${g.key}: ${e.message}`);
    if (/gap too large/.test(e.message)) { g.ready = false; g.nextTryAt = 0; } // too far behind → reload history
    return false;
  } finally {
    g.updating = false;
  }
}

/** Metals: same minute-boundary price snapshot the existing candle-close alerts use (no REST per close). */
function indUpdateMetal(g, boundaryMs) {
  if (!g.ready || g.loading) return;
  const expectedOpen = boundaryMs - g.tfMs;
  if (g.lastOpen >= expectedOpen) return;                 // history already contained this candle
  const price = metalCloseSnapshot[g.sym]?.[g.tf];
  if (!(price > 0)) return;
  indApplyCandle(g, { t: expectedOpen, c: price });
  indEvaluateCandleConfirm(g, g.lastCandle);
}

/** Called from the minute-boundary checker (right after the metal snapshots are taken). */
function onIndicatorBoundary(minNow) {
  if (!indGroups.size) return;
  const boundaryMs = minNow * 60000;
  for (const g of indGroups.values()) {
    if (!g.ready) continue;
    // Yahoo hourly bars of indices start at :15/:30, so poll those every 15 min
    const everyMin = (g.source === 'yahoo' && g.tf === 'H1') ? 15 : g.tfMs / 60000;
    if (minNow % everyMin !== 0) continue;
    if (!indMarketOpen(g.sym)) continue;                  // closed market → no requests at all
    if (g.source === 'capital') { indUpdateMetal(g, boundaryMs); continue; }

    const aligned      = boundaryMs % g.tfMs === 0;
    const expectedOpen = boundaryMs - g.tfMs;
    // The data provider may publish the just-closed candle a few seconds late → retry a few times
    const delays = aligned ? [1500, 5000, 12000, 25000, 45000] : [3000];
    delays.forEach(d => setTimeout(() => {
      if (!indGroups.has(g.key)) return;
      if (aligned && g.lastOpen >= expectedOpen) return;  // already have it
      indUpdateGroup(g).catch(() => {});
    }, d + Math.floor(Math.random() * 700)));              // jitter so groups don't hit APIs at the same instant
  }
}

// ── hit evaluation ───────────────────────────────────────────────────────────

/** Which side of the EMA the alert started on. Persisted in RTDB as `lastSide` so restarts don't flip it. */
function indEnsureBaseline(alert, ema) {
  if (alert._emaSide) return true;
  if (alert.lastSide === 'above' || alert.lastSide === 'below') {
    alert._emaSide = alert.lastSide;
    return true;
  }
  const p = parseFloat(alert.priceAtCreation) || livePrice[alert.pairSymbol] || 0;
  if (!(p > 0)) return false;
  alert._emaSide = p >= ema ? 'above' : 'below';
  alert.lastSide = alert._emaSide;
  // Child-path write: SSE ignores 3-level paths, so this never disturbs the alert cache.
  if (activeAlerts[alert.id]) {
    rtdbSet(`alerts/${alert.userId}/${alert.id}/lastSide`, alert._emaSide)
      .then(() => rtdbRemoveIfDeleted(alert.userId, alert.id))
      .catch(e => warn(`  EMA baseline persist failed ${alert.id}: ${e.message}`));
  }
  log(`  📍 ${indTypeName(alert)} alert ${alert.id} baseline: ${alert.pairSymbol} price ${p} is ${alert._emaSide} the ${indWhat(alert)} trigger level ${ema.toFixed(5)}`);
  return true;
}

function indShouldSkip(alert) {
  const mode = alert.alertMode === 'repeating' ? 'repeating' : 'once';
  if (mode === 'once' && recentlyTriggered.has(alert.id)) return true;
  if (alert.enabled === false) return true;                              // paused by the user
  if (serverStopped && alert.userEmail !== DEV_EMAIL) return true;
  return false;
}

/** Common fire path for Touch + Candle Confirmation. */
function indFire(alert, hitPrice, emaValue, hitSide, kindLabel) {
  const repeating = alert.alertMode === 'repeating';
  alert._emaSide = hitSide;                                              // price is now on this side of the EMA

  if (repeating) {
    const cooldownMs = Math.max(1, parseInt(alert.cooldownMinutes, 10) || 5) * 60000;
    const now = Date.now();
    if (now - (alert.lastTriggeredAt || 0) < cooldownMs) {
      log(`  🔁 ${indTypeName(alert)} alert ${alert.id} ${kindLabel} (${hitSide}) but in cooldown — crossing consumed`);
      alert.lastSide = hitSide;                                          // keep RTDB in step so a restart can't replay it
      if (activeAlerts[alert.id]) {
        rtdbSet(`alerts/${alert.userId}/${alert.id}/lastSide`, hitSide)
          .then(() => rtdbRemoveIfDeleted(alert.userId, alert.id)).catch(() => {});
      }
      return;
    }
    log(`🎯🔁 EMA repeating alert triggered: ${alert.pairSymbol} ${kindLabel} ${indLabel(alert)} ${alert.timeframe} → ${hitSide} (ema=${emaValue} price=${hitPrice}) user=${alert.userId}`);
    processRepeatingTrigger(alert, hitPrice, hitSide, emaValue)
      .catch(e => err(`processRepeatingTrigger (indicator) error: ${e.message}`));
    return;
  }

  if (recentlyTriggered.has(alert.id)) return;
  recentlyTriggered.add(alert.id);
  log(`🎯 ${indTypeName(alert)} alert triggered: ${alert.pairSymbol} ${kindLabel} ${indWhat(alert)} ${alert.timeframe} → ${hitSide} (value=${emaValue} price=${hitPrice}) user=${alert.userId}`);
  // The alert is removed right after this, so it's safe to stamp the EMA level + side on it:
  // history card, FCM and admin log then show "EMA value hit" and the side it crossed to.
  alert.targetPrice = emaValue;
  alert.direction   = hitSide;
  processTriggeredAlert(alert, hitPrice).catch(e => {
    err(`processTriggeredAlert (indicator) error: ${e.message}`);
    recentlyTriggered.delete(alert.id);
  });
}

/**
 * CROSSOVER baseline: which EMA is above the other. "above" = EMA 1 above EMA 2.
 * Persisted in RTDB (`lastSide`) so a restart remembers the position; if the position
 * changed while the server was down, the next evaluation sees the flip and fires.
 */
function indEnsureCrossBaseline(alert, diff) {
  if (alert._emaSide) return true;
  if (alert.lastSide === 'above' || alert.lastSide === 'below') { alert._emaSide = alert.lastSide; return true; }
  if (diff === 0) return false;                                          // exactly equal → no position yet
  alert._emaSide = diff > 0 ? 'above' : 'below';
  alert.lastSide = alert._emaSide;
  if (activeAlerts[alert.id]) {
    rtdbSet(`alerts/${alert.userId}/${alert.id}/lastSide`, alert._emaSide)
      .then(() => rtdbRemoveIfDeleted(alert.userId, alert.id))
      .catch(e => warn(`  EMA cross baseline persist failed ${alert.id}: ${e.message}`));
  }
  log(`  📍 ${indTypeName(alert)} cross alert ${alert.id} baseline: ${alert.pairSymbol} ${indWhat(alert)} → first line is ${alert._emaSide} the second line`);
  return true;
}

/** Returns the side to fire ('above'/'below') or null. Strict: equal EMAs are not a crossover. */
function indCrossHit(alert, diff) {
  if (alert._emaSide === 'above' && diff < 0) return 'below';
  if (alert._emaSide === 'below' && diff > 0) return 'above';
  return null;
}

/** Crossover TOUCH: both EMAs as they would be with the live price in the forming candle. Zero network. */
function indTouchCross(alert, g, price) {
  const [k1, k2] = indKeysOf(alert);
  const s1 = g.periods.get(k1), s2 = g.periods.get(k2);
  if (!s1 || !s2 || !s1.ready || !s2.ready) return;
  const l1 = indLiveValue(s1, price);
  const l2 = indLiveValue(s2, price);
  const diff = l1 - l2;
  if (!indEnsureCrossBaseline(alert, diff)) return;
  const hitSide = indCrossHit(alert, diff);
  if (!hitSide) return;
  indFire(alert, price, (l1 + l2) / 2, hitSide, 'Touch');
}

/** Crossover CANDLE CONFIRMATION: EMAs already include the freshly closed candle. */
function indCandleCross(alert, g, candle) {
  const [k1, k2] = indKeysOf(alert);
  const s1 = g.periods.get(k1), s2 = g.periods.get(k2);
  if (!s1 || !s2 || !s1.ready || !s2.ready) return;
  const diff = s1.val - s2.val;
  if (!indEnsureCrossBaseline(alert, diff)) return;
  const hitSide = indCrossHit(alert, diff);
  if (!hitSide) return;
  indFire(alert, candle.c, (s1.val + s2.val) / 2, hitSide, 'Candle Confirmation');
}

/** TOUCH — every second, zero network. */
function checkIndicatorTouchAlerts() {
  if (!indGroups.size) return;
  for (const alert of Object.values(activeAlerts)) {
    try {
      if (alert.candleClose || !indIsValid(alert)) continue;
      if (indShouldSkip(alert)) continue;
      const g  = indGroups.get(indKey(alert.pairSymbol, alert.timeframe));
      if (g && indIsCross(alert)) {
        if (!g.ready || !indMarketOpen(alert.pairSymbol)) continue;
        const px = livePrice[alert.pairSymbol];
        if (px > 0) indTouchCross(alert, g, px);
        continue;
      }
      const st = g && g.periods.get(indKeysOf(alert)[0]);
      if (!st || !st.ready || !g.ready) continue;
      if (!indMarketOpen(alert.pairSymbol)) continue;                    // stale weekend price — same rule as price alerts
      const price = livePrice[alert.pairSymbol];
      if (!(price > 0)) continue;
      const e = st.type === 'rsi' ? indRsiLevel(st, indTargetOf(alert)) : st.level;   // price == live indicator ⇔ price == level
      if (!indEnsureBaseline(alert, e)) continue;

      let hitSide = null;
      if (alert._emaSide === 'below') {
        if (price > e || (price === e && !alert._emaWait)) hitSide = 'above';
        else if (price < e) alert._emaWait = false;
      } else {
        if (price < e || (price === e && !alert._emaWait)) hitSide = 'below';
        else if (price > e) alert._emaWait = false;
      }
      if (!hitSide) continue;
      alert._emaWait = (price === e);   // exactly on the line right after firing → wait until price leaves it
      indFire(alert, price, st.type === 'rsi' ? indTargetOf(alert) : st.val, hitSide, 'Touch');
    } catch (e) { warn(`checkIndicatorTouchAlerts error: ${e.message}`); }
  }
}

/** CANDLE CONFIRMATION — right after the EMA has been updated with a freshly closed candle. */
function indEvaluateCandleConfirm(g, candle) {
  if (!candle) return;
  for (const alert of Object.values(activeAlerts)) {
    try {
      if (!alert.candleClose || !indIsValid(alert)) continue;
      if (alert.pairSymbol !== g.sym || alert.timeframe !== g.tf) continue;
      if (indShouldSkip(alert)) continue;
      if (indIsCross(alert)) { indCandleCross(alert, g, candle); continue; }
      const st = g.periods.get(indKeysOf(alert)[0]);
      if (!st || !st.ready) continue;
      const e = st.val;                                                  // indicator value including this closed candle
      if (!indEnsureBaseline(alert, e)) continue;

      let hitSide = null;
      if (alert._emaSide === 'below' && candle.c > e) hitSide = 'above';
      else if (alert._emaSide === 'above' && candle.c < e) hitSide = 'below';
      if (!hitSide) continue;
      indFire(alert, candle.c, e, hitSide, 'Candle Confirmation');
    } catch (e) { warn(`indEvaluateCandleConfirm error: ${e.message}`); }
  }
}

/** Snapshot for the secret-gated /admin/indicators endpoint (verify EMA values against TradingView). */
function indDebugSnapshot() {
  const groups = [];
  for (const g of indGroups.values()) {
    groups.push({
      key: g.key, source: g.source, ready: g.ready, loading: g.loading, fails: g.fails,
      lastOpen: g.lastOpen ? new Date(g.lastOpen).toISOString() : null,
      lastClose: g.lastCandle ? g.lastCandle.c : null,
      livePrice: livePrice[g.sym] || 0,
      periods: [...g.periods.values()].map(s => ({ type: s.type, period: s.period, ready: s.ready, value: s.ready ? s.val : null, candlesUsed: s.warm || 0 })),
      alerts: Object.values(activeAlerts).filter(a => indIsValid(a) && a.pairSymbol === g.sym && a.timeframe === g.tf).length,
    });
  }
  return { groups, unsupportedAlerts: Object.values(activeAlerts).filter(a => a.alertKind === 'indicator' && !indIsValid(a)).length };
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 13 — VOICE ALARM  (VOICE-FEATURE)
// Spoken alarm: when an Alarm-type price alert has voiceAlarm=true, the server writes a sentence
// ("Gold reaches your price target four thousand five hundred dollars"), has ElevenLabs speak it ONCE,
// stores the mp3 in Cloudflare R2 (one file per alert, per user) and the app downloads + caches it.
// The app plays the cached file when the alarm fires and falls back to the normal ringtone otherwise.
//
// SELF-CONTAINED / EASY TO REMOVE:
//   1. delete this whole section            2. delete the line  "voiceHandleRequest"  in startHealthServer
//   3. delete the line  "startVoiceModule()"  in main()
// Nothing else in the server reads or writes voice data. Kill switch without code changes: remove the
// VOICE_ENABLED env var (or set it to 0) — the module then does nothing at all.
//
// ENV: VOICE_ENABLED=1, ELEVENLABS_API_KEY, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
// Optional: ELEVEN_VOICE_ID, ELEVEN_MODEL_ID, VOICE_MAX_PER_USER (15), VOICE_MAX_PER_HOUR (60), VOICE_CONCURRENCY (2)
// ════════════════════════════════════════════════════════════════════════════

const VOICE_CFG = {
  voiceId:      process.env.ELEVEN_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL',   // Sarah
  modelId:      process.env.ELEVEN_MODEL_ID || 'eleven_multilingual_v2',
  stability:    0.75,
  similarity:   0.75,
  style:        0,
  speed:        0.90,
  speakerBoost: true,
  format:       'mp3_44100_64',
  maxPerUser:   parseInt(process.env.VOICE_MAX_PER_USER, 10) || 15,   // credit protection
  maxPerHour:   parseInt(process.env.VOICE_MAX_PER_HOUR, 10) || 60,   // credit protection against loops
  concurrency:  parseInt(process.env.VOICE_CONCURRENCY, 10)  || 2,    // ElevenLabs free plan = 2 parallel requests
};
const VOICE_SAFE_ID      = /^[A-Za-z0-9_-]{1,128}$/;
const VOICE_MIN_BYTES    = 6000;      // ~0.7 s at 64 kbps — anything smaller is a broken generation
const VOICE_MAX_BYTES    = 400000;
const VOICE_DELETE_AFTER = 10 * 60 * 1000;   // alert gone / voice switched off this long → delete the file
const VOICE_RETRY_MS     = [30000, 120000, 600000, 3600000, 3600000];

const voiceMeta     = new Map();   // `${uid}/${alertId}` → { textHash, hash, size, updatedAt }
const voiceMissing  = new Map();   // meta key → first time it stopped being wanted
const voiceQueue    = [];          // pending jobs
const voiceInflight = new Set();
const voiceFails    = new Map();   // meta key → { n, nextAt }
const voiceBudget   = [];          // timestamps of generations in the last hour
const voiceRate     = new Map();   // uid → [timestamps] (endpoint rate limit)
let voiceMetaLoaded = false;
let voiceSeenAlerts = false;
let voiceBlockedUntil = 0;         // credits / plan problem → stop generating until then
let voiceStats = { generated: 0, failed: 0, reused: 0, deleted: 0 };
let voiceNet = (opts, body, timeoutMs) => voiceHttps(opts, body, timeoutMs);   // swappable in tests

function voiceEnabled() {
  const e = process.env;
  return e.VOICE_ENABLED === '1' && !!e.ELEVENLABS_API_KEY && !!e.R2_ACCOUNT_ID &&
         !!e.R2_ACCESS_KEY_ID && !!e.R2_SECRET_ACCESS_KEY && !!e.R2_BUCKET;
}

function voiceHttps(opts, body, timeoutMs = 25000) {
  return new Promise((resolve, reject) => {
    const req = https.request(opts, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error('timeout')));
    if (body) req.write(body);
    req.end();
  });
}

// ── Sentence builder ─────────────────────────────────────────────────────────
const VOICE_ONES = ['zero','one','two','three','four','five','six','seven','eight','nine','ten','eleven','twelve',
  'thirteen','fourteen','fifteen','sixteen','seventeen','eighteen','nineteen'];
const VOICE_TENS = ['','','twenty','thirty','forty','fifty','sixty','seventy','eighty','ninety'];
function voiceIntWords(n) {
  if (n < 20)   return VOICE_ONES[n];
  if (n < 100)  return VOICE_TENS[Math.floor(n / 10)] + (n % 10 ? ' ' + VOICE_ONES[n % 10] : '');
  if (n < 1000) return VOICE_ONES[Math.floor(n / 100)] + ' hundred' + (n % 100 ? ' ' + voiceIntWords(n % 100) : '');
  if (n < 1e6)  return voiceIntWords(Math.floor(n / 1000)) + ' thousand' + (n % 1000 ? ' ' + voiceIntWords(n % 1000) : '');
  if (n < 1e9)  return voiceIntWords(Math.floor(n / 1e6)) + ' million' + (n % 1e6 ? ' ' + voiceIntWords(n % 1e6) : '');
  return String(n).split('').map(c => VOICE_ONES[+c]).join(' ');
}
/** 4500.5 → "four thousand five hundred point five";  1.085 → "one point zero eight five" */
function voiceNumberWords(price) {
  let s = Number(price).toFixed(8).replace(/0+$/, '').replace(/\.$/, '');
  const [i, d] = s.split('.');
  let out = voiceIntWords(parseInt(i, 10));
  if (d) out += ' point ' + d.split('').map(c => VOICE_ONES[+c]).join(' ');
  return out;
}
const VOICE_CCY = { USD:'US Dollar', EUR:'Euro', GBP:'British Pound', JPY:'Japanese Yen', AUD:'Australian Dollar',
  CAD:'Canadian Dollar', CHF:'Swiss Franc', NZD:'New Zealand Dollar' };
function voiceSpokenPair(a) {
  let n = String(a.pairName || a.pairSymbol || 'Price').trim();
  const m = n.match(/^([A-Za-z]{3})\s*\/\s*([A-Za-z]{3})$/);
  if (m) return `${VOICE_CCY[m[1].toUpperCase()] || m[1]} ${VOICE_CCY[m[2].toUpperCase()] || m[2]}`;
  n = n.replace(/&/g, ' and ').replace(/([A-Za-z])(\d)/g, '$1 $2').replace(/\s+/g, ' ').trim();
  return n;
}
const VOICE_DOLLAR_SYMBOLS = new Set(['XAU', 'XAG']);
const VOICE_NO_UNIT_SYMBOLS = new Set(['SPX500', 'US30', 'US100', 'DXY', 'NIF50']);
function voiceUnit(a) {
  const sym = String(a.pairSymbol || '').toUpperCase();
  if (VOICE_DOLLAR_SYMBOLS.has(sym)) return ' dollars';
  if (VOICE_NO_UNIT_SYMBOLS.has(sym) || FOREX_PAIRS.has(sym)) return '';
  return ' dollars';                                   // crypto
}
function voiceBuildText(a) {
  const pair = voiceSpokenPair(a);
  const num  = voiceNumberWords(a.targetPrice) + voiceUnit(a);
  if (a.candleClose) {
    const dir = a.direction === 'below' ? 'below' : 'above';
    return `${pair} candle closed ${dir} your price target ${num}.`;
  }
  return `${pair} reaches your price target ${num}.`;
}
function voiceHash(str) { return require('crypto').createHash('sha256').update(str).digest('hex').slice(0, 16); }
function voiceSeed(text) { return parseInt(require('crypto').createHash('sha256').update(text).digest('hex').slice(0, 8), 16); }

function voiceEligible(id, a) {
  return !!a && VOICE_SAFE_ID.test(id) && VOICE_SAFE_ID.test(String(a.userId || '')) &&
         (a.voiceAlarm === true || a.voiceAlarm === 'true') && a.alarm === true &&
         a.alertKind !== 'indicator' && Number(a.targetPrice) > 0;
}

// ── Cloudflare R2 (S3 API, AWS Signature V4 — no SDK needed) ────────────────
function voiceSha256Hex(data) { return require('crypto').createHash('sha256').update(data).digest('hex'); }
function voiceHmac(key, data) { return require('crypto').createHmac('sha256', key).update(data).digest(); }
function awsV4Sign({ method, uri, query = '', headers, payloadHash, amzDate, region, service, secret, accessKey }) {
  const day = amzDate.slice(0, 8);
  const lower = {};
  for (const k of Object.keys(headers)) lower[k.toLowerCase()] = String(headers[k]).trim();
  const names = Object.keys(lower).sort();
  const canonHeaders = names.map(n => `${n}:${lower[n]}\n`).join('');
  const signed = names.join(';');
  const canonical = [method, uri, query, canonHeaders, signed, payloadHash].join('\n');
  const scope = `${day}/${region}/${service}/aws4_request`;
  const toSign = ['AWS4-HMAC-SHA256', amzDate, scope, voiceSha256Hex(canonical)].join('\n');
  const kDate = voiceHmac('AWS4' + secret, day);
  const kReg  = voiceHmac(kDate, region);
  const kSvc  = voiceHmac(kReg, service);
  const kSig  = voiceHmac(kSvc, 'aws4_request');
  const signature = require('crypto').createHmac('sha256', kSig).update(toSign).digest('hex');
  return { signature, authorization: `AWS4-HMAC-SHA256 Credential=${accessKey}/${scope}, SignedHeaders=${signed}, Signature=${signature}` };
}
function r2Key(uid, alertId) { return `voices/${uid}/${alertId}.mp3`; }
async function r2Request(method, key, body, contentType) {
  const e = process.env;
  const host = `${e.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
  const uri  = '/' + e.R2_BUCKET + '/' + key.split('/').map(encodeURIComponent).join('/');
  const payload = body || Buffer.alloc(0);
  const payloadHash = voiceSha256Hex(payload);
  const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
  const headers = { host, 'x-amz-content-sha256': payloadHash, 'x-amz-date': amzDate };
  if (contentType) headers['content-type'] = contentType;
  const { authorization } = awsV4Sign({ method, uri, headers, payloadHash, amzDate, region: 'auto', service: 's3',
    secret: e.R2_SECRET_ACCESS_KEY, accessKey: e.R2_ACCESS_KEY_ID });
  const reqHeaders = { ...headers, Authorization: authorization, 'Content-Length': payload.length };
  delete reqHeaders.host;
  return voiceNet({ hostname: host, port: 443, path: uri, method, headers: reqHeaders }, payload.length ? payload : null, 25000);
}

// ── ElevenLabs ───────────────────────────────────────────────────────────────
class VoiceApiError extends Error { constructor(msg, kind) { super(msg); this.kind = kind; } }   // kind: quota | busy | other
async function elevenSpeak(text) {
  const payload = JSON.stringify({
    text,
    model_id: VOICE_CFG.modelId,
    voice_settings: { stability: VOICE_CFG.stability, similarity_boost: VOICE_CFG.similarity, style: VOICE_CFG.style,
                      speed: VOICE_CFG.speed, use_speaker_boost: VOICE_CFG.speakerBoost },
    seed: voiceSeed(text),
  });
  const res = await voiceNet({
    hostname: 'api.elevenlabs.io', port: 443, method: 'POST',
    path: `/v1/text-to-speech/${encodeURIComponent(VOICE_CFG.voiceId)}?output_format=${VOICE_CFG.format}`,
    headers: { 'xi-api-key': process.env.ELEVENLABS_API_KEY, 'Content-Type': 'application/json',
               'Accept': 'audio/mpeg', 'Content-Length': Buffer.byteLength(payload) },
  }, payload, 30000);
  if (res.status !== 200) {
    const msg = res.body.toString('utf8').slice(0, 300);
    if (res.status === 429 && /concurrent|rate/i.test(msg)) throw new VoiceApiError(`busy: ${msg}`, 'busy');
    if (res.status === 401 || res.status === 402 || /quota|payment|credit|exceeded/i.test(msg))
      throw new VoiceApiError(`${res.status} ${msg}`, 'quota');
    throw new VoiceApiError(`${res.status} ${msg}`, 'other');
  }
  if (res.body.length < VOICE_MIN_BYTES || res.body.length > VOICE_MAX_BYTES)
    throw new VoiceApiError(`unexpected audio size ${res.body.length} bytes`, 'other');
  return res.body;
}

// ── Generation job (generate → upload → verify upload → record) ─────────────
async function voiceRunJob(job) {
  const mk = `${job.uid}/${job.alertId}`;
  const t0 = Date.now();
  const audio = await elevenSpeak(job.text);
  voiceBudget.push(Date.now());
  const put = await r2Request('PUT', r2Key(job.uid, job.alertId), audio, 'audio/mpeg');
  if (put.status < 200 || put.status >= 300) throw new VoiceApiError(`R2 upload failed ${put.status} ${put.body.toString('utf8').slice(0, 150)}`, 'other');
  // verification step: the object must exist in R2 with exactly the bytes we sent
  const head = await r2Request('HEAD', r2Key(job.uid, job.alertId));
  const size = parseInt(head.headers['content-length'], 10);
  if (head.status !== 200 || size !== audio.length) throw new VoiceApiError(`R2 verify failed (status ${head.status}, size ${size} vs ${audio.length})`, 'other');
  const meta = { textHash: job.textHash, hash: voiceSha256Hex(audio).slice(0, 16), size: audio.length, updatedAt: Date.now() };
  const wr = await rtdbSet(`voices/${job.uid}/${job.alertId}`, meta);
  if (wr && typeof wr === 'object' && wr.error) throw new VoiceApiError(`RTDB metadata write failed: ${wr.error}`, 'other');
  voiceMeta.set(mk, meta);
  voiceFails.delete(mk);
  voiceStats.generated++;
  log(`🗣 Voice ready: ${job.alertId} · "${job.text}" · ${(audio.length / 1024).toFixed(1)} KB · ${Date.now() - t0} ms · user=${job.uid}`);
}

function voiceBudgetLeft() {
  const cut = Date.now() - 3600000;
  while (voiceBudget.length && voiceBudget[0] < cut) voiceBudget.shift();
  return VOICE_CFG.maxPerHour - voiceBudget.length;
}

async function voicePump() {
  while (voiceInflight.size < VOICE_CFG.concurrency && voiceQueue.length) {
    if (Date.now() < voiceBlockedUntil) return;
    if (voiceBudgetLeft() <= 0) { warn('Voice: hourly generation limit reached — waiting'); return; }
    const job = voiceQueue.shift();
    const mk = `${job.uid}/${job.alertId}`;
    voiceInflight.add(mk);
    (async () => {
      try {
        await voiceRunJob(job);
      } catch (e) {
        voiceStats.failed++;
        const f = voiceFails.get(mk) || { n: 0, nextAt: 0 };
        if (e.kind === 'busy') {
          f.nextAt = Date.now() + 20000;                       // not the alert's fault — doesn't count as an attempt
        } else {
          f.n++;
          f.nextAt = Date.now() + VOICE_RETRY_MS[Math.min(f.n - 1, VOICE_RETRY_MS.length - 1)];
          if (e.kind === 'quota') {
            voiceBlockedUntil = Date.now() + 60 * 60 * 1000;
            sendAlertEmail('voice_quota', 'Voice alarm: ElevenLabs credits / plan problem',
              `ElevenLabs rejected a voice request:\n${e.message}\n\nNew voices are paused for 1 hour; alerts keep ringing with the normal ringtone.`);
          }
        }
        voiceFails.set(mk, f);
        warn(`Voice generation failed for ${job.alertId} (attempt ${f.n}): ${e.message}`);
      } finally {
        voiceInflight.delete(mk);
        setImmediate(voicePump);
      }
    })();
  }
}

// ── Reconcile: which alerts want a voice? (in-memory scan, no network unless something changed) ──
function voiceWanted() {
  const perUser = {};
  for (const [id, a] of Object.entries(activeAlerts)) {
    if (!voiceEligible(id, a)) continue;
    (perUser[a.userId] = perUser[a.userId] || []).push({ id, a });
  }
  const wanted = new Map();
  for (const uid of Object.keys(perUser)) {
    perUser[uid].sort((x, y) => (x.a.createdAt || 0) - (y.a.createdAt || 0));
    perUser[uid].slice(0, VOICE_CFG.maxPerUser).forEach(({ id, a }) => {
      const text = voiceBuildText(a);
      wanted.set(`${uid}/${id}`, { uid, alertId: id, text, textHash: voiceHash(text) });
    });
  }
  return wanted;
}
function voiceReconcile() {
  if (!voiceEnabled() || !voiceMetaLoaded) return;
  if (Object.keys(activeAlerts).length) voiceSeenAlerts = true;
  const wanted = voiceWanted();
  const queued = new Set(voiceQueue.map(j => `${j.uid}/${j.alertId}`));
  for (const [mk, job] of wanted) {
    voiceMissing.delete(mk);
    const have = voiceMeta.get(mk);
    if (have && have.textHash === job.textHash) continue;
    if (voiceInflight.has(mk) || queued.has(mk)) continue;
    const f = voiceFails.get(mk);
    if (f && (f.nextAt > Date.now() || f.n >= VOICE_RETRY_MS.length)) continue;
    voiceQueue.push(job);
    log(`🗣 Voice queued: ${job.alertId} · "${job.text}"`);
  }
  // Drop queued jobs whose alert is gone or changed
  for (let i = voiceQueue.length - 1; i >= 0; i--) {
    const j = voiceQueue[i], w = wanted.get(`${j.uid}/${j.alertId}`);
    if (!w || w.textHash !== j.textHash) voiceQueue.splice(i, 1);
  }
  voicePump();
}

// ── Sweep: delete files whose alert is gone (triggered once / deleted / voice switched off) ──
async function voiceSweep() {
  if (!voiceEnabled() || !voiceMetaLoaded || !voiceSeenAlerts || process.uptime() < 120) return;
  const wanted = voiceWanted();
  const now = Date.now();
  const due = [];
  for (const mk of voiceMeta.keys()) {
    if (wanted.has(mk)) { voiceMissing.delete(mk); continue; }
    if (!voiceMissing.has(mk)) { voiceMissing.set(mk, now); continue; }
    if (now - voiceMissing.get(mk) >= VOICE_DELETE_AFTER) due.push(mk);
  }
  // Safety net: never mass-delete because of a transient glitch (empty alert list, RTDB hiccup...)
  if (voiceMeta.size >= 5 && due.length > voiceMeta.size * 0.5) {
    warn(`Voice sweep skipped: would delete ${due.length}/${voiceMeta.size} voices at once — looks like a glitch`);
    return;
  }
  for (const mk of due) {
    const [uid, alertId] = mk.split('/');
    try {
      const r = await r2Request('DELETE', r2Key(uid, alertId));
      if (r.status !== 204 && r.status !== 200 && r.status !== 404) throw new Error(`R2 delete ${r.status}`);
      await rtdbDelete(`voices/${uid}/${alertId}`);
      voiceMeta.delete(mk); voiceMissing.delete(mk); voiceFails.delete(mk);
      voiceStats.deleted++;
      log(`🧹 Voice deleted: ${alertId} (alert finished or voice switched off) user=${uid}`);
    } catch (e) { warn(`Voice delete failed for ${alertId}: ${e.message}`); }
  }
}

// ── Firebase ID-token check (so only the owner can download a voice) ────────
let voiceCerts = { at: 0, certs: {} };
async function voiceGetCerts() {
  if (Date.now() - voiceCerts.at < 3600000 && Object.keys(voiceCerts.certs).length) return voiceCerts.certs;
  const res = await voiceNet({ hostname: 'www.googleapis.com', port: 443, method: 'GET',
    path: '/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com', headers: {} }, null, 10000);
  if (res.status !== 200) throw new Error(`cert fetch ${res.status}`);
  voiceCerts = { at: Date.now(), certs: JSON.parse(res.body.toString('utf8')) };
  return voiceCerts.certs;
}
async function voiceVerifyToken(token) {
  try {
    const parts = String(token || '').split('.');
    if (parts.length !== 3) return null;
    const header = JSON.parse(Buffer.from(parts[0], 'base64url').toString('utf8'));
    const claims = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
    if (header.alg !== 'RS256') return null;
    const certs = await voiceGetCerts();
    const pem = certs[header.kid];
    if (!pem) return null;
    const ok = require('crypto').createVerify('RSA-SHA256').update(parts[0] + '.' + parts[1])
      .verify(pem, Buffer.from(parts[2], 'base64url'));
    if (!ok) return null;
    const now = Math.floor(Date.now() / 1000);
    if (claims.aud !== FIREBASE_PROJECT_ID || claims.iss !== `https://securetoken.google.com/${FIREBASE_PROJECT_ID}`) return null;
    if (!(claims.exp > now) || claims.iat > now + 300 || !claims.sub || !VOICE_SAFE_ID.test(claims.sub)) return null;
    return claims.sub;
  } catch (e) { return null; }
}
function voiceRateOk(uid) {
  const now = Date.now(), arr = (voiceRate.get(uid) || []).filter(t => now - t < 60000);
  if (arr.length >= 60) { voiceRate.set(uid, arr); return false; }
  arr.push(now); voiceRate.set(uid, arr); return true;
}

// ── HTTP endpoints (called from startHealthServer) ──────────────────────────
//   GET /voice/list            → { voices:[{alertId,hash,size}], pending:[alertId] }
//   GET /voice/file/{alertId}  → 200 audio/mpeg | 202 still being made | 404 none
// Returns true when the request was a /voice request (handled here).
function voiceHandleRequest(req, res) {
  const url = req.url || '';
  if (url === '/admin/voice') {
    const secret = req.headers['x-admin-secret'];
    if (!process.env.ADMIN_PANEL_SECRET || secret !== process.env.ADMIN_PANEL_SECRET) {
      res.writeHead(401, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'unauthorized' })); return true;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ enabled: voiceEnabled(), stored: voiceMeta.size, queued: voiceQueue.length, inflight: voiceInflight.size,
      blockedForMs: Math.max(0, voiceBlockedUntil - Date.now()), generationsLastHour: voiceBudget.length, ...voiceStats }));
    return true;
  }
  if (!url.startsWith('/voice/')) return false;
  const json = (code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
  (async () => {
    if (!voiceEnabled()) return json(503, { status: 'off' });
    const m = (req.headers['authorization'] || '').match(/^Bearer\s+(.+)$/i);
    const uid = m ? await voiceVerifyToken(m[1]) : null;
    if (!uid) return json(401, { error: 'unauthorized' });
    if (!voiceRateOk(uid)) return json(429, { error: 'slow down' });
    if (url === '/voice/list') {
      const voices = [], pending = [];
      for (const [mk, v] of voiceMeta) if (mk.startsWith(uid + '/')) voices.push({ alertId: mk.slice(uid.length + 1), hash: v.hash, size: v.size });
      for (const mk of voiceWanted().keys()) if (mk.startsWith(uid + '/') && !voiceMeta.has(mk)) pending.push(mk.slice(uid.length + 1));
      return json(200, { voices, pending });
    }
    const fm = url.match(/^\/voice\/file\/([A-Za-z0-9_-]{1,128})$/);
    if (!fm) return json(404, { error: 'not found' });
    const alertId = fm[1], mk = `${uid}/${alertId}`;
    const meta = voiceMeta.get(mk);
    if (!meta) return json(voiceWanted().has(mk) ? 202 : 404, { status: voiceWanted().has(mk) ? 'pending' : 'none' });
    const r = await r2Request('GET', r2Key(uid, alertId));
    if (r.status === 404) { voiceMeta.delete(mk); return json(404, { status: 'missing' }); }   // self-heal: reconcile regenerates it
    if (r.status !== 200) return json(502, { error: 'storage' });
    res.writeHead(200, { 'Content-Type': 'audio/mpeg', 'Content-Length': r.body.length, 'X-Voice-Hash': meta.hash });
    res.end(r.body);
  })().catch(e => { warn(`Voice endpoint error: ${e.message}`); try { json(500, { error: 'server' }); } catch (_) {} });
  return true;
}

async function voiceLoadMeta() {
  try {
    const all = await rtdbGet('voices');
    if (all && typeof all === 'object' && all.error) throw new Error(String(all.error));   // never treat an error reply as "no voices" (would regenerate everything)
    if (typeof all === 'string') throw new Error('unexpected RTDB reply');
    voiceMeta.clear();
    if (all && typeof all === 'object') {
      for (const [uid, byAlert] of Object.entries(all)) {
        if (!byAlert || typeof byAlert !== 'object') continue;
        for (const [alertId, m] of Object.entries(byAlert)) if (m && m.hash) voiceMeta.set(`${uid}/${alertId}`, m);
      }
    }
    voiceMetaLoaded = true;
    log(`🗣 Voice module ready — ${voiceMeta.size} stored voice(s)`);
  } catch (e) {
    warn(`Voice metadata load failed (${e.message}) — retrying in 30s; no voices are generated until it loads`);
    setTimeout(voiceLoadMeta, 30000);
  }
}

function startVoiceModule() {
  if (!voiceEnabled()) { log('🗣 Voice alarm: OFF (VOICE_ENABLED / keys not set)'); return; }
  log(`🗣 Voice alarm: ON — model ${VOICE_CFG.modelId}, voice ${VOICE_CFG.voiceId}, max ${VOICE_CFG.maxPerUser}/user, ${VOICE_CFG.maxPerHour}/hour`);
  voiceLoadMeta();
  setInterval(() => { try { voiceReconcile(); } catch (e) { warn(`voiceReconcile error: ${e.message}`); } }, 15000);
  setInterval(() => { voiceSweep().catch(e => warn(`voiceSweep error: ${e.message}`)); }, 5 * 60 * 1000);
}

// ════════════════════════════════════════════════════════════════════════════
// MAIN
// ════════════════════════════════════════════════════════════════════════════

async function main() {
  log('=== Trade Alert Server Starting ===');

  const required = ['CAP_EMAIL','CAP_PASSWORD','CAP_API_KEY','FIREBASE_URL',
                    'FIREBASE_SECRET','FIREBASE_PROJECT_ID','FIREBASE_SERVICE_ACCOUNT'];
  for (const k of required) {
    if (!process.env[k]) throw new Error(`Missing env var: ${k}`);
  }

  startHealthServer();
  startRtdbListener();

  // Safety-net REST poll for alerts — self-heals activeAlerts if the SSE
  // stream above silently fails to deliver data (as happened once in production).
  // This is NOT the primary sync path — SSE already does exactly what we want
  // (full load once on connect, then only per-alert deltas after that, so
  // bandwidth stays low as alert count grows). SSE is now confirmed working,
  // so this only needs to be an occasional background reconciliation check —
  // worst case an SSE outage takes up to this long to self-heal.
  pollAlertsFallback();
  setInterval(pollAlertsFallback, 60 * 60 * 1000); // every 1 hour

  // Capital.com WebSocket for metals
  await createCapSession();
  connectCapWs();
  setInterval(pingCapSession, 9 * 60 * 1000);
  startCapWsWatchdog(); // detects silent stalls — force reconnects if no tick for 2min

  // Gate.io WebSocket for instant crypto prices
  connectGateWs();
  // REST fallback — only runs if WebSocket is disconnected
  setInterval(pollGateio, 15000);

  // Yahoo Finance polling every 20 seconds for indices + forex (active pairs + market hours only)
  setInterval(pollYahoo, 20000);
  pollYahoo();

  // Update RTDB prices every 5 seconds for mobile display
  setInterval(updateRtdbPrices, 5000);

  // Check instant alerts every second
  setInterval(checkAlerts, 1000);

  // EMA indicator engine: create/drop states + load history when EMA alerts appear (in-memory scan, no network)
  setInterval(() => { try { syncIndicatorStates(); } catch (e) { warn(`syncIndicatorStates error: ${e.message}`); } }, 5000);

  // Minute-boundary candle close checker
  startMinuteBoundaryChecker();

  startVoiceModule();   // VOICE-FEATURE (does nothing unless VOICE_ENABLED=1)

  // Check server stop flag every minute
  setInterval(checkServerStopFlag, 60000);
  checkServerStopFlag();

  log('=== Trade Alert Server Running ===');
}

main().catch(e => {
  err(`Fatal: ${e.message}`);
  process.exit(1);
});
