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
const FIREBASE_URL = process.env.FIREBASE_URL;       // RTDB base URL
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
  // Indices + Forex (from Yahoo polling)
  SPX500:0, US30:0, US100:0, DXY:0, NIF50:0,
  EURUSD:0, GBPUSD:0, USDJPY:0, GBPJPY:0, AUDUSD:0, USDGBP:0, // USDGBP derived as 1/GBPUSD
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
    if (alert.candleClose && alert.timeframe === tf && GATE_SYMBOLS_SET.has(alert.pairSymbol)) {
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

const FOREX_PAIRS = new Set(['EURUSD','GBPUSD','USDJPY','GBPJPY','AUDUSD','USDGBP']);

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
  // Gold/silver follow the forex weekly session but also have a daily ~1hr maintenance
  // break around 21:00-22:00 UTC (2:00-3:00 AM PKT) every weekday.
  // We use 20:55-22:05 UTC as the closed window to give a buffer on both sides.
  if (!isForexOpen()) return false;
  const now  = new Date();
  const mins = now.getUTCHours() * 60 + now.getUTCMinutes();
  if (mins >= 20*60+55 && mins < 22*60+5) return false; // daily break
  return true;
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
      // Remove all alerts for this user first
      for (const id of Object.keys(activeAlerts)) {
        if (activeAlerts[id].userId === userId) delete activeAlerts[id];
      }
      // Add new ones
      if (value && typeof value === 'object') {
        for (const [alertId, alert] of Object.entries(value)) {
          if (alert && typeof alert === 'object') {
            activeAlerts[alertId] = { ...alert, userId };
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
        // Alert added or updated
        activeAlerts[alertId] = { ...value, userId };
        log(`RTDB alert added: ${alertId} (${value.pairSymbol} ${value.direction} ${value.targetPrice})`);
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

    // Don't alert if we haven't received ANY ticks yet since server start or market open
    // (lastTickAt=0 means market just opened or server just started — give it time)
    if (lastTickAt.XAU === 0 && lastTickAt.XAG === 0) return;

    if (lastTickAt.XAU > 0 && xauAge > staleMs) {
      warn(`Capital.com WS stale — XAU last tick ${Math.round(xauAge/1000)}s ago — force reconnecting`);
      // Only email if stale for more than 5 minutes — avoids false alarms on brief
      // daily maintenance windows (gold closes ~1hr around 21:00 UTC daily)
      if (xauAge > 5 * 60 * 1000) {
        sendAlertEmail('cap_ws_stale', 'Capital.com WS price feed stalled', `Gold (XAU) price feed stopped updating.\nLast tick: ${Math.round(xauAge/1000)}s ago.\nAuto-reconnecting now — prices may have been stale for up to ${Math.round(xauAge/60000)} minutes.`);
      }
      lastTickAt.XAU = Date.now(); // reset so we don't re-trigger next minute during reconnect
      lastTickAt.XAG = Date.now();
      reconnectCapWs();
    } else if (lastTickAt.XAG > 0 && xagAge > staleMs) {
      warn(`Capital.com WS stale — XAG last tick ${Math.round(xagAge/1000)}s ago — force reconnecting`);
      if (xagAge > 5 * 60 * 1000) {
        sendAlertEmail('cap_ws_stale', 'Capital.com WS price feed stalled', `Silver (XAG) price feed stopped updating.\nLast tick: ${Math.round(xagAge/1000)}s ago.\nAuto-reconnecting now.`);
      }
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
  ARB:'ARB_USDT', OP:'OP_USDT', SHIB:'SHIB_USDT', TRX:'TRX_USDT'
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
  ARB:'ARBUSDT', OP:'OPUSDT', SHIB:'SHIBUSDT', TRX:'TRXUSDT'
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
  GBPJPY: 'GBPJPY=X', AUDUSD: 'AUDUSD=X'
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

let _checkCount = 0;
function checkAlerts() {
  _checkCount++;
  const alertList = Object.values(activeAlerts);

  if (_checkCount % 30 === 0) {
    log(`Checker alive — ${alertList.length} alerts, BTC=${livePrice.BTC} XAU=${livePrice.XAU} EURUSD=${livePrice.EURUSD}`);
    // Show all active instant alerts for debugging
    alertList.filter(a => !a.candleClose).forEach(a => {
      const p   = livePrice[a.pairSymbol];
      const mkt = (p && p > 0) ? '' : (isYahooSymbolOpen(a.pairSymbol) === false ? ' [market closed]' : ' [no price yet]');
      log(`  active: ${a.pairSymbol} ${a.direction} ${a.targetPrice} price=${p || 0}${mkt}`);
    });
  }
  if (!alertList.length) return;

  const nowMs = Date.now();
  for (const alert of alertList) {
    try {
      if (recentlyTriggered.has(alert.id)) continue;
      if (serverStopped && alert.userEmail !== DEV_EMAIL) continue;
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
          a => a.candleClose && a.timeframe === tf && a.pairSymbol === sym
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
  const alertList = all.filter(a => a.candleClose && closedTfs.includes(a.timeframe));

  log(`  checkCandleCloseAlerts: ${all.length} total, ${alertList.length} match [${closedTfs.join(',')}]`);
  all.filter(a => a.candleClose).forEach(a =>
    log(`    alert: ${a.pairSymbol} ${a.direction} ${a.targetPrice} tf=${a.timeframe} candleClose=${a.candleClose}`)
  );

  const nowMs = Date.now();
  for (const alert of alertList) {
    try {
      if (recentlyTriggered.has(alert.id)) {
        log(`    SKIP ${alert.id}: recentlyTriggered`); continue;
      }
      if (serverStopped && alert.userEmail !== DEV_EMAIL) continue;

      // Skip when this symbol's market is currently closed — avoids firing on
      // a stale weekend price snapshot/candle.
      const sym = alert.pairSymbol;
      if ((sym === 'XAU' || sym === 'XAG') && !isMetalsOpen()) continue;
      if (FOREX_PAIRS.has(sym) && !isForexOpen()) continue;

      const close = getCandleClose(alert.pairSymbol, alert.timeframe);
      log(`    checking ${alert.pairSymbol} ${alert.timeframe} close=${close} target=${alert.targetPrice} dir=${alert.direction}`);
      if (!close || close <= 0) continue;

      const target = parseFloat(alert.targetPrice);
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
    }
  }, 500);
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 10 — TRIGGER PROCESSING
// ════════════════════════════════════════════════════════════════════════════

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
  // Uses commit API with an update + updateMask where the field is absent from the
  // body but present in the mask — this is the only reliable way to delete a single
  // map field via REST. We also verify the response explicitly.
  try {
    const token   = await getAccessToken();
    const docPath = `projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/alerts/${userId}/active_alerts/alerts`;
    const res2 = await fetchJson(
      `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`,
      {
        method:  'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body:    JSON.stringify({
          writes: [{
            update:     { name: docPath, fields: {} },
            updateMask: { fieldPaths: [`\`${alertId}\``] }
          }]
        })
      }
    );
    if (res2?.error) {
      warn(`  Firestore active_alerts delete error: ${JSON.stringify(res2.error)}`);
    } else if (!res2?.writeResults) {
      warn(`  Firestore active_alerts delete unexpected response: ${JSON.stringify(res2)}`);
    } else {
      log(`  Firestore active_alerts field deleted: ${alertId}`);
      // Verify the field is actually gone
      const verifyRes = await fetchJson(
        `https://firestore.googleapis.com/v1/${docPath}`,
        { headers: { 'Authorization': `Bearer ${token}` } }
      );
      if (verifyRes?.fields?.[alertId]) {
        warn(`  Firestore delete verification FAILED — field still exists: ${alertId} — retrying`);
        // Retry once more
        await fetchJson(
          `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`,
          {
            method:  'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body:    JSON.stringify({
              writes: [{
                update:     { name: docPath, fields: {} },
                updateMask: { fieldPaths: [`\`${alertId}\``] }
              }]
            })
          }
        );
        log(`  Firestore active_alerts delete retried: ${alertId}`);
      } else {
        log(`  Firestore active_alerts delete verified OK: ${alertId}`);
      }
    }
  } catch(e) { warn(`  Firestore active_alerts delete failed: ${e.message}`); }

  // 3. Write to Firestore history field
  try {
    const token    = await getAccessToken();
    const hitAlert = { ...alert, triggered: true, hitAt: hitTime };
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

  // 4. (history_index removed — app reads history doc directly on login)

  // 5. Send FCM
  try {
    await sendFCM(userId, alert, hitPrice);
  } catch(e) { warn(`  FCM failed: ${e.message}`); }
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
  const hitType  = alert.candleClose ? `Candle Close · ${alert.timeframe}` : 'Instant Hit';
  const priceStr = formatPrice(hitPrice, alert.pairSymbol);
  const dirLabel = alert.direction === 'above' ? '📈 Above' : '📉 Below';

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
    // Test email endpoint — hit this URL in browser to verify email alerts work
    if (req.url === '/test-email') {
      sendAlertEmail('test', 'TradeAlert server email test', 'This is a test email from your TradeAlert server.\nIf you received this, email alerts are working correctly.');
      // Reset cooldown immediately so a real alert can still fire
      delete emailCooldowns['test'];
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('Test email sent to ' + ALERT_TO + ' — check your inbox.');
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

  // Capital.com WebSocket for metals — session creation retries forever on 429/503,
  // so run it in the background. Gate.io, Yahoo, and alert checking start immediately
  // and don't depend on Capital.com being up.
  createCapSession().then(() => {
    connectCapWs();
  }).catch(e => warn(`Cap session initial error: ${e.message}`)); // won't fire — retries forever
  setInterval(pingCapSession, 9 * 60 * 1000);
  startCapWsWatchdog();

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

  // Minute-boundary candle close checker
  startMinuteBoundaryChecker();

  // Check server stop flag every minute
  setInterval(checkServerStopFlag, 60000);
  checkServerStopFlag();

  log('=== Trade Alert Server Running ===');
}

main().catch(e => {
  err(`Fatal: ${e.message}`);
  process.exit(1);
});
