/**
 * Trade Alert — Combined Server
 * Runs 24/7 on Render (Web Service, port required).
 *
 * Replaces:  price-bridge.js  +  Cloudflare Worker
 *
 * Flow:
 *   1. Capital.com WebSocket  → live XAU/XAG prices + OHLC candles
 *   2. MEXC WebSocket         → live crypto prices
 *   3. Yahoo Finance polling  → indices + forex (every 10s, no WebSocket)
 *   4. RTDB listener          → watches alerts node, maintains live alert cache
 *   5. Price checker          → every second, checks all cached alerts vs live prices
 *   6. On trigger             → FCM → delete RTDB alert → update Firestore
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

const DEV_EMAIL = 'dev.dreamlabs.org@gmail.com';

// ── Logging ──────────────────────────────────────────────────────────────────
function log(msg)  { console.log(`[${new Date().toISOString()}] ${msg}`); }
function warn(msg) { console.warn(`[${new Date().toISOString()}] ⚠️  ${msg}`); }
function err(msg)  { console.error(`[${new Date().toISOString()}] ❌ ${msg}`); }

// ── Live price store ─────────────────────────────────────────────────────────
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
  EURUSD:0, GBPUSD:0, USDJPY:0, GBPJPY:0, AUDUSD:0, USDGBP:0,
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
  log('Starting RTDB alerts listener...');

  const url = `${FIREBASE_URL}/alerts.json?auth=${FIREBASE_SECRET}&stream=true`;
  const u   = new URL(url);

  function connect() {
    const req = https.request({
      hostname: u.hostname,
      port:     443,
      path:     u.pathname + u.search,
      method:   'GET',
      headers:  { 'Accept': 'text/event-stream', 'Cache-Control': 'no-cache' }
    }, res => {
      log(`RTDB SSE connected: ${res.statusCode}`);
      let buf = '';

      res.on('data', chunk => {
        buf += chunk.toString();
        const lines = buf.split('\n');
        buf = lines.pop(); // keep incomplete line

        let event = null;
        let data  = null;

        for (const line of lines) {
          if (line.startsWith('event:')) {
            event = line.slice(6).trim();
          } else if (line.startsWith('data:')) {
            data = line.slice(5).trim();
          } else if (line === '' && event && data) {
            // Complete event
            handleRtdbEvent(event, data);
            event = null;
            data  = null;
          }
        }
      });

      res.on('end', () => {
        warn('RTDB SSE stream ended — reconnecting in 3s');
        setTimeout(connect, 3000);
      });

      res.on('error', e => {
        warn(`RTDB SSE error: ${e.message} — reconnecting in 3s`);
        setTimeout(connect, 3000);
      });
    });

    req.on('error', e => {
      warn(`RTDB SSE request error: ${e.message} — reconnecting in 5s`);
      setTimeout(connect, 5000);
    });

    req.end();
  }

  connect();
}

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
      // Full data dump on initial connect
      // value = { userId1: { alertId1: {...}, alertId2: {...} }, userId2: {...} }
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
      log(`RTDB initial load: ${Object.keys(activeAlerts).length} active alerts`);
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

    } else if (parts.length === 2) {
      const [userId, alertId] = parts;
      if (value === null) {
        // Alert deleted
        delete activeAlerts[alertId];
        log(`RTDB alert removed: ${alertId}`);
      } else {
        // Alert added or updated
        activeAlerts[alertId] = { ...value, userId };
        log(`RTDB alert added: ${alertId} (${value.pairSymbol} ${value.direction} ${value.targetPrice})`);
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

const metalOhlc = {
  XAU: { MINUTE:{}, MINUTE_5:{}, MINUTE_15:{}, HOUR:{} },
  XAG: { MINUTE:{}, MINUTE_5:{}, MINUTE_15:{}, HOUR:{} },
};

async function createCapSession() {
  log('Creating Capital.com session...');
  const res = await fetchJson(`${CAP_REST_URL}/api/v1/session`, {
    method:  'POST',
    headers: { 'X-CAP-API-KEY': CAP_API_KEY },
    body:    JSON.stringify({ identifier: CAP_EMAIL, password: CAP_PASSWORD, encryptedPassword: false })
  });

  // Headers come back on the raw response — use http.request directly
  return new Promise((resolve, reject) => {
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
          reject(new Error(`Capital.com session failed: ${res.statusCode} ${d}`));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
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
        const mid = bid > 0 && ask > 0 ? (bid + ask) / 2 : (bid || ask);
        if (mid > 0) {
          livePrice[sym] = mid;
          updateOpenCandle(sym, mid);
        }
      }

      // OHLC candle close
      if (msg.destination === 'ohlc.event' && sym) {
        const res = msg.payload.resolution;
        const c   = msg.payload.c;
        const h   = msg.payload.h;
        const l   = msg.payload.l;
        if (c && h && l) {
          metalOhlc[sym][res] = { h, l, c, t: msg.payload.t };
          if (res === 'MINUTE') {
            // Update m1Candle for miss-hit detection
            m1Candle[sym] = { high: h, low: l, close: c };
          }
        }
      }

    } catch(e) { /* ignore parse errors */ }
  });

  capWs.on('close', (code, reason) => {
    if (capWs._ping) { clearInterval(capWs._ping); capWs._ping = null; }
    warn(`Capital.com WS closed ${code} — reconnecting in 5s`);
    setTimeout(reconnectCapWs, 5000);
  });

  capWs.on('error', e => warn(`Capital.com WS error: ${e.message}`));
}

function reconnectCapWs() {
  if (capWs) { try { capWs.terminate(); } catch {} capWs = null; }
  connectCapWs();
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 5 — GATE.IO REST POLLING (Crypto prices)
// ════════════════════════════════════════════════════════════════════════════

const GATE_SYMBOLS = {
  BTC:'BTC_USDT', ETH:'ETH_USDT', BNB:'BNB_USDT', SOL:'SOL_USDT',
  XRP:'XRP_USDT', ADA:'ADA_USDT', DOGE:'DOGE_USDT', AVAX:'AVAX_USDT',
  DOT:'DOT_USDT', MATIC:'MATIC_USDT', LINK:'LINK_USDT', UNI:'UNI_USDT',
  ATOM:'ATOM_USDT', LTC:'LTC_USDT', BCH:'BCH_USDT', NEAR:'NEAR_USDT',
  ARB:'ARB_USDT', OP:'OP_USDT', SHIB:'SHIB_USDT', TRX:'TRX_USDT'
};

const GATE_REVERSE = {};
for (const [sym, gs] of Object.entries(GATE_SYMBOLS)) GATE_REVERSE[gs] = sym;

const MEXC_SYMBOLS = {
  BTC:'BTCUSDT', ETH:'ETHUSDT', BNB:'BNBUSDT', SOL:'SOLUSDT',
  XRP:'XRPUSDT', ADA:'ADAUSDT', DOGE:'DOGEUSDT', AVAX:'AVAXUSDT',
  DOT:'DOTUSDT', MATIC:'MATICUSDT', LINK:'LINKUSDT', UNI:'UNIUSDT',
  ATOM:'ATOMUSDT', LTC:'LTCUSDT', BCH:'BCHUSDT', NEAR:'NEARUSDT',
  ARB:'ARBUSDT', OP:'OPUSDT', SHIB:'SHIBUSDT', TRX:'TRXUSDT'
};

async function pollGateio() {
  try {
    // Gate.io tickers endpoint — returns all spot tickers in one call
    const res = await fetchJson('https://api.gateio.ws/api/v4/spot/tickers');
    if (!Array.isArray(res)) {
      warn(`Gate.io returned invalid response: ${JSON.stringify(res).slice(0,100)}`);
      return;
    }
    let updated = 0;
    for (const ticker of res) {
      const sym = GATE_REVERSE[ticker.currency_pair];
      const price = parseFloat(ticker.last) || 0;
      if (sym && price > 0) {
        livePrice[sym] = price;
        updateOpenCandle(sym, price);
        updated++;
      }
    }
    log(`Gate.io: ${updated} prices updated. BTC=${livePrice.BTC} ETH=${livePrice.ETH}`);
  } catch(e) { warn(`Gate.io poll error: ${e.message}`); }
}

// Gate.io REST for M1 candle close (for candle-close alerts)
async function pollGateioCandles() {
  for (const [sym, gSym] of Object.entries(GATE_SYMBOLS)) {
    try {
      const url = `https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair=${gSym}&interval=1m&limit=2`;
      const res = await fetchJson(url);
      // res = array of [timestamp, volume, close, high, low, open, ...]
      if (Array.isArray(res) && res.length >= 2) {
        const prev = res[0]; // first = oldest = previous closed candle
        m1Candle[sym] = {
          high:  parseFloat(prev[3]) || 0,
          low:   parseFloat(prev[4]) || 0,
          close: parseFloat(prev[2]) || 0,
        };
      }
    } catch(e) { /* skip */ }
  }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 6 — YAHOO FINANCE POLLING (Indices + Forex)
// ════════════════════════════════════════════════════════════════════════════

const YAHOO_SYMBOLS = {
  SPX500: '%5EGSPC', US30: '%5EDJI', US100: '%5EIXIC',
  DXY: 'DX-Y.NYB', NIF50: '%5ENSEI',
  EURUSD: 'EURUSD=X', GBPUSD: 'GBPUSD=X', USDJPY: 'USDJPY=X',
  GBPJPY: 'GBPJPY=X', AUDUSD: 'AUDUSD=X', USDGBP: 'GBPUSD=X'
};

async function pollYahoo() {
  for (const [sym, yahooSym] of Object.entries(YAHOO_SYMBOLS)) {
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${yahooSym}?interval=1m&range=2m`;
      const res = await fetchJson(url);
      const meta  = res?.chart?.result?.[0]?.meta;
      const price = parseFloat(meta?.regularMarketPrice) || 0;
      if (price > 0) livePrice[sym] = price;

      // For candle close alerts — get the last closed M1 candle
      const quotes = res?.chart?.result?.[0]?.indicators?.quote?.[0];
      const times  = res?.chart?.result?.[0]?.timestamp;
      if (quotes && times && times.length >= 2) {
        const i = times.length - 2; // second-to-last = last closed candle
        if (quotes.close?.[i] != null) {
          // Only update m1Candle close — used for candle-close alerts only
          if (!m1Candle[sym]) m1Candle[sym] = {};
          m1Candle[sym].close = quotes.close[i];
        }
      }
    } catch(e) { /* skip, try next poll */ }
  }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 7 — CANDLE BUILDER (tick-based M1 for candle close alerts)
// ════════════════════════════════════════════════════════════════════════════
// Builds the current open M1 candle from live ticks.
// When the minute rolls, the completed candle's close is saved to m1Candle.
// Used ONLY for candle-close alerts — instant alerts use live price directly.

function updateOpenCandle(sym, price) {
  const nowMin = Math.floor(Date.now() / 60000);
  if (!openCandle[sym]) {
    openCandle[sym] = { open: price, high: price, low: price, startMin: nowMin };
    return;
  }
  const oc = openCandle[sym];
  if (oc.startMin !== nowMin) {
    // Minute rolled — save completed candle close for candle-close alerts
    m1Candle[sym] = { high: oc.high, low: oc.low, close: oc.open };
    openCandle[sym] = { open: price, high: price, low: price, startMin: nowMin };
  } else {
    if (price > oc.high) oc.high = price;
    if (price < oc.low)  oc.low  = price;
  }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 8 — PRICE ALERT CHECKER
// ════════════════════════════════════════════════════════════════════════════

// Server is stopped flag (matches RTDB worker_control/stop)
let serverStopped = false;

async function checkServerStopFlag() {
  try {
    const val = await rtdbGet('worker_control/stop');
    serverStopped = val === true || val === 'true';
    if (serverStopped) log('Server stop flag is TRUE — only dev alerts will be checked');
  } catch(e) { /* keep current state */ }
}

let _checkCount = 0;
function checkAlerts() {
  _checkCount++;
  const alertList = Object.values(activeAlerts);

  // Log every 30 seconds so we know the checker is alive
  if (_checkCount % 30 === 0) {
    log(`Checker alive — ${alertList.length} alerts, BTC=${livePrice.BTC} XAU=${livePrice.XAU}`);
  }

  if (!alertList.length) return;

  const nowMs = Date.now();

  for (const alert of alertList) {
    try {
      // Skip if already being processed
      if (recentlyTriggered.has(alert.id)) continue;

      // Server stopped — only process dev account
      if (serverStopped && alert.userEmail !== DEV_EMAIL) continue;

      // Skip candle-close alerts created less than 2 minutes ago
      if (alert.candleClose) {
        const ageMs = nowMs - (alert.createdAt || 0);
        if (ageMs < 120000) continue;
      }

      const price = livePrice[alert.pairSymbol];
      if (!price || price <= 0) continue;

      const target = parseFloat(alert.targetPrice);
      if (!target) continue;

      let hit      = false;
      let hitPrice = price;

      if (!alert.candleClose) {
        // ── Instant alert ──────────────────────────────────────────────
        // Live WebSocket checks every second — no miss-hit needed.
        // If price ever touches the target, we catch it in real time.
        if (alert.direction === 'above' && price >= target) { hit = true; }
        if (alert.direction === 'below' && price <= target) { hit = true; }

      } else {
        // ── Candle close alert ─────────────────────────────────────────
        const candle = getCandleClose(alert.pairSymbol, alert.timeframe);
        if (!candle) continue;
        if (alert.direction === 'above' && candle >= target) { hit = true; hitPrice = candle; }
        if (alert.direction === 'below' && candle <= target) { hit = true; hitPrice = candle; }
      }

      if (hit) {
        log(`🎯 Alert triggered: ${alert.pairSymbol} ${alert.direction} ${target} (price=${hitPrice}) user=${alert.userId}`);
        recentlyTriggered.add(alert.id);
        // Process async but don't await — keep checker fast
        processTriggeredAlert(alert, hitPrice).catch(e => {
          err(`processTriggeredAlert error: ${e.message}`);
          recentlyTriggered.delete(alert.id); // allow retry
        });
      }

    } catch(e) { warn(`checkAlerts error for ${alert.id}: ${e.message}`); }
  }
}

function getCandleClose(sym, tf) {
  if (sym === 'XAU' || sym === 'XAG') {
    const resMap = { M1: 'MINUTE', M5: 'MINUTE_5', M15: 'MINUTE_15', H1: 'HOUR' };
    return metalOhlc[sym]?.[resMap[tf]]?.c || 0;
  }
  if (GATE_SYMBOLS[sym])   return m1Candle[sym]?.close || 0;
  if (YAHOO_SYMBOLS[sym])  return m1Candle[sym]?.close || 0;
  return 0;
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 9 — TRIGGER PROCESSING (FCM + Firestore + RTDB cleanup)
// ════════════════════════════════════════════════════════════════════════════

async function processTriggeredAlert(alert, hitPrice) {
  const alertId = alert.id;
  const userId  = alert.userId;
  const hitTime = Date.now();

  // Remove from local cache immediately to prevent double-fire
  delete activeAlerts[alertId];

  // 1. Delete from RTDB
  try {
    await rtdbDelete(`alerts/${userId}/${alertId}`);
    log(`  RTDB deleted: ${alertId}`);
  } catch(e) { warn(`  RTDB delete failed: ${e.message}`); }

  // 2. Delete from Firestore active_alerts field
  try {
    const token   = await getAccessToken();
    const docPath = `projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/alerts/${userId}/active_alerts/alerts`;
    const res2 = await fetchJson(
      `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`,
      {
        method:  'POST',
        headers: { 'Authorization': `Bearer ${token}` },
        body:    JSON.stringify({
          writes: [{
            update:     { name: docPath, fields: {} },
            updateMask: { fieldPaths: ['`' + alertId + '`'] }
          }]
        })
      }
    );
    if (res2?.error) warn(`  Firestore active_alerts delete error: ${JSON.stringify(res2.error)}`);
    else log(`  Firestore active_alerts field deleted: ${alertId}`);
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

  // 4. Update history_index
  try {
    await updateHistoryIndex(userId, alertId, hitTime);
    log(`  history_index updated for ${userId}`);
  } catch(e) { warn(`  history_index update failed: ${e.message}`); }

  // 5. Send FCM
  try {
    await sendFCM(userId, alert, hitPrice);
  } catch(e) { warn(`  FCM failed: ${e.message}`); }
}

async function updateHistoryIndex(userId, alertId, hitTime) {
  const HISTORY_LIMIT = 100;
  try {
    const doc = await firestoreGet(`alerts/${userId}/meta/history_index`);
    let entries = [];
    if (doc?.fields?.entries?.stringValue) {
      try { entries = JSON.parse(doc.fields.entries.stringValue); } catch {}
    }
    entries = entries.filter(e => e.id !== alertId);
    entries.unshift({ id: alertId, updatedAt: hitTime });
    entries.sort((a, b) => b.updatedAt - a.updatedAt);
    if (entries.length > HISTORY_LIMIT) entries = entries.slice(0, HISTORY_LIMIT);
    await firestorePatch(`alerts/${userId}/meta/history_index`, {
      entries:   { stringValue: JSON.stringify(entries) },
      updatedAt: { integerValue: String(hitTime) }
    });
  } catch(e) { throw e; }
}

async function sendFCM(userId, alert, hitPrice) {
  const token   = await getAccessToken();
  const BASE    = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents`;
  const userDoc = await fetchJson(`${BASE}/users/${userId}`, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  const fcmToken = userDoc?.fields?.fcmToken?.stringValue;
  if (!fcmToken) { warn(`No FCM token for user ${userId}`); return; }

  const isAlarm    = alert.alarm === true;
  const hitType    = alert.candleClose ? `Candle Close · ${alert.timeframe}` : 'Instant Hit';
  const priceStr   = formatPrice(hitPrice, alert.pairSymbol);
  const dirLabel   = alert.direction === 'above' ? '📈 Above' : '📉 Below';

  const message = {
    message: {
      token: fcmToken,
      data: {
        type:          'PRICE_ALERT',
        alertId:       alert.id,
        pairSymbol:    alert.pairSymbol,
        pairName:      alert.pairName,
        pairEmoji:     alert.pairEmoji || '',
        targetPrice:   String(alert.targetPrice),
        currentPrice:  String(hitPrice),
        direction:     alert.direction,
        isAlarm:       String(isAlarm),
        hitType,
        hitTime:       String(Date.now()),
        vibration:     String(alert.vibrationEnabled !== false),
        sound:         String(alert.soundEnabled !== false),
      },
      notification: isAlarm ? undefined : {
        title: `${alert.pairEmoji || ''} ${alert.pairName} Alert`,
        body:  `${dirLabel} ${priceStr} — ${hitType}`,
      },
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
  if (res?.name) {
    log(`  FCM sent to ${userId}: ${res.name}`);
  } else {
    warn(`  FCM response: ${JSON.stringify(res)}`);
  }
}

function formatPrice(price, sym) {
  if (!price) return '—';
  if (sym === 'XAU' || sym === 'XAG' || YAHOO_SYMBOLS[sym]) {
    return price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  if (price < 0.01) return price.toFixed(8);
  if (price < 1)    return price.toFixed(4);
  if (price < 100)  return price.toFixed(2);
  return price.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 10 — RTDB LIVE PRICE UPDATES (for app mobile display)
// ════════════════════════════════════════════════════════════════════════════

// Write XAU/XAG current price to RTDB every 5s so mobile app can display it
async function updateRtdbPrices() {
  if (!livePrice.XAU && !livePrice.XAG) return;
  try {
    await rtdbSet('prices', {
      xau: { current: livePrice.XAU || 0, updatedAt: Date.now() },
      xag: { current: livePrice.XAG || 0, updatedAt: Date.now() },
    });
  } catch(e) { warn(`RTDB price update failed: ${e.message}`); }
}

// ════════════════════════════════════════════════════════════════════════════
// SECTION 11 — HEALTH SERVER (keeps Render alive)
// ════════════════════════════════════════════════════════════════════════════

function startHealthServer() {
  const port = process.env.PORT || 3000;
  http.createServer((req, res) => {
    const alertCount  = Object.keys(activeAlerts).length;
    const priceCount  = Object.values(livePrice).filter(p => p > 0).length;
    const status = {
      ok:           true,
      alerts:       alertCount,
      prices:       `${priceCount}/${Object.keys(livePrice).length}`,
      stopped:      serverStopped,
      xau:          livePrice.XAU,
      xag:          livePrice.XAG,
      btc:          livePrice.BTC,
      uptime:       Math.floor(process.uptime()),
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

  // Validate env
  const required = ['CAP_EMAIL','CAP_PASSWORD','CAP_API_KEY','FIREBASE_URL',
                    'FIREBASE_SECRET','FIREBASE_PROJECT_ID','FIREBASE_SERVICE_ACCOUNT'];
  for (const k of required) {
    if (!process.env[k]) throw new Error(`Missing env var: ${k}`);
  }

  startHealthServer();

  // Start RTDB alert listener (SSE)
  startRtdbListener();

  // Start Capital.com WebSocket
  await createCapSession();
  connectCapWs();

  // Ping Capital.com session every 9 minutes
  setInterval(pingCapSession, 9 * 60 * 1000);

  // Poll Gate.io every 10 seconds for live crypto prices
  setInterval(pollGateio, 10000);
  pollGateio(); // immediate first poll

  // Poll Gate.io REST every 60 seconds for M1 candle close (candle-close alerts)
  setInterval(pollGateioCandles, 60000);
  pollGateioCandles();

  // Poll Yahoo Finance every 10 seconds for indices + forex
  setInterval(pollYahoo, 10000);
  pollYahoo(); // immediate first poll

  // Update RTDB prices every 5 seconds (for mobile app display)
  setInterval(updateRtdbPrices, 5000);

  // Check alerts every second
  setInterval(checkAlerts, 1000);

  // Check server stop flag every minute
  setInterval(checkServerStopFlag, 60000);
  checkServerStopFlag();

  log('=== Trade Alert Server Running ===');
}

main().catch(e => {
  err(`Fatal: ${e.message}`);
  process.exit(1);
});
