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
            const existingByTf = m1Candle[sym]?.byTf;
            m1Candle[sym] = { high: h, low: l, close: c, byTf: existingByTf || {} };
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
  DOT:'DOT_USDT', MATIC:'POL_USDT', LINK:'LINK_USDT', UNI:'UNI_USDT',
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

let gateWs = null;
let gateWsConnected = false;

function connectGateWs() {
  log('Connecting Gate.io WebSocket...');
  gateWs = new WebSocket('wss://api.gateio.ws/ws/v4/');

  gateWs.on('open', () => {
    log('Gate.io WebSocket connected');
    gateWsConnected = true;

    const tickers = Object.values(GATE_SYMBOLS);
    gateWs.send(JSON.stringify({
      time:    Math.floor(Date.now() / 1000),
      channel: 'spot.tickers',
      event:   'subscribe',
      payload: tickers
    }));

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
      if (msg.event === 'subscribe') {
        if (msg.error) warn(`Gate.io WS subscribe error: ${JSON.stringify(msg.error)}`);
        else log(`Gate.io WS subscribed: ${msg.channel}`);
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
    if (gateWs._ping) { clearInterval(gateWs._ping); gateWs._ping = null; }
    warn(`Gate.io WS closed ${code} — reconnecting in 5s`);
    setTimeout(connectGateWs, 5000);
  });

  gateWs.on('error', e => warn(`Gate.io WS error: ${e.message}`));
}

// Fallback REST polling — only runs if WebSocket is not connected
async function pollGateio() {
  if (gateWsConnected) return;
  try {
    const results = await Promise.all(
      Object.entries(GATE_SYMBOLS).map(async ([sym, gSym]) => {
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
    if (updated > 0) log(`Gate.io REST fallback: ${updated} prices. BTC=${livePrice.BTC}`);
  } catch(e) { warn(`Gate.io REST fallback error: ${e.message}`); }
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
  let updated = 0;
  for (const [sym, yahooSym] of Object.entries(YAHOO_SYMBOLS)) {
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
        updated++;
      } else {
        // Log error details for EURUSD to debug
        if (sym === 'EURUSD') {
          warn(`Yahoo EURUSD failed — response: ${JSON.stringify(res).slice(0, 200)}`);
        }
      }
    } catch(e) {
      if (sym === 'EURUSD') warn(`Yahoo EURUSD error: ${e.message}`);
    }
  }
  log(`Yahoo: ${updated}/${Object.keys(YAHOO_SYMBOLS).length} updated. EURUSD=${livePrice.EURUSD} SPX500=${livePrice.SPX500}`);
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
    serverStopped = val === true || val === 'true';
    if (serverStopped) log('Server stop flag is TRUE — only dev alerts will be checked');
  } catch(e) { /* keep current state */ }
}

let _checkCount = 0;
function checkAlerts() {
  _checkCount++;
  const alertList = Object.values(activeAlerts);

  if (_checkCount % 30 === 0) {
    log(`Checker alive — ${alertList.length} alerts, BTC=${livePrice.BTC} XAU=${livePrice.XAU} EURUSD=${livePrice.EURUSD}`);
    // Show all active instant alerts for debugging
    alertList.filter(a => !a.candleClose).forEach(a =>
      log(`  active: ${a.pairSymbol} ${a.direction} ${a.targetPrice} price=${livePrice[a.pairSymbol]}`)
    );
  }
  if (!alertList.length) return;

  const nowMs = Date.now();
  for (const alert of alertList) {
    try {
      if (recentlyTriggered.has(alert.id)) continue;
      if (serverStopped && alert.userEmail !== DEV_EMAIL) continue;
      if (alert.candleClose) continue; // handled by checkCandleCloseAlerts at minute boundary

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

  for (const tf of closedTfs) {
    const interval = gateIntervals[tf];
    for (const [sym, gSym] of Object.entries(GATE_SYMBOLS)) {
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

  // Yahoo M1 candle close for indices + forex
  for (const [sym, yahooSym] of Object.entries(YAHOO_SYMBOLS)) {
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${yahooSym}?interval=1m&range=5m`;
      const res = await fetchJson(url);
      const quotes = res?.chart?.result?.[0]?.indicators?.quote?.[0];
      const times  = res?.chart?.result?.[0]?.timestamp;
      if (quotes && times && times.length >= 2) {
        const i = times.length - 2;
        const close = quotes.close?.[i] || 0;
        if (close > 0) {
          if (!m1Candle[sym])       m1Candle[sym]       = {};
          if (!m1Candle[sym].byTf)  m1Candle[sym].byTf  = {};
          m1Candle[sym].byTf['M1'] = { close, high: quotes.high?.[i]||0, low: quotes.low?.[i]||0 };
          m1Candle[sym].close = close;
        }
      }
    } catch(e) { /* skip */ }
  }

  // Metals — Capital.com WebSocket stores OHLC in metalOhlc automatically
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
    const resMap = { M1: 'MINUTE', M5: 'MINUTE_5', M15: 'MINUTE_15', H1: 'HOUR' };
    return metalOhlc[sym]?.[resMap[tf]]?.c || 0;
  }
  return m1Candle[sym]?.byTf?.[tf]?.close || 0;
}

function startMinuteBoundaryChecker() {
  setInterval(() => {
    const secMs = Date.now() % 60000;
    if (secMs < 3000) {
      onMinuteClose().catch(e => warn(`onMinuteClose error: ${e.message}`));
    }
  }, 1000);
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
        vibration:    String(alert.vibrationEnabled !== false),
        sound:        String(alert.soundEnabled !== false),
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
    const status = {
      ok:       true,
      alerts:   Object.keys(activeAlerts).length,
      prices:   `${Object.values(livePrice).filter(p => p > 0).length}/${Object.keys(livePrice).length}`,
      stopped:  serverStopped,
      gateWs:   gateWsConnected,
      xau:      livePrice.XAU,
      xag:      livePrice.XAG,
      btc:      livePrice.BTC,
      uptime:   Math.floor(process.uptime()),
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

  // Capital.com WebSocket for metals
  await createCapSession();
  connectCapWs();
  setInterval(pingCapSession, 9 * 60 * 1000);

  // Gate.io WebSocket for instant crypto prices
  connectGateWs();
  // REST fallback — only runs if WebSocket is disconnected
  setInterval(pollGateio, 15000);

  // Yahoo Finance polling every 15 seconds for indices + forex
  setInterval(pollYahoo, 15000);
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
