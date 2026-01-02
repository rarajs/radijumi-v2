'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const basicAuth = require('basic-auth');

const multer = require('multer');
const XLSX = require('xlsx');
const ExcelJS = require('exceljs');

const { Pool } = require('pg');
const { DateTime } = require('luxon');

const app = express();

/* ===================== ENV ===================== */
const PORT = process.env.PORT || 8080;
const DATABASE_URL = process.env.DATABASE_URL;

const TZ = 'Europe/Riga';
const ENFORCE_WINDOW = String(process.env.ENFORCE_WINDOW || '0') === '1';

const PUBLIC_ORIGIN = (process.env.PUBLIC_ORIGIN || '').trim();

const ADMIN_USER = process.env.ADMIN_USER || '';
const ADMIN_PASS = process.env.ADMIN_PASS || '';

const RATE_LIMIT_SUBMIT_PER_10MIN = parseInt(process.env.RATE_LIMIT_SUBMIT_PER_10MIN || '20', 10);
const RATE_LIMIT_ADDR_PER_MIN = parseInt(process.env.RATE_LIMIT_ADDR_PER_MIN || '120', 10);
const RATE_LIMIT_LOOKUP_PER_MIN = parseInt(process.env.RATE_LIMIT_LOOKUP_PER_MIN || '120', 10);

if (!DATABASE_URL) {
  console.error('FATAL: DATABASE_URL is missing');
  process.exit(1);
}

/* ===================== DB ===================== */
const pool = new Pool({ connectionString: DATABASE_URL });

/* ===================== schema ensure ===================== */
async function ensureSchema() {
  // Core tables (safe)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS submissions (
      id bigserial PRIMARY KEY,
      client_submission_id uuid UNIQUE NOT NULL,
      subscriber_code text,
      contract_nr text,
      billing_batch_id bigint,
      client_name text,
      address text,
      source_origin text,
      user_agent text,
      ip text,
      client_meta jsonb,
      submitted_at timestamptz NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS submission_lines (
      id bigserial PRIMARY KEY,
      submission_id bigint NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
      contract_nr text,
      meter_no text,
      address text,
      meter_type text,
      period_from text,
      period_to text,
      next_verif_date text,
      last_reading_date text,
      previous_reading numeric,
      reading numeric,
      consumption numeric,
      stage text,
      notes text,
      qty_type text
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS billing_import_batches (
      id bigserial PRIMARY KEY,
      source_filename text,
      uploaded_at timestamptz NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS billing_meters_snapshot (
      id bigserial PRIMARY KEY,
      batch_id bigint NOT NULL REFERENCES billing_import_batches(id) ON DELETE CASCADE,
      subscriber_code text,
      contract_nr text,
      address_raw text,
      meter_serial text,
      last_reading numeric,
      last_reading_date text,
      next_verif_date text,
      period_from text,
      period_to text,
      meter_type text,
      stage text,
      notes text,
      qty_type text,
      client_name text
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS billing_meters_snapshot_batch_sub_idx ON billing_meters_snapshot(batch_id, subscriber_code);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS billing_meters_snapshot_batch_contract_idx ON billing_meters_snapshot(batch_id, contract_nr);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS submission_lines_sub_idx ON submission_lines(submission_id);`);

  // History per meter
  await pool.query(`
    CREATE TABLE IF NOT EXISTS history_monthly_meter (
      id bigserial PRIMARY KEY,
      contract_nr text NOT NULL,
      meter_no text NOT NULL,
      month text NOT NULL, -- YYYY-MM
      m3 numeric(14,2) NOT NULL DEFAULT 0,
      updated_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(contract_nr, meter_no, month)
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS history_monthly_meter_month_idx ON history_monthly_meter(month);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS history_monthly_meter_contract_meter_idx ON history_monthly_meter(contract_nr, meter_no);`);

  // contract -> email map (from history XLSX)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS contract_email_map (
      contract_nr text PRIMARY KEY,
      email text,
      updated_at timestamptz NOT NULL DEFAULT now()
    );
  `);

  // invite tokens (token on subscriber/month)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS invite_tokens (
      id bigserial PRIMARY KEY,
      month text NOT NULL, -- YYYY-MM (Europe/Riga)
      subscriber_code text NOT NULL,
      token_hash text NOT NULL UNIQUE,
      token_plain text,
      created_at timestamptz NOT NULL DEFAULT now(),
      expires_at timestamptz,
      UNIQUE(month, subscriber_code)
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS invite_tokens_month_idx ON invite_tokens(month);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS invite_tokens_sub_idx ON invite_tokens(subscriber_code);`);

  // contract submissions per month (LOCKED) for invite flow
  await pool.query(`
    CREATE TABLE IF NOT EXISTS contract_submissions (
      id bigserial PRIMARY KEY,
      month text NOT NULL, -- YYYY-MM (Europe/Riga)
      contract_nr text NOT NULL,
      submission_id bigint,
      submitted_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(month, contract_nr)
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS contract_submissions_month_idx ON contract_submissions(month);`);
}

/* ===================== middleware ===================== */
app.set('trust proxy', 1);

app.use(helmet({ contentSecurityPolicy: false }));
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: false, limit: '1mb' }));

// quick health (no DB)
app.get('/healthz', (req, res) => res.status(200).send('ok'));

// static + SPA
app.use(express.static(path.join(__dirname, 'public'), { etag: true, maxAge: '1h' }));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/i/:token', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

/* ===================== rate limiters ===================== */
const submitLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: RATE_LIMIT_SUBMIT_PER_10MIN,
  standardHeaders: true,
  legacyHeaders: false,
});
const addressesLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: RATE_LIMIT_ADDR_PER_MIN,
  standardHeaders: true,
  legacyHeaders: false,
});
const lookupLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: RATE_LIMIT_LOOKUP_PER_MIN,
  standardHeaders: true,
  legacyHeaders: false,
});

/* ===================== helpers ===================== */
function getSubmissionWindow(now = DateTime.now().setZone(TZ)) {
  const start = now.startOf('month').plus({ days: 24 }).startOf('day'); // 25th 00:00
  const end = now.endOf('month'); // last day 23:59:59.999
  const isOpen = now >= start && now <= end;
  return { timezone: TZ, now: now.toISO(), start: start.toISO(), end: end.toISO(), isOpen };
}
function isWindowOpen() {
  if (!ENFORCE_WINDOW) return true;
  return getSubmissionWindow().isOpen;
}

function getClientIp(req) { return req.ip || null; }
function getOriginOrReferer(req) {
  return { origin: (req.get('origin') || '').trim(), referer: (req.get('referer') || '').trim() };
}

function enforceSameOrigin(req, res) {
  if (!PUBLIC_ORIGIN) return res.status(500).json({ ok:false, error:'Server misconfigured: PUBLIC_ORIGIN missing' });
  const { origin, referer } = getOriginOrReferer(req);

  if (origin) {
    if (origin !== PUBLIC_ORIGIN) return res.status(403).json({ ok:false, error:'Forbidden origin' });
    return null;
  }
  if (referer) {
    if (!referer.startsWith(PUBLIC_ORIGIN + '/')) return res.status(403).json({ ok:false, error:'Forbidden referer' });
    return null;
  }
  return res.status(403).json({ ok:false, error:'Missing origin/referer' });
}
function enforceSameOriginSoft(req, res) {
  if (!PUBLIC_ORIGIN) return null;
  const { origin, referer } = getOriginOrReferer(req);
  if (origin && origin !== PUBLIC_ORIGIN) return res.status(403).json({ ok:false, error:'Forbidden origin' });
  if (referer && !referer.startsWith(PUBLIC_ORIGIN + '/')) return res.status(403).json({ ok:false, error:'Forbidden referer' });
  return null;
}

/* CSV injection guard */
function csvSanitize(value) {
  const s = value == null ? '' : String(value);
  return /^[=+\-@]/.test(s) ? "'" + s : s;
}
function csvEscape(value) {
  const s = value == null ? '' : String(value);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function toCSVRow(fields) {
  return fields.map(v => csvEscape(csvSanitize(v))).join(',') + '\n';
}

/* Basic auth middleware */
function requireBasicAuth(req, res, next) {
  if (!ADMIN_USER || !ADMIN_PASS) return res.status(500).send('Server misconfigured: ADMIN_USER/ADMIN_PASS missing');
  const creds = basicAuth(req);
  if (!creds || creds.name !== ADMIN_USER || creds.pass !== ADMIN_PASS) {
    res.set('WWW-Authenticate', 'Basic realm="Admin"');
    return res.status(401).send('Unauthorized');
  }
  next();
}

/* Subscriber/contract validation */
function pickSubscriberCode(bodyOrQuery) {
  const v = bodyOrQuery?.subscriber_code ?? bodyOrQuery?.abonenta_numurs ?? bodyOrQuery?.subscriberCode ?? bodyOrQuery?.subscriber;
  const digits = String(v ?? '').trim().replace(/\D+/g, '');
  if (/^\d{8}$/.test(digits)) return digits;
  return null;
}
function pickContractNr(bodyOrQuery) {
  const v = bodyOrQuery?.contract_nr ?? bodyOrQuery?.contractNr ?? bodyOrQuery?.contract;
  const s = String(v ?? '').trim();
  if (!s) return null;
  if (s.length > 80) return null;
  return s;
}
function normalizeMeterNo(v) {
  const s = String(v ?? '').trim();
  if (!/^\d+$/.test(s)) return null;
  return s;
}
function parseReading(value) {
  const s = String(value ?? '').trim().replace(',', '.');
  if (!/^\d+(\.\d{1,2})?$/.test(s)) return null;
  const num = Number(s);
  if (!Number.isFinite(num) || num < 0) return null;
  return s;
}

/* ===================== Invite helpers ===================== */
function currentMonthYYYYMM() {
  return DateTime.now().setZone(TZ).toFormat('yyyy-MM');
}
function newToken() {
  return crypto.randomBytes(32).toString('base64url');
}
function sha256Hex(s) {
  return crypto.createHash('sha256').update(String(s), 'utf8').digest('hex');
}
function getBaseUrl(req) {
  const proto = (req.get('x-forwarded-proto') || req.protocol || 'https').split(',')[0].trim();
  const host = req.get('host');
  return `${proto}://${host}`;
}
function isValidEmail(email) {
  const s = String(email || '').trim();
  if (!s) return false;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
}

/* ===================== Addresses loader from XLSX ===================== */
const ADDR_XLSX = path.join(__dirname, 'data', 'adresesJurmala.xlsx');
let addrCache = { loadedAt: 0, mtimeMs: 0, rows: [], geoByKey: new Map() };

function stripDiacritics(s) {
  return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}
function normalizeForSearch(s) {
  return stripDiacritics(String(s || '').trim().toLowerCase())
    .replace(/[^\p{L}\p{N}\s]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function addressBase(s) {
  return String(s || '').split(',')[0].trim();
}
function loadAddressesIfNeeded() {
  if (!fs.existsSync(ADDR_XLSX)) {
    addrCache = { loadedAt: Date.now(), mtimeMs: 0, rows: [], geoByKey: new Map() };
    return;
  }

  const stat = fs.statSync(ADDR_XLSX);
  const mtime = stat.mtimeMs;
  if (addrCache.loadedAt && addrCache.mtimeMs === mtime && addrCache.rows.length) return;

  const wb = XLSX.readFile(ADDR_XLSX);
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });

  const out = [];
  const seen = new Set();
  const geoByKey = new Map();

  for (const r of rows) {
    const std = String(r.STD || '').trim();
    if (!std) continue;

    const cleaned = std.split(',')[0].trim();
    if (!cleaned) continue;
    if (seen.has(cleaned)) continue;
    seen.add(cleaned);

    const lat = r.DD_N == null || r.DD_N === '' ? null : Number(r.DD_N);
    const lon = r.DD_E == null || r.DD_E === '' ? null : Number(r.DD_E);

    const key = normalizeForSearch(cleaned);
    out.push({ original: cleaned, key });

    if (!geoByKey.has(key) && Number.isFinite(lat) && Number.isFinite(lon)) {
      geoByKey.set(key, { lat, lon, original: cleaned });
    }
  }

  addrCache = { loadedAt: Date.now(), mtimeMs: mtime, rows: out, geoByKey };
  console.log(`ADDR_XLSX loaded: ${out.length} addresses (geo=${geoByKey.size})`);
}
function geoForAddress(addrRaw) {
  loadAddressesIfNeeded();
  const base = addressBase(addrRaw);
  const key = normalizeForSearch(base);
  return addrCache.geoByKey.get(key) || null;
}

function parseQuery(qRaw) {
  const q = normalizeForSearch(qRaw);
  const parts = q ? q.split(' ').filter(Boolean) : [];
  const nums = parts.filter(t => /^\d+$/.test(t));
  const words = parts.filter(t => /[a-zā-ž]/i.test(t));
  return { q, nums, words };
}
function hasHouseNumber(key, num) {
  const re = new RegExp(`(^|[^0-9])${num}[a-z]?([^0-9]|$)`, 'i');
  return re.test(key);
}

/* ===================== Billing snapshot upload ===================== */
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 35 * 1024 * 1024 }
});

function excelDateToISO(v) {
  if (v == null || v === '') return null;
  if (v instanceof Date) {
    const y = v.getUTCFullYear();
    const m = String(v.getUTCMonth() + 1).padStart(2, '0');
    const d = String(v.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }
  if (typeof v === 'number') {
    const d = XLSX.SSF.parse_date_code(v);
    if (!d) return null;
    return `${String(d.y).padStart(4,'0')}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
  }
  const s = String(v).trim();
  const m = s.match(/^(\d{2})\.(\d{2})\.(\d{4})/);
  if (m) return `${m[3]}-${m[2]}-${m[1]}`;
  const m2 = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m2) return `${m2[1]}-${m2[2]}-${m2[3]}`;
  return null;
}
function isoToMonth(isoDate) {
  if (!isoDate) return null;
  const s = String(isoDate).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  return s.slice(0, 7);
}

async function getLatestBillingBatchId(client) {
  const r = await client.query(`SELECT id FROM billing_import_batches ORDER BY uploaded_at DESC LIMIT 1`);
  return r.rowCount ? r.rows[0].id : null;
}
async function getLatestBillingBatchInfo() {
  const client = await pool.connect();
  try {
    const r = await client.query(`
      SELECT id, source_filename, uploaded_at
      FROM billing_import_batches
      ORDER BY uploaded_at DESC
      LIMIT 1
    `);
    return r.rowCount ? r.rows[0] : null;
  } finally {
    client.release();
  }
}

/* ===================== ROUTES ===================== */

app.get('/health', async (req, res) => {
  try {
    const r = await pool.query('SELECT 1 AS ok');
    res.json({ ok: true, db: r.rows[0].ok === 1 });
  } catch {
    res.status(500).json({ ok: false, error: 'db failed' });
  }
});

app.get('/api/window', (req, res) => {
  const info = getSubmissionWindow();
  res.json({
    ok: true,
    enforce: ENFORCE_WINDOW,
    timezone: TZ,
    now: info.now,
    start: info.start,
    end: info.end,
    is_open: info.isOpen,
    isOpen: info.isOpen,
  });
});

/* Addresses autocomplete */
app.get('/api/addresses', addressesLimiter, (req, res) => {
  const originError = enforceSameOriginSoft(req, res);
  if (originError) return;

  loadAddressesIfNeeded();

  const qRaw = String(req.query.q || '').trim();
  if (!qRaw) return res.json({ ok:true, items:[] });

  const { q, nums, words } = parseQuery(qRaw);
  const out = [];
  const limit = Math.min(parseInt(req.query.limit || '20', 10) || 20, 50);

  if (nums.length && words.length) {
    const prefix = words.join(' ');
    for (const r of addrCache.rows) {
      if (!r.key.startsWith(prefix)) continue;
      if (!nums.every(n => hasHouseNumber(r.key, n))) continue;
      out.push(r.original);
      if (out.length >= limit) break;
    }
    return res.json({ ok:true, items: out });
  }

  if (q && words.length && !nums.length) {
    for (const r of addrCache.rows) {
      if (r.key.startsWith(q)) {
        out.push(r.original);
        if (out.length >= limit) break;
      }
    }
    return res.json({ ok:true, items: out });
  }

  if (nums.length && !words.length) {
    const n0 = nums[0];
    for (const r of addrCache.rows) {
      if (hasHouseNumber(r.key, n0)) {
        out.push(r.original);
        if (out.length >= limit) break;
      }
    }
    return res.json({ ok:true, items: out });
  }

  return res.json({ ok:true, items: [] });
});

/* LOOKUP */
app.get('/api/lookup', lookupLimiter, async (req, res) => {
  const originError = enforceSameOriginSoft(req, res);
  if (originError) return;

  const subscriber = String(req.query.subscriber || '').trim().replace(/\D+/g, '');
  const contract = String(req.query.contract || '').trim();

  if (!/^\d{8}$/.test(subscriber)) return res.status(400).json({ ok:false, error:'Invalid subscriber' });
  if (!contract) return res.status(400).json({ ok:false, error:'Invalid contract' });

  const client = await pool.connect();
  try {
    const batchId = await getLatestBillingBatchId(client);
    if (!batchId) return res.json({ ok:true, found:false });

    const okMatch = await client.query(`
      SELECT 1
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code=$2 AND contract_nr=$3
      LIMIT 1
    `, [batchId, subscriber, contract]);

    if (!okMatch.rowCount) return res.json({ ok:true, found:false });

    const q = await client.query(`
      SELECT contract_nr, address_raw, meter_serial, last_reading, client_name
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code=$2
      ORDER BY contract_nr, address_raw, meter_serial
    `, [batchId, subscriber]);

    if (!q.rowCount) return res.json({ ok:true, found:false });

    const byAddr = new Map();
    const contracts = new Set();

    for (const r of q.rows) {
      const addr = r.address_raw || '';
      const c = r.contract_nr || '';
      if (c) contracts.add(c);

      if (!byAddr.has(addr)) byAddr.set(addr, []);
      byAddr.get(addr).push({
        meter_serial: r.meter_serial,
        last_reading: r.last_reading,
        contract_nr: r.contract_nr || null
      });
    }

    res.json({
      ok: true,
      found: true,
      batch_id: batchId,
      client_name: q.rows[0].client_name || null,
      contracts: Array.from(contracts),
      addresses: Array.from(byAddr.entries()).map(([address, meters]) => ({ address, meters }))
    });
  } catch (e) {
    console.error('lookup error', e);
    res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    client.release();
  }
});

/* HISTORY API */
app.get('/api/history', lookupLimiter, async (req, res) => {
  const originError = enforceSameOriginSoft(req, res);
  if (originError) return;

  const subscriber = String(req.query.subscriber || '').trim().replace(/\D+/g, '');
  const contract = String(req.query.contract || '').trim();
  const meter = String(req.query.meter || '').trim();

  if (!/^\d{8}$/.test(subscriber)) return res.status(400).json({ ok:false, error:'Invalid subscriber' });
  if (!contract) return res.status(400).json({ ok:false, error:'Invalid contract' });
  if (!meter) return res.status(400).json({ ok:false, error:'Invalid meter' });

  const client = await pool.connect();
  try {
    const batchId = await getLatestBillingBatchId(client);
    if (!batchId) return res.status(503).json({ ok:false, error:'Billing data not uploaded' });

    const auth = await client.query(`
      SELECT 1
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code=$2 AND contract_nr=$3 AND meter_serial=$4
      LIMIT 1
    `, [batchId, subscriber, contract, meter]);

    if (!auth.rowCount) return res.status(403).json({ ok:false, error:'Not allowed' });

    const last = await client.query(`
      SELECT month
      FROM history_monthly_meter
      WHERE contract_nr=$1 AND meter_no=$2
      ORDER BY month DESC
      LIMIT 1
    `, [contract, meter]);

    if (!last.rowCount) {
      return res.json({ ok:true, contract, meter, items: [] });
    }

    const lastMonth = last.rows[0].month; // YYYY-MM
    const dt0 = DateTime.fromFormat(lastMonth + '-01', 'yyyy-MM-dd', { zone: TZ }).startOf('month');

    const months = [];
    for (let i=11; i>=0; i--) months.push(dt0.minus({ months: i }).toFormat('yyyy-MM'));

    const q = await client.query(`
      SELECT month, m3
      FROM history_monthly_meter
      WHERE contract_nr=$1 AND meter_no=$2 AND month = ANY($3::text[])
    `, [contract, meter, months]);

    const map = new Map();
    for (const r of q.rows) {
      const v = Number(r.m3);
      map.set(r.month, (Number.isFinite(v) && v > 0) ? v : 0);
    }

    const items = months.map(m => ({ month: m, m3: map.get(m) ?? 0 }));
    return res.json({ ok:true, contract, meter, items });
  } catch (e) {
    console.error('history api error', e);
    res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    client.release();
  }
});

/* ===================== INVITE API ===================== */

app.get('/api/invite/resolve', lookupLimiter, async (req, res) => {
  const originError = enforceSameOriginSoft(req, res);
  if (originError) return;

  if (!isWindowOpen()) {
    return res.status(403).json({ ok:false, error:'WINDOW_CLOSED', window: getSubmissionWindow() });
  }

  const token = String(req.query.token || '').trim();
  if (!token) return res.status(400).json({ ok:false, error:'INVALID_LINK' });

  const month = currentMonthYYYYMM();
  const tokenHash = sha256Hex(token);

  const client = await pool.connect();
  try {
    const inv = await client.query(`
      SELECT subscriber_code, expires_at
      FROM invite_tokens
      WHERE month=$1 AND token_hash=$2
      LIMIT 1
    `, [month, tokenHash]);

    if (!inv.rowCount) return res.status(400).json({ ok:false, error:'INVALID_LINK' });

    const subscriber = String(inv.rows[0].subscriber_code || '').trim();
    if (!subscriber) return res.status(400).json({ ok:false, error:'INVALID_LINK' });

    const expiresAt = inv.rows[0].expires_at;
    if (expiresAt) {
      const nowUtc = DateTime.utc();
      const exp = DateTime.fromJSDate(expiresAt instanceof Date ? expiresAt : new Date(expiresAt)).toUTC();
      if (nowUtc > exp) return res.status(400).json({ ok:false, error:'INVALID_LINK' });
    }

    const batchId = await getLatestBillingBatchId(client);
    if (!batchId) return res.status(503).json({ ok:false, error:'Billing data not uploaded' });

    const q = await client.query(`
      SELECT contract_nr, address_raw, meter_serial, last_reading, client_name
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code=$2
      ORDER BY contract_nr, address_raw, meter_serial
    `, [batchId, subscriber]);

    if (!q.rowCount) return res.status(400).json({ ok:false, error:'INVALID_LINK' });

    const contracts = Array.from(new Set(q.rows.map(r => String(r.contract_nr || '').trim()).filter(Boolean)));

    let lockedSet = new Set();
    if (contracts.length) {
      const s = await client.query(`
        SELECT contract_nr
        FROM contract_submissions
        WHERE month=$1 AND contract_nr = ANY($2::text[])
      `, [month, contracts]);
      lockedSet = new Set(s.rows.map(r => String(r.contract_nr)));
    }

    const byAddr = new Map();
    for (const r of q.rows) {
      const addr = r.address_raw || '';
      if (!byAddr.has(addr)) byAddr.set(addr, []);
      const c = String(r.contract_nr || '').trim();
      byAddr.get(addr).push({
        meter_serial: r.meter_serial,
        last_reading: r.last_reading,
        contract_nr: c || null,
        locked: c ? lockedSet.has(c) : true
      });
    }

    const allLocked = contracts.length ? contracts.every(c => lockedSet.has(c)) : true;

    res.json({
      ok: true,
      month,
      subscriber_code: subscriber,
      all_locked: allLocked,
      addresses: Array.from(byAddr.entries()).map(([address, meters]) => ({ address, meters }))
    });
  } catch (e) {
    console.error('invite resolve error', e);
    res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    client.release();
  }
});

app.post('/api/invite/submit', submitLimiter, async (req, res) => {
  const originError = enforceSameOrigin(req, res);
  if (originError) return;

  if (!isWindowOpen()) {
    return res.status(403).json({ ok:false, error:'WINDOW_CLOSED', window: getSubmissionWindow() });
  }

  const hp = String(req.body.website || req.body.honeypot || '').trim();
  if (hp) return res.status(400).json({ ok:false, error:'Rejected' });

  const token = String(req.body.token || '').trim();
  if (!token) return res.status(400).json({ ok:false, error:'INVALID_LINK' });

  const rawLines = Array.isArray(req.body.lines) ? req.body.lines : [];
  if (!rawLines.length || rawLines.length > 800) return res.status(400).json({ ok:false, error:'Invalid lines' });

  const month = currentMonthYYYYMM();
  const tokenHash = sha256Hex(token);

  const db = await pool.connect();
  try {
    await db.query('BEGIN');

    const inv = await db.query(`
      SELECT subscriber_code
      FROM invite_tokens
      WHERE month=$1 AND token_hash=$2
      FOR UPDATE
    `, [month, tokenHash]);

    if (!inv.rowCount) {
      await db.query('ROLLBACK');
      return res.status(400).json({ ok:false, error:'INVALID_LINK' });
    }

    const subscriber = String(inv.rows[0].subscriber_code || '').trim();
    if (!subscriber) {
      await db.query('ROLLBACK');
      return res.status(400).json({ ok:false, error:'INVALID_LINK' });
    }

    const cleanLines = [];
    for (const l of rawLines) {
      const meter_no = normalizeMeterNo(l.meter_no);
      const contract_nr = String(l.contract_nr || '').trim();
      const readingStr = parseReading(l.reading);

      if (!meter_no || !contract_nr || readingStr == null) {
        await db.query('ROLLBACK');
        return res.status(400).json({ ok:false, error:'Invalid lines' });
      }
      cleanLines.push({ meter_no, contract_nr, reading: readingStr });
    }

    const contractsInPayload = Array.from(new Set(cleanLines.map(x => x.contract_nr)));

    const lockedQ = await db.query(`
      SELECT contract_nr
      FROM contract_submissions
      WHERE month=$1 AND contract_nr = ANY($2::text[])
    `, [month, contractsInPayload]);

    const lockedSet = new Set(lockedQ.rows.map(r => String(r.contract_nr)));
    const openContracts = contractsInPayload.filter(c => !lockedSet.has(c));
    const openLines = cleanLines.filter(x => openContracts.includes(x.contract_nr));

    if (!openLines.length) {
      await db.query('ROLLBACK');
      return res.json({ ok:true, newly_locked: [], locked_contracts: Array.from(lockedSet), all_locked: true });
    }

    const batchId = await getLatestBillingBatchId(db);
    if (!batchId) {
      await db.query('ROLLBACK');
      return res.status(503).json({ ok:false, error:'Billing data not uploaded' });
    }

    const snap = await db.query(`
      SELECT contract_nr, meter_serial, address_raw, last_reading, last_reading_date, next_verif_date,
             period_from, period_to, meter_type, stage, notes, qty_type, client_name
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code=$2 AND contract_nr = ANY($3::text[])
    `, [batchId, subscriber, openContracts]);

    if (!snap.rowCount) {
      await db.query('ROLLBACK');
      return res.status(400).json({ ok:false, error:'Invalid' });
    }

    const snapByKey = new Map();
    for (const r of snap.rows) snapByKey.set(String(r.contract_nr) + '|' + String(r.meter_serial), r);

    for (const x of openLines) {
      const key = x.contract_nr + '|' + x.meter_no;
      if (!snapByKey.has(key)) {
        await db.query('ROLLBACK');
        return res.status(400).json({ ok:false, error:'Meter mismatch' });
      }
    }

    const firstSnap = snap.rows[0];

    const submissionIdRes = await db.query(`
      INSERT INTO submissions (
        client_submission_id, subscriber_code, contract_nr, billing_batch_id, client_name,
        address, source_origin, user_agent, ip, client_meta
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
      RETURNING id
    `, [
      crypto.randomUUID(),
      subscriber,
      'INVITE',
      batchId,
      firstSnap.client_name || null,
      'MULTI',
      (req.get('origin') || req.get('referer') || null),
      req.get('user-agent') || null,
      getClientIp(req),
      JSON.stringify({ invite: true })
    ]);

    const submissionId = submissionIdRes.rows[0].id;

    const insertLineSql = `
      INSERT INTO submission_lines (
        submission_id, contract_nr, meter_no, address, meter_type, period_from, period_to,
        next_verif_date, last_reading_date, previous_reading, reading, consumption,
        stage, notes, qty_type
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,
        $10::numeric, $11::numeric, $12::numeric,
        $13,$14,$15
      )
    `;

    const newlyLocked = new Set();

    for (const x of openLines) {
      const s = snapByKey.get(x.contract_nr + '|' + x.meter_no);
      const prev = s.last_reading == null ? null : Number(s.last_reading);
      const cur = Number(String(x.reading));
      const cons = (prev == null) ? null : (cur - prev);

      await db.query(insertLineSql, [
        submissionId,
        x.contract_nr,
        x.meter_no,
        s.address_raw || null,
        s.meter_type || null,
        s.period_from || null,
        s.period_to || null,
        s.next_verif_date || null,
        s.last_reading_date || null,
        prev,
        cur,
        cons == null ? null : cons,
        s.stage || 'Sagatave',
        s.notes || null,
        s.qty_type || null,
      ]);

      newlyLocked.add(x.contract_nr);
    }

    for (const c of newlyLocked) {
      await db.query(`
        INSERT INTO contract_submissions (month, contract_nr, submission_id)
        VALUES ($1,$2,$3)
        ON CONFLICT (month, contract_nr) DO NOTHING
      `, [month, c, submissionId]);
    }

    await db.query('COMMIT');

    const allContractsQ = await pool.query(`
      SELECT DISTINCT contract_nr
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code=$2
    `, [batchId, subscriber]);

    const allContracts = allContractsQ.rows.map(r => String(r.contract_nr)).filter(Boolean);

    const lockedNowQ = await pool.query(`
      SELECT contract_nr
      FROM contract_submissions
      WHERE month=$1 AND contract_nr = ANY($2::text[])
    `, [month, allContracts]);

    const lockedNow = new Set(lockedNowQ.rows.map(r => String(r.contract_nr)));
    const allLocked = allContracts.length ? allContracts.every(c => lockedNow.has(c)) : true;

    return res.json({
      ok: true,
      newly_locked: Array.from(newlyLocked),
      locked_contracts: Array.from(lockedNow),
      all_locked: allLocked
    });
  } catch (e) {
    try { await db.query('ROLLBACK'); } catch {}
    console.error('invite submit error', e);
    return res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    db.release();
  }
});

/* ===================== SUBMIT ===================== */
app.post('/api/submit', submitLimiter, async (req, res) => {
  const originError = enforceSameOrigin(req, res);
  if (originError) return;

  if (!isWindowOpen()) {
    const info = getSubmissionWindow();
    return res.status(403).json({ ok:false, error:'Submission window closed', window: info });
  }

  const hp = String(req.body.website || req.body.honeypot || '').trim();
  if (hp) return res.status(400).json({ ok:false, error:'Rejected' });

  const mode = String(req.body.mode || 'manual').trim().toLowerCase();
  if (mode !== 'lookup' && mode !== 'manual') return res.status(400).json({ ok:false, error:'Invalid mode' });

  const subscriber_code = pickSubscriberCode(req.body);
  if (!subscriber_code) return res.status(400).json({ ok:false, error:'Invalid subscriber_code (must be 8 digits)' });

  const rawLines = Array.isArray(req.body.lines) ? req.body.lines : [];
  if (!rawLines.length || rawLines.length > 400) return res.status(400).json({ ok:false, error:'Invalid lines' });

  let client_submission_id = String(req.body.client_submission_id || req.body.clientSubmissionId || '').trim();
  if (client_submission_id) {
    if (!/^[0-9a-fA-F-]{36}$/.test(client_submission_id)) return res.status(400).json({ ok:false, error:'Invalid client_submission_id' });
  } else {
    client_submission_id = crypto.randomUUID();
  }

  const ip = getClientIp(req);
  const ua = req.get('user-agent') || null;
  const { origin, referer } = getOriginOrReferer(req);
  const source_origin = origin || (referer ? referer.slice(0, 500) : null);
  const clientMeta = { referer: referer || null, origin: origin || null };

  const db = await pool.connect();
  try {
    await db.query('BEGIN');

    if (mode === 'lookup') {
      const auth_contract_nr = pickContractNr(req.body);
      if (!auth_contract_nr) {
        await db.query('ROLLBACK');
        return res.status(400).json({ ok:false, error:'Invalid contract_nr' });
      }

      const cleanLines = [];
      for (const l of rawLines) {
        const meter_no = normalizeMeterNo(l.meter_no ?? l.skaititaja_numurs ?? l.skaititajaNr);
        if (!meter_no) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Invalid meter_no' }); }

        const lnContract = String(l.contract_nr ?? l.contractNr ?? l.contract ?? auth_contract_nr).trim();
        if (!lnContract || lnContract.length > 80) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Invalid contract_nr in lines' }); }

        const readingStr = parseReading(l.reading ?? l.radijums);
        if (readingStr == null) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Invalid reading' }); }

        cleanLines.push({ meter_no, reading: readingStr, contract_nr: lnContract });
      }

      const batchId = await getLatestBillingBatchId(db);
      if (!batchId) { await db.query('ROLLBACK'); return res.status(503).json({ ok:false, error:'Billing data not uploaded' }); }

      const auth = await db.query(`
        SELECT 1
        FROM billing_meters_snapshot
        WHERE batch_id=$1 AND subscriber_code=$2 AND contract_nr=$3
        LIMIT 1
      `, [batchId, subscriber_code, auth_contract_nr]);

      if (!auth.rowCount) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Subscriber/contract not found' }); }

      const contractsWanted = Array.from(new Set(cleanLines.map(x => x.contract_nr)));
      const snap = await db.query(`
        SELECT contract_nr, meter_serial, address_raw, last_reading, last_reading_date, next_verif_date,
               period_from, period_to, meter_type, stage, notes, qty_type, client_name
        FROM billing_meters_snapshot
        WHERE batch_id=$1 AND subscriber_code=$2 AND contract_nr = ANY($3::text[])
      `, [batchId, subscriber_code, contractsWanted]);

      if (!snap.rowCount) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Subscriber/contract not found' }); }

      const snapByKey = new Map();
      for (const r of snap.rows) snapByKey.set(String(r.contract_nr) + '|' + String(r.meter_serial), r);

      for (const x of cleanLines) {
        const key = String(x.contract_nr) + '|' + String(x.meter_no);
        if (!snapByKey.has(key)) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Meter mismatch' }); }
      }

      const firstSnap = snap.rows[0];

      const subRes = await db.query(`
        INSERT INTO submissions (
          client_submission_id, subscriber_code, contract_nr, billing_batch_id, client_name,
          address, source_origin, user_agent, ip, client_meta
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
        ON CONFLICT (client_submission_id)
        DO UPDATE SET
          subscriber_code = EXCLUDED.subscriber_code,
          contract_nr = EXCLUDED.contract_nr,
          billing_batch_id = EXCLUDED.billing_batch_id,
          client_name = EXCLUDED.client_name,
          address = EXCLUDED.address
        RETURNING id
      `, [
        client_submission_id,
        subscriber_code,
        auth_contract_nr,
        batchId,
        firstSnap.client_name || null,
        'MULTI',
        source_origin,
        ua,
        ip,
        JSON.stringify(clientMeta),
      ]);

      const submissionId = subRes.rows[0].id;
      await db.query('DELETE FROM submission_lines WHERE submission_id = $1', [submissionId]);

      const insertLineSql = `
        INSERT INTO submission_lines (
          submission_id, contract_nr, meter_no, address, meter_type, period_from, period_to,
          next_verif_date, last_reading_date, previous_reading, reading, consumption,
          stage, notes, qty_type
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,
          $10::numeric, $11::numeric, $12::numeric,
          $13,$14,$15
        )
      `;

      for (const x of cleanLines) {
        const s = snapByKey.get(String(x.contract_nr) + '|' + String(x.meter_no));
        const prev = s.last_reading == null ? null : Number(s.last_reading);
        const cur = Number(String(x.reading));
        const cons = (prev == null) ? null : (cur - prev);

        await db.query(insertLineSql, [
          submissionId,
          x.contract_nr,
          x.meter_no,
          s.address_raw || null,
          s.meter_type || null,
          s.period_from || null,
          s.period_to || null,
          s.next_verif_date || null,
          s.last_reading_date || null,
          prev,
          cur,
          cons == null ? null : cons,
          s.stage || 'Sagatave',
          s.notes || null,
          s.qty_type || null,
        ]);
      }

      await db.query('COMMIT');
      return res.json({ ok:true, submission_id: submissionId, client_submission_id });
    }

    // manual
    const cleanLines = [];
    for (const l of rawLines) {
      const address = String(l.adrese ?? l.address ?? '').trim();
      if (!address || address.length < 2 || address.length > 200) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Invalid address' }); }

      const meter_no = normalizeMeterNo(l.meter_no ?? l.skaititaja_numurs ?? l.skaititajaNr);
      if (!meter_no) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Invalid meter_no' }); }

      const readingStr = parseReading(l.reading ?? l.radijums);
      if (readingStr == null) { await db.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'Invalid reading' }); }

      cleanLines.push({ address, meter_no, reading: readingStr });
    }

    const addrSet = new Set(cleanLines.map(x => x.address));
    const submissionAddress = addrSet.size === 1 ? cleanLines[0].address : 'MULTI';

    const subRes = await db.query(`
      INSERT INTO submissions (client_submission_id, subscriber_code, address, source_origin, user_agent, ip, client_meta)
      VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
      ON CONFLICT (client_submission_id)
      DO UPDATE SET subscriber_code = EXCLUDED.subscriber_code, address = EXCLUDED.address
      RETURNING id
    `, [client_submission_id, subscriber_code, submissionAddress, source_origin, ua, ip, JSON.stringify(clientMeta)]);

    const submissionId = subRes.rows[0].id;
    await db.query('DELETE FROM submission_lines WHERE submission_id = $1', [submissionId]);

    for (const l of cleanLines) {
      await db.query(`
        INSERT INTO submission_lines (submission_id, contract_nr, meter_no, address, previous_reading, reading, consumption)
        VALUES ($1,$2,$3,$4,$5::numeric,$6::numeric,$7::numeric)
      `, [submissionId, null, l.meter_no, l.address, null, l.reading, null]);
    }

    await db.query('COMMIT');
    return res.json({ ok:true, submission_id: submissionId, client_submission_id });
  } catch (err) {
    try { await db.query('ROLLBACK'); } catch {}
    console.error('submit error', err);
    return res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    db.release();
  }
});

/* ===================== ADMIN pages (landing) ===================== */
function pageShell(title, bodyHtml) {
  return `<!doctype html>
<html lang="lv">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>${title}</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px;background:#f6f7f9;color:#111}
  .wrap{max-width:860px;margin:0 auto}
  .card{background:#fff;border:1px solid #d8dde3;border-radius:16px;padding:16px;box-shadow:0 6px 18px rgba(0,0,0,.06)}
  h1{margin:0 0 12px;font-size:18px;font-weight:900}
  .grid{display:grid;grid-template-columns:1fr;gap:10px}
  @media(min-width:720px){.grid{grid-template-columns:1fr 1fr}}
  a.btn,button.btn{display:block;text-decoration:none;text-align:center;padding:14px 12px;border-radius:14px;border:1px solid #d8dde3;background:#fff;font-weight:900;color:#111;cursor:pointer}
  a.btn:hover,button.btn:hover{background:#eef3f8}
  .muted{color:#666;font-size:12px;margin-top:10px}
  label{display:block;margin:10px 0 6px;font-weight:900}
  input,select{width:100%;padding:10px 12px;border-radius:12px;border:1px solid #d8dde3;font:inherit}
  .ok{background:#1f5f86;color:#fff;border-color:#1f5f86}
</style>
</head>
<body>
<div class="wrap"><div class="card">
${bodyHtml}
</div></div>
</body></html>`;
}

app.get('/admin', requireBasicAuth, async (req, res) => {
  const latest = await getLatestBillingBatchInfo();
  const latestHtml = latest
    ? `<div class="muted"><b>Billing snapshot:</b> batch #${latest.id} (${latest.source_filename || 'file'}) — ${String(latest.uploaded_at)}</div>`
    : `<div class="muted"><b>Billing snapshot:</b> nav ielādēts (lookup nestrādās).</div>`;

  const html = pageShell('Admin', `
    <h1>Admin</h1>
    ${latestHtml}
    <div class="muted">Izvēlies darbību:</div>
    <div class="grid" style="margin-top:12px">
      <a class="btn ok" href="/admin/billing">Ielādēt pēdējo periodu</a>
      <a class="btn ok" href="/admin/history">Ielādēt 12 mēnešu pārskatu</a>
      <a class="btn" href="/admin/analytics">Dashboard</a>
      <a class="btn" href="/admin/exports">Iesniegtie dati</a>
    </div>
    <div class="muted" style="margin-top:12px">Uzaicinājumi: <a href="/admin/invites">/admin/invites</a></div>
  `);
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(html);
});

app.get('/admin/analytics', requireBasicAuth, (req, res) => {
  const p = path.join(__dirname, 'public', 'admin-analytics.html');
  if (fs.existsSync(p)) return res.sendFile(p);
  res.status(404).send('admin-analytics.html not found');
});

/* Minimal exports page (lai /admin vairs nekrīt) */
app.get('/admin/exports', requireBasicAuth, async (req, res) => {
  const html = pageShell('Exports', `
    <h1>Iesniegtie dati</h1>
    <div class="grid" style="margin-top:12px">
      <a class="btn" href="/admin/exports/export.csv">Lejupielādēt CSV</a>
    </div>
    <div class="muted" style="margin-top:10px"><a href="/admin">← atpakaļ</a></div>
  `);
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(html);
});

app.get('/admin/exports/export.csv', requireBasicAuth, async (req, res) => {
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="submissions_export.csv"`);
  res.write(toCSVRow(['submitted_at','subscriber_code','contract_nr','address','meter_no','reading','consumption','ip']));

  const client = await pool.connect();
  try {
    const q = await client.query(`
      SELECT s.submitted_at, s.subscriber_code, s.contract_nr, s.address, l.meter_no, l.reading, l.consumption, s.ip
      FROM submissions s
      LEFT JOIN submission_lines l ON l.submission_id = s.id
      ORDER BY s.submitted_at DESC, s.id DESC, l.id ASC
      LIMIT 200000
    `);
    for (const r of q.rows) {
      res.write(toCSVRow([
        r.submitted_at ? new Date(r.submitted_at).toISOString() : '',
        r.subscriber_code || '',
        r.contract_nr || '',
        r.address || '',
        r.meter_no || '',
        r.reading == null ? '' : String(r.reading),
        r.consumption == null ? '' : String(r.consumption),
        r.ip || ''
      ]));
    }
    res.end();
  } catch (e) {
    console.error('export csv error', e);
    res.status(500);
    res.end('Export failed');
  } finally {
    client.release();
  }
});

/* ===================== Admin: invites ===================== */
app.get('/admin/invites', requireBasicAuth, async (req, res) => {
  const month = currentMonthYYYYMM();
  const baseUrl = getBaseUrl(req);

  const client = await pool.connect();
  try {
    const c = await client.query(`SELECT count(*)::int AS n FROM invite_tokens WHERE month=$1`, [month]);
    const n = c.rows[0]?.n || 0;

    const html = pageShell('Invites', `
      <h1>Uzaicinājumi (mēnesis: ${month})</h1>
      <div class="muted">Linku formāts: <b>${baseUrl}/i/&lt;token&gt;</b></div>
      <div class="muted">Tokeni šim mēnesim DB: <b>${n}</b></div>

      <form method="POST" action="/admin/invites/generate" style="margin-top:12px">
        <button class="btn ok" type="submit">Ģenerēt uzaicinājumus šim mēnesim</button>
      </form>

      <div class="grid" style="margin-top:12px">
        <a class="btn" href="/admin/invites/export.csv">Lejupielādēt CSV (email, link)</a>
        <a class="btn" href="/admin/invites/missing.csv">Lejupielādēt trūkstošos e-pastus</a>
      </div>

      <div class="muted" style="margin-top:10px"><a href="/admin">← atpakaļ</a></div>
    `);

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(html);
  } catch (e) {
    console.error('admin invites page error', e);
    res.status(500).send('Error');
  } finally {
    client.release();
  }
});

app.post('/admin/invites/generate', requireBasicAuth, async (req, res) => {
  const month = currentMonthYYYYMM();
  const now = DateTime.now().setZone(TZ);
  const expiresAt = now.endOf('month').toUTC().toISO();

  const client = await pool.connect();
  try {
    const batchId = await getLatestBillingBatchId(client);
    if (!batchId) return res.status(503).send('Billing snapshot nav ielādēts.');

    const subs = await client.query(`
      SELECT DISTINCT subscriber_code
      FROM billing_meters_snapshot
      WHERE batch_id = $1 AND subscriber_code IS NOT NULL
    `, [batchId]);

    await client.query('BEGIN');

    let created = 0;
    let already = 0;

    for (const r of subs.rows) {
      const subscriber = String(r.subscriber_code || '').trim();
      if (!subscriber) continue;

      const token = newToken();
      const tokenHash = sha256Hex(token);

      const ins = await client.query(`
        INSERT INTO invite_tokens (month, subscriber_code, token_hash, token_plain, expires_at)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (month, subscriber_code) DO NOTHING
      `, [month, subscriber, tokenHash, token, expiresAt]);

      if (ins.rowCount) created++;
      else already++;
    }

    await client.query('COMMIT');

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(pageShell('Invites OK', `
      <h1>OK — uzaicinājumi sagatavoti</h1>
      <div class="muted">Mēnesis: <b>${month}</b></div>
      <div class="muted">Jauni: <b>${created}</b></div>
      <div class="muted">Jau bija: <b>${already}</b></div>
      <div class="muted" style="margin-top:10px"><a href="/admin/invites">← atpakaļ</a></div>
    `));
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('invites generate error', e);
    res.status(500).send('Generate failed');
  } finally {
    client.release();
  }
});

app.get('/admin/invites/export.csv', requireBasicAuth, async (req, res) => {
  const month = currentMonthYYYYMM();
  const baseUrl = getBaseUrl(req);

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="invites_${month}.csv"`);
  res.write(toCSVRow(['email','link']));

  const client = await pool.connect();
  try {
    const batchId = await getLatestBillingBatchId(client);
    if (!batchId) { res.end(); return; }

    const q = await client.query(`
      SELECT DISTINCT
        i.subscriber_code,
        i.token_plain,
        b.contract_nr,
        cem.email
      FROM invite_tokens i
      JOIN billing_meters_snapshot b
        ON b.batch_id = $1 AND b.subscriber_code = i.subscriber_code
      LEFT JOIN contract_email_map cem
        ON cem.contract_nr = b.contract_nr
      WHERE i.month = $2
      ORDER BY i.subscriber_code, b.contract_nr
    `, [batchId, month]);

    const map = new Map();

    for (const r of q.rows) {
      const sub = String(r.subscriber_code || '').trim();
      const token = String(r.token_plain || '').trim();
      const email = String(r.email || '').trim();

      if (!map.has(sub)) map.set(sub, { token, emails: new Set() });
      const rec = map.get(sub);
      if (!rec.token && token) rec.token = token;
      if (isValidEmail(email)) rec.emails.add(email);
    }

    for (const rec of map.values()) {
      if (!rec.token) continue;
      const link = `${baseUrl}/i/${rec.token}`;
      for (const em of rec.emails) {
        res.write(toCSVRow([em, link]));
      }
    }

    res.end();
  } catch (e) {
    console.error('invites export error', e);
    res.status(500);
    res.end('Export failed');
  } finally {
    client.release();
  }
});

app.get('/admin/invites/missing.csv', requireBasicAuth, async (req, res) => {
  const month = currentMonthYYYYMM();

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="invites_missing_${month}.csv"`);
  res.write(toCSVRow(['subscriber_code','contract_nr','reason']));

  const client = await pool.connect();
  try {
    const batchId = await getLatestBillingBatchId(client);
    if (!batchId) { res.end(); return; }

    const q = await client.query(`
      SELECT DISTINCT
        i.subscriber_code,
        i.token_plain,
        b.contract_nr,
        cem.email
      FROM invite_tokens i
      JOIN billing_meters_snapshot b
        ON b.batch_id = $1 AND b.subscriber_code = i.subscriber_code
      LEFT JOIN contract_email_map cem
        ON cem.contract_nr = b.contract_nr
      WHERE i.month = $2
      ORDER BY i.subscriber_code, b.contract_nr
    `, [batchId, month]);

    for (const r of q.rows) {
      const sub = String(r.subscriber_code || '').trim();
      const token = String(r.token_plain || '').trim();
      const contract = String(r.contract_nr || '').trim();
      const email = String(r.email || '').trim();

      if (!token) {
        res.write(toCSVRow([sub, contract, 'NO_TOKEN']));
        continue;
      }
      if (!isValidEmail(email)) {
        res.write(toCSVRow([sub, contract, 'NO_EMAIL']));
      }
    }

    res.end();
  } catch (e) {
    console.error('invites missing export error', e);
    res.status(500);
    res.end('Export failed');
  } finally {
    client.release();
  }
});

/* ===================== error handling + START ===================== */
process.on('unhandledRejection', (err) => console.error('UNHANDLED REJECTION:', err));
process.on('uncaughtException', (err) => console.error('UNCAUGHT EXCEPTION:', err));

app.use((err, req, res, next) => {
  console.error('EXPRESS ERROR:', err);
  if (res.headersSent) return next(err);
  res.status(500).send('Server error');
});

(async () => {
  await ensureSchema();
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Listening on ${PORT}`);
    console.log(`PUBLIC_ORIGIN=${PUBLIC_ORIGIN || '(empty)'}`);
    console.log(`ENFORCE_WINDOW=${ENFORCE_WINDOW ? '1' : '0'} TZ=${TZ}`);
  });
})();
