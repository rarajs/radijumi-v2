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

// Optional: safer invite token storage (AES-256-GCM). Set in Railway ENV.
const INVITE_TOKEN_SECRET = (process.env.INVITE_TOKEN_SECRET || '').trim();

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
  // submissions
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

  // submission lines
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
  await pool.query(`CREATE INDEX IF NOT EXISTS submission_lines_sub_idx ON submission_lines(submission_id);`);

  // billing snapshot batches
  await pool.query(`
    CREATE TABLE IF NOT EXISTS billing_import_batches (
      id bigserial PRIMARY KEY,
      source_filename text,
      uploaded_at timestamptz NOT NULL DEFAULT now()
    );
  `);

  // billing snapshot rows
  await pool.query(`
    CREATE TABLE IF NOT EXISTS billing_meters_snapshot (
      id bigserial PRIMARY KEY,
      batch_id bigint NOT NULL REFERENCES billing_import_batches(id) ON DELETE CASCADE,
      meter_type text,
      contract_nr text,
      client_name text,
      subscriber_code text,
      address_raw text,
      period_from text,
      period_to text,
      meter_serial text,
      next_verif_date text,
      last_reading_date text,
      last_reading numeric,
      consumption numeric,
      reading numeric,
      stage text,
      notes text,
      qty_type text
    );
  `);
  await pool.query(`ALTER TABLE billing_meters_snapshot ADD COLUMN IF NOT EXISTS consumption numeric;`);
  await pool.query(`ALTER TABLE billing_meters_snapshot ADD COLUMN IF NOT EXISTS reading numeric;`);
  await pool.query(`ALTER TABLE billing_meters_snapshot ADD COLUMN IF NOT EXISTS meter_type text;`);
  await pool.query(`CREATE INDEX IF NOT EXISTS billing_meters_snapshot_batch_sub_idx ON billing_meters_snapshot(batch_id, subscriber_code);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS billing_meters_snapshot_batch_contract_idx ON billing_meters_snapshot(batch_id, contract_nr);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS billing_meters_snapshot_batch_meter_idx ON billing_meters_snapshot(batch_id, contract_nr, meter_serial);`);

  // history per meter: monthly m3 per (contract + meter + month)
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
      token_enc text,
      created_at timestamptz NOT NULL DEFAULT now(),
      expires_at timestamptz,
      UNIQUE(month, subscriber_code)
    );
  `);
  await pool.query(`ALTER TABLE invite_tokens ADD COLUMN IF NOT EXISTS token_plain text;`);
  await pool.query(`ALTER TABLE invite_tokens ADD COLUMN IF NOT EXISTS token_enc text;`);
  await pool.query(`CREATE INDEX IF NOT EXISTS invite_tokens_month_idx ON invite_tokens(month);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS invite_tokens_sub_idx ON invite_tokens(subscriber_code);`);

  // meter-level locks for invite flow
  await pool.query(`
    CREATE TABLE IF NOT EXISTS meter_submissions (
      id bigserial PRIMARY KEY,
      month text NOT NULL, -- YYYY-MM (Europe/Riga)
      contract_nr text NOT NULL,
      meter_no text NOT NULL,
      submission_id bigint,
      submitted_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(month, contract_nr, meter_no)
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS meter_submissions_month_idx ON meter_submissions(month);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS meter_submissions_contract_idx ON meter_submissions(contract_nr);`);
}

/* ===================== middleware ===================== */
app.set('trust proxy', 1);

app.use(helmet({ contentSecurityPolicy: false }));
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: false, limit: '2mb' }));

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
function currentMonthYYYYMM() {
  return DateTime.now().setZone(TZ).toFormat('yyyy-MM');
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

/* Validation helpers */
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
function isValidEmail(email) {
  const s = String(email || '').trim();
  if (!s) return false;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
}


function extractEmails(raw) {
  // Multiple emails are separated by ';' (sometimes also ',')
  return String(raw || '')
    .split(/[;,]/g)
    .map(s => s.trim())
    .filter(Boolean)
    .filter(isValidEmail);
}


/* ===================== Invite token crypto helpers ===================== */
function newToken() {
  return crypto.randomBytes(32).toString('base64url');
}
function sha256Hex(s) {
  return crypto.createHash('sha256').update(String(s), 'utf8').digest('hex');
}
function inviteKey32() {
  if (!INVITE_TOKEN_SECRET) return null;
  return crypto.createHash('sha256').update(INVITE_TOKEN_SECRET, 'utf8').digest(); // 32 bytes
}
function encryptInviteToken(tokenPlain) {
  const key = inviteKey32();
  if (!key) return null;
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ct = Buffer.concat([cipher.update(String(tokenPlain), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, ct]).toString('base64url');
}
function decryptInviteToken(tokenEnc) {
  const key = inviteKey32();
  if (!key) return null;
  const buf = Buffer.from(String(tokenEnc), 'base64url');
  if (buf.length < 12 + 16 + 1) return null;
  const iv = buf.subarray(0, 12);
  const tag = buf.subarray(12, 28);
  const ct = buf.subarray(28);
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);
  const pt = Buffer.concat([decipher.update(ct), decipher.final()]);
  return pt.toString('utf8');
}

function getBaseUrl(req) {
  const proto = (req.get('x-forwarded-proto') || req.protocol || 'https').split(',')[0].trim();
  const host = req.get('host');
  return `${proto}://${host}`;
}

/* ===================== Addresses loader from XLSX (lat/lon for dashboard) ===================== */
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

/* Prefix-only search + “12 bu” */
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

/* ===================== Upload ===================== */
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
async function listAvailableMonths() {
  const client = await pool.connect();
  try {
    const sql = `
      SELECT to_char(date_trunc('month', submitted_at AT TIME ZONE $1), 'YYYY-MM') AS month
      FROM submissions
      GROUP BY 1
      ORDER BY 1 DESC
    `;
    const r = await client.query(sql, [TZ]);
    return r.rows.map(x => x.month).filter(Boolean);
  } finally {
    client.release();
  }
}


async function listReportMonths() {
  const client = await pool.connect();
  try {
    const a = await client.query(`
      SELECT DISTINCT to_char(date_trunc('month', submitted_at AT TIME ZONE $1), 'YYYY-MM') AS m
      FROM submissions
      ORDER BY m DESC
    `, [TZ]);

    const b = await client.query(`
      SELECT DISTINCT month AS m
      FROM invite_tokens
      ORDER BY m DESC
    `);

    const set = new Set();
    for (const r of a.rows) if (r.m) set.add(r.m);
    for (const r of b.rows) if (r.m) set.add(r.m);

    return Array.from(set).sort().reverse();
  } finally {
    client.release();
  }
}


/* ===================== SSE live (Dashboard) ===================== */
const sseClients = new Set();
function sseSend(res, event, obj) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(obj)}\n\n`);
}
async function startPgListener() {
  const client = await pool.connect();
  await client.query('LISTEN submissions_live');
  client.on('notification', (msg) => {
    if (msg.channel !== 'submissions_live') return;
    let data = null;
    try { data = JSON.parse(msg.payload || '{}'); } catch { data = { raw: msg.payload }; }
    for (const res of sseClients) {
      try { sseSend(res, 'submission', data); } catch {}
    }
  });
  client.on('error', (e) => console.error('PG LISTEN error', e));
  console.log('PG LISTEN started: submissions_live');
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

/* LOOKUP: if subscriber+contract exists -> return ALL subscriber meters */
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

/* HISTORY API — per meter */
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

    // authorize by snapshot
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

    if (!last.rowCount) return res.json({ ok:true, contract, meter, items: [] });

    const lastMonth = last.rows[0].month;
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

/* ===================== Invite API (meter-level locks) ===================== */
app.get('/api/invite/resolve', lookupLimiter, async (req, res) => {
  const originError = enforceSameOriginSoft(req, res);
  if (originError) return;

  if (!isWindowOpen()) return res.status(403).json({ ok:false, error:'WINDOW_CLOSED', window: getSubmissionWindow() });

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

    let lockedMeterSet = new Set();
    if (contracts.length) {
      const s = await client.query(`
        SELECT contract_nr, meter_no
        FROM meter_submissions
        WHERE month=$1 AND contract_nr = ANY($2::text[])
      `, [month, contracts]);
      lockedMeterSet = new Set(s.rows.map(r => `${String(r.contract_nr)}|${String(r.meter_no)}`));
    }
    // Map: "contract|meter" -> { reading, consumption }
let submittedMap = new Map();
if (contracts.length) {
  const rr = await client.query(`
    SELECT ms.contract_nr, ms.meter_no, l.reading, l.consumption
    FROM meter_submissions ms
    JOIN submission_lines l
      ON l.submission_id = ms.submission_id
     AND l.contract_nr = ms.contract_nr
     AND l.meter_no = ms.meter_no
    WHERE ms.month = $1
      AND ms.contract_nr = ANY($2::text[])
  `, [month, contracts]);

  for (const r of rr.rows) {
    const k = `${String(r.contract_nr)}|${String(r.meter_no)}`;
    submittedMap.set(k, {
      reading: (r.reading == null) ? null : String(r.reading),
      consumption: (r.consumption == null) ? null : String(r.consumption)
    });
  }
}
    const byAddr = new Map();
    for (const r of q.rows) {
      const addr = r.address_raw || '';
      if (!byAddr.has(addr)) byAddr.set(addr, []);
      const c = String(r.contract_nr || '').trim();
      const m = String(r.meter_serial || '').trim();
      const key = (c && m) ? `${c}|${m}` : "";
	  const locked = (c && m) ? lockedMeterSet.has(key) : true;
	  const sub = (locked && key) ? (submittedMap.get(key) || null) : null;
	  byAddr.get(addr).push({
        meter_serial: r.meter_serial,
        last_reading: r.last_reading,
		contract_nr: c || null,
		locked,
        submitted_reading: sub ? sub.reading : null,
		submitted_consumption: sub ? sub.consumption : null
      });
    }

    const allLocked = q.rows.length ? q.rows.every(r => {
      const c = String(r.contract_nr || '').trim();
      const m = String(r.meter_serial || '').trim();
      return c && m ? lockedMeterSet.has(`${c}|${m}`) : true;
    }) : true;

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

  if (!isWindowOpen()) return res.status(403).json({ ok:false, error:'WINDOW_CLOSED', window: getSubmissionWindow() });

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
      SELECT contract_nr, meter_no
      FROM meter_submissions
      WHERE month=$1 AND contract_nr = ANY($2::text[])
    `, [month, contractsInPayload]);

    const lockedMeterSet = new Set(lockedQ.rows.map(r => `${String(r.contract_nr)}|${String(r.meter_no)}`));
    const openLines = cleanLines.filter(x => !lockedMeterSet.has(`${x.contract_nr}|${x.meter_no}`));

    if (!openLines.length) {
      await db.query('ROLLBACK');
      return res.json({ ok:true, newly_locked_meters: [], all_locked: true });
    }

    const batchId = await getLatestBillingBatchId(db);
    if (!batchId) {
      await db.query('ROLLBACK');
      return res.status(503).json({ ok:false, error:'Billing data not uploaded' });
    }

    const openContracts = Array.from(new Set(openLines.map(x => x.contract_nr)));

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
      JSON.stringify({
        invite: true,
        origin: (req.get('origin') || '').trim() || null,
        referer: (req.get('referer') || '').trim() || null,
        accept_language: (req.get('accept-language') || '').slice(0, 200) || null,
        sec_ch_ua: (req.get('sec-ch-ua') || '').slice(0, 200) || null,
        sec_ch_ua_platform: (req.get('sec-ch-ua-platform') || '').slice(0, 50) || null,
        sec_ch_ua_mobile: (req.get('sec-ch-ua-mobile') || '').slice(0, 10) || null,
        x_forwarded_for: (req.get('x-forwarded-for') || '').slice(0, 300) || null,
      })
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

    const newlyLockedMeters = [];

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

      await db.query(`
        INSERT INTO meter_submissions (month, contract_nr, meter_no, submission_id)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (month, contract_nr, meter_no) DO NOTHING
      `, [month, x.contract_nr, x.meter_no, submissionId]);

      newlyLockedMeters.push({ contract_nr: x.contract_nr, meter_no: x.meter_no });
    }

    await db.query('COMMIT');

    const allMetersQ = await pool.query(`
      SELECT contract_nr, meter_serial
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code=$2
    `, [batchId, subscriber]);

    const allKeys = allMetersQ.rows.map(r => `${String(r.contract_nr)}|${String(r.meter_serial)}`);
    const allContracts = Array.from(new Set(allMetersQ.rows.map(r => String(r.contract_nr)).filter(Boolean)));

    let lockedNowSet = new Set();
    if (allContracts.length) {
      const lockedNowQ = await pool.query(`
        SELECT contract_nr, meter_no
        FROM meter_submissions
        WHERE month=$1 AND contract_nr = ANY($2::text[])
      `, [month, allContracts]);
      lockedNowSet = new Set(lockedNowQ.rows.map(r => `${String(r.contract_nr)}|${String(r.meter_no)}`));
    }

    const allLocked = allKeys.length ? allKeys.every(k => lockedNowSet.has(k)) : true;

    return res.json({ ok:true, newly_locked_meters: newlyLockedMeters, all_locked: allLocked });
  } catch (e) {
    try { await db.query('ROLLBACK'); } catch {}
    console.error('invite submit error', e);
    return res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    db.release();
  }
});

/* ===================== SUBMIT (unchanged from your working version) ===================== */
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
  const clientMeta = {
    referer: referer || null,
    origin: origin || null,
    accept_language: (req.get('accept-language') || '').slice(0, 200) || null,
    sec_ch_ua: (req.get('sec-ch-ua') || '').slice(0, 200) || null,
    sec_ch_ua_platform: (req.get('sec-ch-ua-platform') || '').slice(0, 50) || null,
    sec_ch_ua_mobile: (req.get('sec-ch-ua-mobile') || '').slice(0, 10) || null,
    x_forwarded_for: (req.get('x-forwarded-for') || '').slice(0, 300) || null,
  };

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

      // LIVE notify: one message per address
      try {
        const p = await pool.query(`
          SELECT l.address, coalesce(sum(l.consumption),0)::numeric(14,2) AS consumption_sum
          FROM submission_lines l
          WHERE l.submission_id = $1 AND l.address IS NOT NULL
          GROUP BY l.address
        `, [submissionId]);

        for (const r of p.rows) {
          const g = geoForAddress(r.address);
          if (!g) continue;
          await pool.query(
            `SELECT pg_notify('submissions_live', $1)`,
            [JSON.stringify({
              address: r.address,
              lat: g.lat,
              lon: g.lon,
              consumption_sum: String(r.consumption_sum),
              submitted_at: new Date().toISOString()
            })]
          );
        }
      } catch (e) {
        console.warn('live notify failed', e);
      }

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

    try {
      const p = await pool.query(`
        SELECT DISTINCT l.address
        FROM submission_lines l
        WHERE l.submission_id = $1 AND l.address IS NOT NULL
      `, [submissionId]);

      for (const r of p.rows) {
        const g = geoForAddress(r.address);
        if (!g) continue;
        await pool.query(
          `NOTIFY submissions_live, $1`,
          [JSON.stringify({
            address: r.address,
            lat: g.lat,
            lon: g.lon,
            submitted_at: new Date().toISOString()
          })]
        );
      }
    } catch (e) {
      console.warn('live notify failed', e);
    }

    return res.json({ ok:true, submission_id: submissionId, client_submission_id });
  } catch (err) {
    try { await db.query('ROLLBACK'); } catch {}
    console.error('submit error', err);
    return res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    db.release();
  }
});

/* ===================== ADMIN: Landing + pages ===================== */

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
  .danger{border:1px solid #f0c9cf;background:#fff5f7;border-radius:14px;padding:12px;margin-top:14px}
  .danger h3{margin:0 0 6px;color:#7a0016}
  .ok{background:#1f5f86;color:#fff;border-color:#1f5f86}
</style>
</head>
<body>
<div class="wrap"><div class="card">
${bodyHtml}
</div></div>
</body></html>`;
}


app.get('/admin/reports', requireBasicAuth, async (req, res) => {
  const months = await listReportMonths();
  const optionsHtml = months.length
    ? months.map((m, i) => `<option value="${m}" ${i === 0 ? 'selected' : ''}>${m}</option>`).join('')
    : `<option value="" disabled selected>Nav datu</option>`;

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(pageShell('Atskaites', `
    <h1>Atskaites</h1>

    <div class="grid" style="margin-top:12px">

      <div>
        <h2 style="margin:0 0 6px">Rādījumi</h2>
        <div class="muted">Lejupielādēt saņemtos rādījumus</div>
        <label>Mēnesis</label>
        <select id="m1" ${months.length ? '' : 'disabled'}>${optionsHtml}</select>
        <div class="grid" style="margin-top:12px">
          <a class="btn ok" id="btnXlsx" href="#">XLSX export</a>
          <a class="btn" id="btnCsv" href="#">CSV debug export</a>
        </div>
      </div>

      <div>
        <h2 style="margin:0 0 6px">Neatbildētie uzaicinājumi</h2>
        <div class="muted">Atgādinājuma nosūtīšanai – ja par objektu nav saņemts rādījums</div>
        <label>Mēnesis</label>
        <select id="m2" ${months.length ? '' : 'disabled'}>${optionsHtml}</select>
        <div class="grid" style="margin-top:12px">
          <a class="btn ok" id="btnIncomplete" href="#">INVITE_INCOMPLETE.csv</a>
        </div>
      </div>

      <div>
        <h2 style="margin:0 0 6px">Tehniskā atskaite</h2>
        <div class="muted">IP, User-Agent, origin/referer u.c. (glabājam 3 mēnešus)</div>
        <label>Mēnesis</label>
        <select id="m3" ${months.length ? '' : 'disabled'}>${optionsHtml}</select>
        <div class="grid" style="margin-top:12px">
          <a class="btn ok" id="btnTech" href="#">TECHNICAL CSV</a>
        </div>
      </div>

    </div>

    <div class="muted" style="margin-top:12px"><a href="/admin">← atpakaļ</a></div>

    <script>
      const m1 = document.getElementById('m1');
      const m2 = document.getElementById('m2');
      const m3 = document.getElementById('m3');
      const btnXlsx = document.getElementById('btnXlsx');
      const btnCsv = document.getElementById('btnCsv');
      const btnIncomplete = document.getElementById('btnIncomplete');
      const btnTech = document.getElementById('btnTech');

      function sync() {
        btnXlsx.href = '/admin/export.xlsx?month=' + encodeURIComponent(m1.value || '');
        btnCsv.href = '/admin/export.csv?month=' + encodeURIComponent(m1.value || '');
        btnIncomplete.href = '/admin/invite_incomplete.csv?month=' + encodeURIComponent(m2.value || '');
        btnTech.href = '/admin/tech.csv?month=' + encodeURIComponent(m3.value || '');
      }
      m1.addEventListener('change', sync);
      m2.addEventListener('change', sync);
      m3.addEventListener('change', sync);
      sync();
    </script>
  `));
});


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
      <a class="btn" href="/admin/reports">Atskaites</a>
      <a class="btn" href="/admin/invites">Invite links</a>
    </div>
  `);
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(html);
});

app.get('/admin/billing', requireBasicAuth, async (req, res) => {
  const latest = await getLatestBillingBatchInfo();
  const latestHtml = latest
    ? `<div class="muted"><b>Pēdējais billing batch:</b> #${latest.id} (${latest.source_filename || 'file'}) — ${String(latest.uploaded_at)}</div>`
    : `<div class="muted"><b>Pēdējais billing batch:</b> nav ielādēts.</div>`;

  const html = pageShell('Billing upload', `
    <h1>Ielādēt pēdējo periodu (billing snapshot)</h1>
    ${latestHtml}
    <form method="POST" action="/admin/billing/upload" enctype="multipart/form-data" style="margin-top:12px">
      <label>XLSX fails (billing snapshot)</label>
      <input name="file" type="file" accept=".xlsx" required />
      <button class="btn ok" type="submit" style="margin-top:12px">Ielādēt XLSX</button>
    </form>
    <div class="muted" style="margin-top:10px"><a href="/admin">← atpakaļ</a></div>
  `);
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(html);
});

app.get('/admin/history', requireBasicAuth, async (req, res) => {
  const html = pageShell('History upload', `
    <h1>Ielādēt 12 mēnešu pārskatu (vēsturiskais patēriņš)</h1>
    <div class="muted">Imports: <b>līgums + skaitītājs + Periods līdz + Daudzums iev.</b> (negatīvs/trūkst → 0). Saglabā pa skaitītāju.</div>

    <form method="POST" action="/admin/history/upload" enctype="multipart/form-data" style="margin-top:12px">
      <label>XLSX fails (12 mēnešu pārskats)</label>
      <input name="file" type="file" accept=".xlsx" required />
      <button class="btn ok" type="submit" style="margin-top:12px">Ielādēt vēsturi</button>
    </form>

    <div class="muted" style="margin-top:10px"><a href="/admin">← atpakaļ</a></div>
  `);
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(html);
});

app.get('/admin/analytics', requireBasicAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin-analytics.html'));
});

app.get('/admin/exports', requireBasicAuth, (req, res) => res.redirect('/admin/reports'));

/* Admin SSE */
app.get('/admin/events', requireBasicAuth, (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  if (typeof res.flushHeaders === 'function') res.flushHeaders();

  sseClients.add(res);
  sseSend(res, 'hello', { ok:true, ts: new Date().toISOString() });

  req.on('close', () => {
    sseClients.delete(res);
  });
});

/* Admin analytics APIs */
app.get('/admin/api/latest', requireBasicAuth, async (req, res) => {
  const month = String(req.query.month || '').trim(); // YYYY-MM
  const limit = Math.min(parseInt(req.query.limit || '10', 10) || 10, 50);
  if (!/^\d{4}-\d{2}$/.test(month)) return res.status(400).json({ ok:false, error:'Invalid month' });

  const client = await pool.connect();
  try {
    const q = await client.query(`
      SELECT
        s.id,
        s.submitted_at,
        s.address,
        coalesce(sum(l.consumption), 0)::numeric(12,2) AS consumption_sum
      FROM submissions s
      JOIN submission_lines l ON l.submission_id = s.id
      WHERE to_char(date_trunc('month', s.submitted_at AT TIME ZONE $1), 'YYYY-MM') = $2
      GROUP BY s.id, s.submitted_at, s.address
      ORDER BY s.submitted_at DESC
      LIMIT $3
    `, [TZ, month, limit]);

    res.json({ ok:true, rows: q.rows });
  } catch (e) {
    console.error('latest api error', e);
    res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    client.release();
  }
});

app.get('/admin/api/by_hour', requireBasicAuth, async (req, res) => {
  const month = String(req.query.month || '').trim();
  if (!/^\d{4}-\d{2}$/.test(month)) return res.status(400).json({ ok:false, error:'Invalid month' });

  const client = await pool.connect();
  try {
    const q = await client.query(`
      SELECT
        to_char(date_trunc('hour', submitted_at AT TIME ZONE $1), 'YYYY-MM-DD HH24:00') AS hour,
        count(*)::int AS cnt
      FROM submissions
      WHERE to_char(date_trunc('month', submitted_at AT TIME ZONE $1), 'YYYY-MM') = $2
      GROUP BY 1
      ORDER BY 1
    `, [TZ, month]);

    res.json({ ok:true, rows: q.rows });
  } catch (e) {
    console.error('by_hour api error', e);
    res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    client.release();
  }
});

app.get('/admin/api/map_points', requireBasicAuth, async (req, res) => {
  const month = String(req.query.month || '').trim();
  if (!/^\d{4}-\d{2}$/.test(month)) return res.status(400).json({ ok:false, error:'Invalid month' });

  const client = await pool.connect();
  try {
    const q = await client.query(`
      SELECT DISTINCT l.address
      FROM submissions s
      JOIN submission_lines l ON l.submission_id = s.id
      WHERE to_char(date_trunc('month', s.submitted_at AT TIME ZONE $1), 'YYYY-MM') = $2
        AND l.address IS NOT NULL
    `, [TZ, month]);

    const pts = [];
    for (const r of q.rows) {
      const g = geoForAddress(r.address);
      if (!g) continue;
      pts.push({ address: r.address, lat: g.lat, lon: g.lon });
    }

    res.json({ ok:true, points: pts });
  } catch (e) {
    console.error('map_points api error', e);
    res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    client.release();
  }
});

/* Admin: clear */
app.post('/admin/clear', requireBasicAuth, async (req, res) => {
  const confirm = String(req.body.confirm || '').trim();
  if (confirm !== 'DELETE') return res.status(400).send('Nepareizs apstiprinājums. Ieraksti DELETE.');

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('TRUNCATE TABLE submission_lines, submissions RESTART IDENTITY CASCADE;');
    await client.query('COMMIT');
    return res.redirect('/admin/exports');
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('admin clear error', e);
    return res.status(500).send('Dzēšana neizdevās.');
  } finally {
    client.release();
  }
});

/* ===================== Admin: billing XLSX upload ===================== */
app.post('/admin/billing/upload', requireBasicAuth, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).send('No file');
  const filename = req.file.originalname || 'billing.xlsx';

  const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true });

  const headerRowIndex = rows.findIndex(r =>
    Array.isArray(r) &&
    r.map(x => String(x ?? '').trim()).includes('Klienta kods') &&
    r.map(x => String(x ?? '').trim()).includes('Līg. Nr.')
  );
  if (headerRowIndex === -1) return res.status(400).send('Header not found');

  const header = rows[headerRowIndex].map(x => String(x ?? '').trim());
  const col = (name) => header.indexOf(name);

  const idx = {
    meter_type: col('Skait. veids'),
    contract: col('Līg. Nr.'),
    client: col('Klients'),
    subscriber: col('Klienta kods'),
    address: col('NĪO adrese'),
    p_from: col('Periods no'),
    p_to: col('Periods līdz'),
    meter: col('Skait. eks. Nr'),
    next_verif: col('Nāk. verifikācijas datums'),
    last_date: col('Pēdējā rādījuma datums'),
    last_val: col('Pēdējais rādījums'),
    cons: col('Daudzums iev.'),
    reading: col('Rādījums'),
    stage: col('Stadija'),
    notes: col('Piezīmes'),
    qty_type: col('Daudzuma tips'),
  };

  if (idx.contract < 0 || idx.subscriber < 0 || idx.meter < 0) {
    return res.status(400).send('Missing required columns');
  }

  const dataRows = rows.slice(headerRowIndex + 1);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const b = await client.query(
      `INSERT INTO billing_import_batches (source_filename) VALUES ($1) RETURNING id`,
      [filename]
    );
    const batchId = b.rows[0].id;

    const sql = `
      INSERT INTO billing_meters_snapshot (
        batch_id,
        meter_type,
        contract_nr,
        client_name,
        subscriber_code,
        address_raw,
        period_from,
        period_to,
        meter_serial,
        next_verif_date,
        last_reading_date,
        last_reading,
        consumption,
        reading,
        stage,
        notes,
        qty_type
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
    `;

    const seen = new Set();
    for (const r of dataRows) {
      if (!Array.isArray(r) || r.length === 0) continue;

      const subscriber = String(r[idx.subscriber] ?? '').trim();
      const contract = String(r[idx.contract] ?? '').trim();
      const meter = String(r[idx.meter] ?? '').trim();
      if (!subscriber || !contract || !meter) continue;

      const key = `${batchId}|${contract}|${meter}`;
      if (seen.has(key)) continue;
      seen.add(key);

      const lastRaw = idx.last_val >= 0 ? r[idx.last_val] : null;
      const lastNum = (lastRaw == null || lastRaw === '') ? null : Number(lastRaw);

      const consRaw = idx.cons >= 0 ? r[idx.cons] : null;
      const consNum = (consRaw == null || consRaw === '') ? null : Number(consRaw);

      const readingRaw = idx.reading >= 0 ? r[idx.reading] : null;
      const readingNum = (readingRaw == null || readingRaw === '') ? null : Number(readingRaw);

      await client.query(sql, [
        batchId,
        idx.meter_type >= 0 ? (String(r[idx.meter_type] ?? '').trim() || null) : null,
        contract,
        idx.client >= 0 ? (String(r[idx.client] ?? '').trim() || null) : null,
        subscriber,
        idx.address >= 0 ? (String(r[idx.address] ?? '').trim() || null) : null,
        idx.p_from >= 0 ? excelDateToISO(r[idx.p_from]) : null,
        idx.p_to >= 0 ? excelDateToISO(r[idx.p_to]) : null,
        meter,
        idx.next_verif >= 0 ? excelDateToISO(r[idx.next_verif]) : null,
        idx.last_date >= 0 ? excelDateToISO(r[idx.last_date]) : null,
        Number.isFinite(lastNum) ? lastNum : null,
        Number.isFinite(consNum) ? consNum : null,
        Number.isFinite(readingNum) ? readingNum : null,
        idx.stage >= 0 ? (String(r[idx.stage] ?? '').trim() || null) : null,
        idx.notes >= 0 ? (String(r[idx.notes] ?? '').trim() || null) : null,
        idx.qty_type >= 0 ? (String(r[idx.qty_type] ?? '').trim() || null) : null
      ]);
    }

    await client.query('COMMIT');
    return res.redirect('/admin/billing');
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('billing upload error', e);
    res.status(500).send('Upload failed');
  } finally {
    client.release();
  }
});


/* ===================== Admin: history XLSX upload (per meter) ===================== */
function normCell(x) {
  return String(x ?? '')
    .replace(/\uFEFF/g, '')      // BOM
    .replace(/\u00A0/g, ' ')     // NBSP
    .replace(/[\u200B-\u200D]/g,'') // zero-width
    .replace(/\r/g,'')
    .replace(/\s+/g,' ')
    .trim();
}

function findHeaderRow(rows, wantedNames) {
  const want = wantedNames.map(normCell);
  for (let i = 0; i < Math.min(rows.length, 300); i++) {
    const r = rows[i];
    if (!Array.isArray(r)) continue;
    const cells = r.map(normCell).filter(Boolean);
    let hit = 0;
    for (const w of want) {
      // dažreiz šūnā ir papildus teksts → ļaujam "contains"
      if (cells.some(c => c === w || c.includes(w))) hit++;
    }
    if (hit >= 3) return i;
  }
  return -1;
}

function findColIndex(headerRow, name) {
  const target = normCell(name);
  for (let i = 0; i < headerRow.length; i++) {
    const c = normCell(headerRow[i]);
    if (!c) continue;
    if (c === target || c.includes(target)) return i;
  }
  return -1;
}

app.post('/admin/history/upload', requireBasicAuth, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).send('No file');

  const wb = XLSX.read(req.file.buffer, { type: 'buffer' });

  const WANT = ['NĪPLigPap.NĪPLīg.Numurs', 'SkaE.Numurs', 'Periods līdz', 'Daudzums iev.'];

  // atrod sheet + header rindu
  let sheetName = null;
  let headerRowIndex = -1;
  let rows = null;

  for (const sn of wb.SheetNames) {
    const ws = wb.Sheets[sn];
    const r = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true });
    const idx = findHeaderRow(r, WANT);
    if (idx !== -1) {
      sheetName = sn;
      headerRowIndex = idx;
      rows = r;
      break;
    }
  }

  if (!rows) return res.status(400).send('History header not found (need contract/meter/period/consumption columns).');

  const header = rows[headerRowIndex];

  const iContract = findColIndex(header, 'NĪPLigPap.NĪPLīg.Numurs');
  const iMeter    = findColIndex(header, 'SkaE.Numurs');
  const iPeriodTo = findColIndex(header, 'Periods līdz');
  const iQty      = findColIndex(header, 'Daudzums iev.');
  const iEmail    = findColIndex(header, 'NĪPLigPap.NĪPLīg.Ab.E-pasts');

  if (iContract < 0 || iMeter < 0 || iPeriodTo < 0 || iQty < 0) {
    return res.status(400).send('Missing required history columns.');
  }

  const dataRows = rows.slice(headerRowIndex + 1);

  const agg = new Map();               // contract|meter|month -> m3
  const emailCounts = new Map();       // contract -> Map(email -> count)

  for (const r of dataRows) {
    if (!Array.isArray(r) || !r.length) continue;

    const contract = normCell(r[iContract]);
    const meterNo  = normCell(r[iMeter]);
    const periodToIso = excelDateToISO(r[iPeriodTo]);
    const month = isoToMonth(periodToIso);

    if (!contract || !meterNo || !month) continue;

    const qtyRaw = r[iQty];
    let m3 = (qtyRaw == null || qtyRaw === '') ? 0 : Number(qtyRaw);
    if (!Number.isFinite(m3) || m3 < 0) m3 = 0;

    const key = `${contract}|${meterNo}|${month}`;
    agg.set(key, (agg.get(key) || 0) + m3);

    if (iEmail >= 0) {
      const email = normCell(r[iEmail]);
      if (isValidEmail(email)) {
        if (!emailCounts.has(contract)) emailCounts.set(contract, new Map());
        const m = emailCounts.get(contract);
        m.set(email, (m.get(email) || 0) + 1);
      }
    }
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const up = `
      INSERT INTO history_monthly_meter (contract_nr, meter_no, month, m3, updated_at)
      VALUES ($1,$2,$3,$4::numeric, now())
      ON CONFLICT (contract_nr, meter_no, month)
      DO UPDATE SET m3 = EXCLUDED.m3, updated_at = now()
    `;

    let count = 0;
    for (const [key, sum] of agg.entries()) {
      const [contract, meterNo, month] = key.split('|');
      const v = Number.isFinite(sum) && sum > 0 ? sum : 0;
      await client.query(up, [contract, meterNo, month, v.toFixed(2)]);
      count++;
    }

    // contract -> email (choose most frequent)
    for (const [contract, m] of emailCounts.entries()) {
      let bestEmail = '';
      let bestCnt = -1;
      for (const [em, cnt] of m.entries()) {
        if (cnt > bestCnt) { bestCnt = cnt; bestEmail = em; }
      }
      if (bestEmail) {
        await client.query(`
          INSERT INTO contract_email_map (contract_nr, email, updated_at)
          VALUES ($1,$2, now())
          ON CONFLICT (contract_nr) DO UPDATE SET email=EXCLUDED.email, updated_at=now()
        `, [contract, bestEmail]);
      }
    }

    await client.query('COMMIT');

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(pageShell('History OK', `
      <h1>OK — vēsture ielādēta</h1>
      <div class="muted">Sheet: <b>${sheetName}</b></div>
      <div class="muted">Importētas/atjaunotas rindas: <b>${count}</b></div>
      <div class="muted" style="margin-top:10px"><a href="/admin/history">← atpakaļ</a></div>
    `));
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('history upload error', e);
    res.status(500).send('History upload failed');
  } finally {
    client.release();
  }
});



/* ===================== Exports CSV/XLSX ===================== */
async function exportCsv(res, req) {
  const month = String(req?.query?.month || '').trim();

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="export.csv"');

  res.write(toCSVRow([
    'submission_id','client_submission_id','subscriber_code','contract_nr','address',
    'submitted_at_utc','meter_no','previous_reading','reading','consumption'
  ]));

  const client = await pool.connect();
  try {
    let sql = `
      SELECT
        s.id AS submission_id,
        s.client_submission_id,
        s.subscriber_code,
        COALESCE(l.contract_nr, s.contract_nr) AS contract_nr,
        s.address,
        (s.submitted_at AT TIME ZONE 'UTC') AS submitted_at_utc,
        l.meter_no,
        l.previous_reading,
        l.reading,
        l.consumption
      FROM submissions s
      JOIN submission_lines l ON l.submission_id = s.id
    `;

    const params = [];
    if (/^\d{4}-\d{2}$/.test(month)) {
      sql += ` WHERE to_char(date_trunc('month', s.submitted_at AT TIME ZONE $1), 'YYYY-MM') = $2`;
      params.push(TZ, month);
    }

    sql += ` ORDER BY s.submitted_at DESC, s.id DESC, l.id ASC`;

    const result = await client.query(sql, params);
    for (const r of result.rows) {
      res.write(toCSVRow([
        r.submission_id,
        r.client_submission_id,
        r.subscriber_code,
        r.contract_nr || '',
        r.address || '',
        r.submitted_at_utc instanceof Date ? r.submitted_at_utc.toISOString() : String(r.submitted_at_utc),
        r.meter_no,
        r.previous_reading == null ? '' : r.previous_reading,
        r.reading,
        r.consumption == null ? '' : r.consumption
      ]));
    }
    res.end();
  } catch (err) {
    console.error('export error', err);
    if (!res.headersSent) res.status(500);
    res.end('Export failed');
  } finally {
    client.release();
  }
}

app.get('/admin/export.csv', requireBasicAuth, async (req, res) => {
  await exportCsv(res, req);
});

const TEMPLATE_PATH = path.join(__dirname, 'data', 'billing_template.xlsx');

function toExcelDate(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  const s = String(v).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  return new Date(s + 'T00:00:00Z');
}
function findHeaderMap(ws, headersWanted) {
  for (let r = 1; r <= 15; r++) {
    const row = ws.getRow(r);
    const map = new Map();
    let hit = 0;

    for (let c = 1; c <= row.cellCount; c++) {
      const v = row.getCell(c).value;
      const s = (v && typeof v === 'object' && v.richText)
        ? v.richText.map(x => x.text).join('')
        : (v == null ? '' : String(v));
      const t = s.trim();
      if (!t) continue;
      if (headersWanted.includes(t)) { map.set(t, c); hit++; }
    }

    if (hit >= Math.min(5, headersWanted.length)) return { headerRow: r, map };
  }
  return null;
}

app.get('/admin/export.xlsx', requireBasicAuth, async (req, res) => {
  const month = String(req?.query?.month || '').trim();

  if (!fs.existsSync(TEMPLATE_PATH)) return res.status(500).send('Template missing: data/billing_template.xlsx');

  const client = await pool.connect();
  try {
    let sql = `
      SELECT
        COALESCE(l.contract_nr, s.contract_nr) AS contract_nr,
        s.client_name,
        s.subscriber_code,
        l.address,
        l.period_from,
        l.period_to,
        l.meter_no,
        l.next_verif_date,
        l.last_reading_date,
        l.previous_reading,
        l.consumption,
        l.reading,
        COALESCE(l.stage,'Sagatave') AS stage,
        l.notes,
        l.qty_type,
        l.meter_type
      FROM submissions s
      JOIN submission_lines l ON l.submission_id = s.id
    `;

    const params = [];
    if (/^\d{4}-\d{2}$/.test(month)) {
      sql += ` WHERE to_char(date_trunc('month', s.submitted_at AT TIME ZONE $1), 'YYYY-MM') = $2`;
      params.push(TZ, month);
    }
    sql += ` ORDER BY s.submitted_at DESC, s.id DESC, l.id ASC`;

    const data = await client.query(sql, params);

    const wb = new ExcelJS.Workbook();
    await wb.xlsx.readFile(TEMPLATE_PATH);
    const ws = wb.worksheets[0];

    const headersWanted = [
      'Skait. veids','Līg. Nr.','Klients','Klienta kods','NĪO adrese',
      'Periods no','Periods līdz','Skait. eks. Nr',
      'Nāk. verifikācijas datums','Pēdējā rādījuma datums',
      'Pēdējais rādījums','Daudzums iev.','Rādījums',
      'Stadija','Piezīmes','Daudzuma tips'
    ];

    const headerInfo = findHeaderMap(ws, headersWanted);
    if (!headerInfo) return res.status(500).send('Template headers not found.');

    const headerRow = headerInfo.headerRow;
    const map = headerInfo.map;
    const startRow = headerRow + 1;

    const maxClear = Math.max(ws.rowCount, startRow + 1500);
    for (let i = startRow; i <= maxClear; i++) {
      const row = ws.getRow(i);
      for (const h of headersWanted) {
        const col = map.get(h);
        if (col) row.getCell(col).value = null;
      }
      row.commit();
    }

    let rowIdx = startRow;
    for (const x of data.rows) {
      const row = ws.getRow(rowIdx);

      const set = (h, v) => {
        const col = map.get(h);
        if (!col) return;
        row.getCell(col).value = v;
      };

      set('Skait. veids', x.meter_type || null);
      set('Līg. Nr.', x.contract_nr || null);
      set('Klients', x.client_name || null);
      set('Klienta kods', x.subscriber_code || null);
      set('NĪO adrese', x.address || null);

      set('Periods no', toExcelDate(x.period_from));
      set('Periods līdz', toExcelDate(x.period_to));
      set('Skait. eks. Nr', x.meter_no || null);

      set('Nāk. verifikācijas datums', toExcelDate(x.next_verif_date));
      set('Pēdējā rādījuma datums', toExcelDate(x.last_reading_date));

      set('Pēdējais rādījums', x.previous_reading == null ? null : Number(x.previous_reading));
      set('Daudzums iev.', x.consumption == null ? null : Number(x.consumption));
      set('Rādījums', x.reading == null ? null : Number(x.reading));

      set('Stadija', x.stage || 'Sagatave');
      set('Piezīmes', x.notes || null);
      set('Daudzuma tips', x.qty_type || null);

      row.commit();
      rowIdx++;
    }

    const fname = month && /^\d{4}-\d{2}$/.test(month) ? `export_${month}.xlsx` : `export.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);

    const buf = await wb.xlsx.writeBuffer();
    res.end(Buffer.from(buf));
  } catch (e) {
    console.error('export.xlsx error', e);
    if (!res.headersSent) res.status(500);
    res.end('Export XLSX failed');
  } finally {
    client.release();
  }
});


function guessBrowser(ua) {
  const s = String(ua || '');
  if (!s) return '';
  if (s.includes('Edg/')) return 'Edge';
  if (s.includes('OPR/') || s.includes('Opera')) return 'Opera';
  if (s.includes('Chrome/')) return 'Chrome';
  if (s.includes('Firefox/')) return 'Firefox';
  if (s.includes('Safari/') && !s.includes('Chrome/')) return 'Safari';
  return 'Other';
}
function guessOS(ua) {
  const s = String(ua || '');
  if (!s) return '';
  if (s.includes('Windows')) return 'Windows';
  if (s.includes('Android')) return 'Android';
  if (s.includes('iPhone') || s.includes('iPad') || s.includes('iOS')) return 'iOS';
  if (s.includes('Mac OS X') || s.includes('Macintosh')) return 'macOS';
  if (s.includes('Linux')) return 'Linux';
  return 'Other';
}

app.get('/admin/tech.csv', requireBasicAuth, async (req, res) => {
  const month = String(req?.query?.month || '').trim(); // YYYY-MM
  if (!/^[0-9]{4}-[0-9]{2}$/.test(month)) return res.status(400).send('Invalid month');

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="tech_${month}.csv"`);

  res.write(toCSVRow([
    'submission_id',
    'client_submission_id',
    'submitted_at_riga',
    'subscriber_code',
    'contract_nr',
    'address',
    'ip',
    'browser_guess',
    'os_guess',
    'user_agent',
    'source_origin',
    'meta_origin',
    'meta_referer',
    'accept_language',
    'sec_ch_ua',
    'sec_ch_ua_platform',
    'sec_ch_ua_mobile',
    'x_forwarded_for',
    'client_meta_json'
  ]));

  const client = await pool.connect();
  try {
    const q = await client.query(`
      SELECT
        id,
        client_submission_id,
        to_char(submitted_at AT TIME ZONE $1, 'YYYY-MM-DD HH24:MI:SS') AS submitted_at_riga,
        subscriber_code,
        contract_nr,
        address,
        ip,
        user_agent,
        source_origin,
        client_meta
      FROM submissions
      WHERE to_char(date_trunc('month', submitted_at AT TIME ZONE $1), 'YYYY-MM') = $2
      ORDER BY submitted_at DESC, id DESC
    `, [TZ, month]);

    for (const r of q.rows) {
      const meta = r.client_meta || {};
      const ua = r.user_agent || '';
      res.write(toCSVRow([
        r.id,
        r.client_submission_id,
        r.submitted_at_riga,
        r.subscriber_code || '',
        r.contract_nr || '',
        r.address || '',
        r.ip || '',
        guessBrowser(ua),
        guessOS(ua),
        ua,
        r.source_origin || '',
        meta.origin || '',
        meta.referer || '',
        meta.accept_language || '',
        meta.sec_ch_ua || '',
        meta.sec_ch_ua_platform || '',
        meta.sec_ch_ua_mobile || '',
        meta.x_forwarded_for || '',
        JSON.stringify(meta)
      ]));
    }
    res.end();
  } catch (e) {
    console.error('tech export error', e);
    if (!res.headersSent) res.status(500);
    res.end('Tech export failed');
  } finally {
    client.release();
  }
});


app.get('/admin/invite_incomplete.csv', requireBasicAuth, async (req, res) => {
  const month = String(req?.query?.month || '').trim(); // YYYY-MM
  if (!/^[0-9]{4}-[0-9]{2}$/.test(month)) return res.status(400).send('Invalid month');

  const baseUrl = (PUBLIC_ORIGIN || getBaseUrl(req)).replace(/\/+$/, '');

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="invite_incomplete_${month}.csv"`);
  res.write(toCSVRow(['subscriber_code','email','invite_link','missing_meters_count','missing_meters_list']));

  const client = await pool.connect();
  try {
    // latest billing batch
    const b = await client.query(`SELECT id FROM billing_import_batches ORDER BY uploaded_at DESC LIMIT 1`);
    const batchId = b.rows[0]?.id;
    if (!batchId) return res.end();

    // invite tokens for requested month
    const tokensQ = await client.query(`
      SELECT subscriber_code, token_plain, token_enc
      FROM invite_tokens
      WHERE month=$1
      ORDER BY subscriber_code
    `, [month]);
    if (!tokensQ.rowCount) return res.end();

    const subs = tokensQ.rows.map(r => String(r.subscriber_code || '').trim()).filter(Boolean);
    if (!subs.length) return res.end();

    // token by subscriber
    const tokenBySub = new Map();
    for (const t of tokensQ.rows) {
      const sub = String(t.subscriber_code || '').trim();
      if (!sub) continue;
      let token = '';
      if (t.token_enc) token = decryptInviteToken(t.token_enc) || '';
      if (!token && t.token_plain) token = String(t.token_plain);
      if (token) tokenBySub.set(sub, token);
    }

    // snapshot meters for these subscribers (latest batch)
    const snapQ = await client.query(`
      SELECT subscriber_code, contract_nr, meter_serial
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code = ANY($2::text[])
    `, [batchId, subs]);

    // contractsBySub + meters rows
    const contractsBySub = new Map();
    const metersRows = [];
    for (const r of snapQ.rows) {
      const sub = String(r.subscriber_code || '').trim();
      const c = String(r.contract_nr || '').trim();
      const m = String(r.meter_serial || '').trim();
      if (!sub || !c || !m) continue;
      metersRows.push([sub, c, m]);
      if (!contractsBySub.has(sub)) contractsBySub.set(sub, new Set());
      contractsBySub.get(sub).add(c);
    }
    if (!metersRows.length) return res.end();

    // Build missing list using meter_submissions (join by contract_nr + meter_no)
    const valuesSql = metersRows.map((_, i) => `($${i*3+1}, $${i*3+2}, $${i*3+3})`).join(',');
    const params = metersRows.flat();

    const missingQ = await client.query(`
      WITH all_meters(subscriber_code, contract_nr, meter_serial) AS (
        VALUES ${valuesSql}
      ),
      submitted AS (
        SELECT contract_nr, meter_no
        FROM meter_submissions
        WHERE month = $${params.length + 1}
      ),
      missing AS (
        SELECT a.subscriber_code, a.contract_nr, a.meter_serial
        FROM all_meters a
        LEFT JOIN submitted s
          ON s.contract_nr = a.contract_nr
         AND s.meter_no = a.meter_serial
        WHERE s.meter_no IS NULL
      )
      SELECT
        subscriber_code,
        COUNT(*) AS missing_count,
        STRING_AGG(contract_nr || ':' || meter_serial, ' | ' ORDER BY contract_nr, meter_serial) AS missing_list
      FROM missing
      GROUP BY subscriber_code
      HAVING COUNT(*) > 0
      ORDER BY missing_count DESC, subscriber_code
    `, [...params, month]);

    // contract email map
    const emailQ = await client.query(`SELECT contract_nr, email FROM contract_email_map`);
    const emailByContract = new Map();
    for (const r of emailQ.rows) {
      const c = String(r.contract_nr || '').trim();
      const em = String(r.email || '').trim();
      if (c) emailByContract.set(c, em);
    }

    for (const r of missingQ.rows) {
      const sub = String(r.subscriber_code || '').trim();
      if (!sub) continue;

      const token = tokenBySub.get(sub) || '';
      if (!token) continue;

      const link = `${baseUrl}/i/${token}`;
      const missingCount = String(r.missing_count || '0');
      const missingList = String(r.missing_list || '');

      // collect emails from all subscriber contracts
      const emailSet = new Set();
      const contracts = contractsBySub.get(sub) ? Array.from(contractsBySub.get(sub)) : [];
      for (const c of contracts) {
        const raw = String(emailByContract.get(c) || '').trim();
        for (const e of extractEmails(raw)) emailSet.add(e);
      }

      if (emailSet.size === 0) {
        res.write(toCSVRow([sub, '', link, missingCount, missingList]));
        continue;
      }
      for (const e of emailSet) {
        res.write(toCSVRow([sub, e, link, missingCount, missingList]));
      }
    }

    res.end();
  } catch (e) {
    console.error('invite_incomplete export error', e);
    if (!res.headersSent) res.status(500);
    res.end('Invite incomplete export failed');
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
        <a class="btn" href="/admin/invites/export.csv">Lejupielādēt CSV (subscriber_code,email,link)</a>
      </div>

      <div class="muted" style="margin-top:10px"><a href="/admin">← atpakaļ</a></div>
    `);

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(html);
  } catch (e) {
    console.error('admin invites page error', e);
    res.status(500).send('Internal Server Error');
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

    for (const r of subs.rows) {
      const subscriber = String(r.subscriber_code || '').trim();
      if (!subscriber) continue;

      const token = newToken();
      const tokenHash = sha256Hex(token);
      const tokenEnc = encryptInviteToken(token);

      await client.query(`
        INSERT INTO invite_tokens (month, subscriber_code, token_hash, token_plain, token_enc, expires_at)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (month, subscriber_code)
        DO UPDATE SET
          token_hash = EXCLUDED.token_hash,
          token_plain = EXCLUDED.token_plain,
          token_enc = EXCLUDED.token_enc,
          expires_at = EXCLUDED.expires_at
      `, [month, subscriber, tokenHash, tokenEnc ? null : token, tokenEnc, expiresAt]);

      created++;
    }

    await client.query('COMMIT');

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(pageShell('Invites OK', `
      <h1>OK — uzaicinājumi sagatavoti</h1>
      <div class="muted">Mēnesis: <b>${month}</b></div>
      <div class="muted">Rindas: <b>${created}</b></div>
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
  res.write(toCSVRow(['subscriber_code','email','link']));

  const client = await pool.connect();
  try {
    const batchId = await getLatestBillingBatchId(client);

    const tokensQ = await client.query(`
      SELECT subscriber_code, token_plain, token_enc
      FROM invite_tokens
      WHERE month=$1
      ORDER BY subscriber_code
    `, [month]);

    const subs = tokensQ.rows.map(r => String(r.subscriber_code || '').trim()).filter(Boolean);
    let contractRows = [];
    if (batchId && subs.length) {
      const q = await client.query(`
        SELECT subscriber_code, contract_nr
        FROM billing_meters_snapshot
        WHERE batch_id=$1 AND subscriber_code = ANY($2::text[])
      `, [batchId, subs]);
      contractRows = q.rows;
    }

    const contractsBySub = new Map();
    for (const r of contractRows) {
      const sub = String(r.subscriber_code || '').trim();
      const c = String(r.contract_nr || '').trim();
      if (!sub || !c) continue;
      if (!contractsBySub.has(sub)) contractsBySub.set(sub, new Set());
      contractsBySub.get(sub).add(c);
    }

    const allContracts = Array.from(new Set(contractRows.map(r => String(r.contract_nr || '').trim()).filter(Boolean)));
    const emailByContract = new Map();
    if (allContracts.length) {
      const e = await client.query(`
        SELECT contract_nr, email
        FROM contract_email_map
        WHERE contract_nr = ANY($1::text[])
      `, [allContracts]);
      for (const r of e.rows) {
        const c = String(r.contract_nr || '').trim();
        const em = String(r.email || '').trim();
        if (c) emailByContract.set(c, em);
      }
    }

    for (const t of tokensQ.rows) {
      const sub = String(t.subscriber_code || '').trim();
      if (!sub) continue;

      let token = '';
      if (t.token_enc) token = decryptInviteToken(t.token_enc) || '';
      if (!token && t.token_plain) token = String(t.token_plain);
      if (!token) continue;

      let bestEmail = '';
      const contracts = contractsBySub.get(sub) ? Array.from(contractsBySub.get(sub)) : [];
      if (contracts.length) {
        const counts = new Map();
        for (const c of contracts) {
          const em = String(emailByContract.get(c) || '').trim();
          if (!isValidEmail(em)) continue;
          counts.set(em, (counts.get(em) || 0) + 1);
        }
        let bestCnt = -1;
        for (const [em, cnt] of counts.entries()) {
          if (cnt > bestCnt) { bestCnt = cnt; bestEmail = em; }
        }
      }

      const link = `${baseUrl}/i/${token}`;
      res.write(toCSVRow([sub, bestEmail, link]));
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


async function enforceTechRetention() {
  const client = await pool.connect();
  try {
    const r = await client.query(`
      UPDATE submissions
      SET ip = NULL,
          user_agent = NULL,
          source_origin = NULL,
          client_meta = NULL
      WHERE submitted_at < now() - interval '3 months'
        AND (ip IS NOT NULL OR user_agent IS NOT NULL OR source_origin IS NOT NULL OR client_meta IS NOT NULL)
    `);
    if (r.rowCount) console.log('[retention] anonymized submissions:', r.rowCount);
  } catch (e) {
    console.error('[retention] error', e);
  } finally {
    client.release();
  }
}


/* ===================== start ===================== */
(async () => {
  try {
    await ensureSchema();
    enforceTechRetention();
    setInterval(enforceTechRetention, 24 * 60 * 60 * 1000);
    await startPgListener();
    loadAddressesIfNeeded();
    app.listen(PORT, () => {
      console.log(`server listening on :${PORT} (enforceWindow=${ENFORCE_WINDOW}, tz=${TZ})`);
    });
  } catch (e) {
    console.error('FATAL: failed to start', e);
    process.exit(1);
  }
})();
