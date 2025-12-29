'use strict';

const fs = require('fs');
const path = require('path');

const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const basicAuth = require('basic-auth');

const multer = require('multer');
const XLSX = require('xlsx');
const ExcelJS = require('exceljs');

const { Pool } = require('pg');
const { DateTime } = require('luxon');
const { v4: uuidv4 } = require('uuid');

const app = express();

/* ===================== ENV ===================== */
const PORT = process.env.PORT || 8080;
const DATABASE_URL = process.env.DATABASE_URL;

const TZ = 'Europe/Riga';
const ENFORCE_WINDOW = String(process.env.ENFORCE_WINDOW || '0') === '1';

const PUBLIC_ORIGIN = (process.env.PUBLIC_ORIGIN || '').trim(); // https://radijumi.jurmalasudens.lv

const ADMIN_KEY = (process.env.ADMIN_KEY || '').trim();         // optional (only for /api/export.csv)
const ADMIN_USER = process.env.ADMIN_USER || '';               // required for /admin/*
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

/* ===================== middleware ===================== */
app.set('trust proxy', 1);

app.use(helmet({ contentSecurityPolicy: false }));
app.use(express.json({ limit: '512kb' }));
app.use(express.urlencoded({ extended: false, limit: '512kb' }));

/* Block direct access to any addresses CSV by name */
app.get('/adreses.csv', (req, res) => res.status(404).end());

/* Static frontend from ./public */
app.use(express.static(path.join(__dirname, 'public'), { etag: true, maxAge: '1h' }));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

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

function getClientIp(req) {
  return req.ip || null;
}

function getOriginOrReferer(req) {
  return {
    origin: (req.get('origin') || '').trim(),
    referer: (req.get('referer') || '').trim(),
  };
}

/* Strict submit origin check */
function enforceSameOrigin(req, res) {
  if (!PUBLIC_ORIGIN) {
    return res.status(500).json({ ok: false, error: 'Server misconfigured: PUBLIC_ORIGIN missing' });
  }

  const { origin, referer } = getOriginOrReferer(req);

  if (origin) {
    if (origin !== PUBLIC_ORIGIN) return res.status(403).json({ ok: false, error: 'Forbidden origin' });
    return null;
  }
  if (referer) {
    if (!referer.startsWith(PUBLIC_ORIGIN + '/')) return res.status(403).json({ ok: false, error: 'Forbidden referer' });
    return null;
  }
  return res.status(403).json({ ok: false, error: 'Missing origin/referer' });
}

/* Soft origin check for GET endpoints (do not block if missing) */
function enforceSameOriginSoft(req, res) {
  if (!PUBLIC_ORIGIN) return null;
  const { origin, referer } = getOriginOrReferer(req);

  if (origin && origin !== PUBLIC_ORIGIN) return res.status(403).json({ ok: false, error: 'Forbidden origin' });
  if (referer && !referer.startsWith(PUBLIC_ORIGIN + '/')) return res.status(403).json({ ok: false, error: 'Forbidden referer' });
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

/* Bearer auth middleware */
function requireAdminBearer(req, res, next) {
  if (!ADMIN_KEY) return res.status(500).send('Server misconfigured: ADMIN_KEY missing');
  const auth = req.get('authorization') || '';
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (!m) return res.status(401).send('Unauthorized');
  if (m[1].trim() !== ADMIN_KEY) return res.status(403).send('Forbidden');
  next();
}

/* Subscriber code: only 8 digits */
function pickSubscriberCode(body) {
  const v = body?.subscriber_code ?? body?.abonenta_numurs ?? body?.subscriberCode ?? body?.subscriber;
  const digits = String(v ?? '').trim().replace(/\D+/g, '');
  if (/^\d{8}$/.test(digits)) return digits;
  return null;
}

/* Contract number: free-form (various structures) */
function pickContractNr(body) {
  const v = body?.contract_nr ?? body?.contractNr ?? body?.contract;
  const s = String(v ?? '').trim();
  if (!s) return null;
  if (s.length > 80) return null;
  return s;
}

/* Meter no digits only */
function normalizeMeterNo(v) {
  const s = String(v ?? '').trim();
  if (!/^\d+$/.test(s)) return null;
  return s;
}

/* Reading: allow 123 / 123.4 / 123.45 / 123,45 (max 2 decimals) */
function parseReading(value) {
  const s = String(value ?? '').trim().replace(',', '.');
  if (!/^\d+(\.\d{1,2})?$/.test(s)) return null;
  const num = Number(s);
  if (!Number.isFinite(num) || num < 0) return null;
  return s;
}

/* Diacritics helper (Ausekļa -> ausekla) */
function stripDiacritics(s) {
  return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

/* ===================== Addresses loader from XLSX ===================== */
const ADDR_XLSX = path.join(__dirname, 'data', 'adresesJurmala.xlsx');
let addrCache = { loadedAt: 0, mtimeMs: 0, rows: [] }; // rows: { key, original }

function normalizeForSearch(s) {
  return stripDiacritics(String(s || '').trim().toLowerCase())
    .replace(/[^\p{L}\p{N}\s]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function loadAddressesIfNeeded() {
  if (!fs.existsSync(ADDR_XLSX)) {
    if (addrCache.loadedAt === 0) console.warn(`ADDR_XLSX missing on server: ${ADDR_XLSX}`);
    addrCache = { loadedAt: Date.now(), mtimeMs: 0, rows: [] };
    return;
  }

  const stat = fs.statSync(ADDR_XLSX);
  const mtime = stat.mtimeMs;

  if (addrCache.loadedAt && addrCache.mtimeMs === mtime && addrCache.rows.length) return;

  // Read XLSX, column "STD", take "street+nr" up to first comma
  const wb = XLSX.readFile(ADDR_XLSX);
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });

  const out = [];
  const seen = new Set();

  for (const r of rows) {
    const std = String(r.STD || '').trim();
    if (!std) continue;

    const cleaned = std.split(',')[0].trim();
    if (!cleaned) continue;
    if (seen.has(cleaned)) continue;

    seen.add(cleaned);
    out.push({ original: cleaned, key: normalizeForSearch(cleaned) });
  }

  addrCache = { loadedAt: Date.now(), mtimeMs: mtime, rows: out };
  console.log(`ADDR_XLSX loaded: ${out.length} addresses`);
}

/* Helpers for "12 bu" behavior, and prefix-only search */
function parseQuery(qRaw) {
  const q = normalizeForSearch(qRaw);
  const parts = q ? q.split(' ').filter(Boolean) : [];
  const nums = parts.filter(t => /^\d+$/.test(t));
  const words = parts.filter(t => /[a-zā-ž]/i.test(t));
  return { q, parts, nums, words };
}

// house number match: num not adjacent to other digits, optional one letter suffix (12a)
function hasHouseNumber(key, num) {
  const re = new RegExp(`(^|[^0-9])${num}[a-z]?([^0-9]|$)`, 'i');
  return re.test(key);
}

/* ===================== Billing snapshot (XLSX upload) ===================== */
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 } // 25MB
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

/* ===================== DB: months list ===================== */
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
    return r.rows.map(x => x.month);
  } finally {
    client.release();
  }
}

/* ===================== routes ===================== */

app.get('/health', async (req, res) => {
  try {
    const r = await pool.query('SELECT 1 AS ok');
    res.json({ ok: true, db: r.rows[0].ok === 1 });
  } catch (e) {
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

/* ✅ Addresses search (manual mode):
   - if query has BOTH number(s) and word(s): street MUST start with words (prefix), and house number must match number(s)
   - if only words: ONLY prefix (startsWith)
   - if only number: match house number
   - NO token-anywhere search */
app.get('/api/addresses', addressesLimiter, (req, res) => {
  const originError = enforceSameOriginSoft(req, res);
  if (originError) return;

  loadAddressesIfNeeded();

  const qRaw = String(req.query.q || '').trim();
  if (!qRaw) return res.json({ ok: true, items: [], results: [] });

  const { q, nums, words } = parseQuery(qRaw);
  const out = [];
  const limit = Math.min(parseInt(req.query.limit || '20', 10) || 20, 50);

  // Special mode: "12 bu" => street prefix "bu" + house number 12
  if (nums.length && words.length) {
    const prefix = words.join(' ');
    for (const r of addrCache.rows) {
      if (!r.key.startsWith(prefix)) continue;
      if (!nums.every(n => hasHouseNumber(r.key, n))) continue;

      out.push(r.original);
      if (out.length >= limit) break;
    }
    return res.json({ ok: true, items: out, results: out });
  }

  // Only words => prefix-only
  if (q && words.length && !nums.length) {
    for (const r of addrCache.rows) {
      if (r.key.startsWith(q)) {
        out.push(r.original);
        if (out.length >= limit) break;
      }
    }
    return res.json({ ok: true, items: out, results: out });
  }

  // Only number(s) => match by house number (use first)
  if (nums.length && !words.length) {
    const n0 = nums[0];
    for (const r of addrCache.rows) {
      if (hasHouseNumber(r.key, n0)) {
        out.push(r.original);
        if (out.length >= limit) break;
      }
    }
    return res.json({ ok: true, items: out, results: out });
  }

  return res.json({ ok: true, items: [], results: [] });
});

/* ===================== LOOKUP: subscriber + contract -> meters + last reading ===================== */
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

    const q = await client.query(`
      SELECT address_raw, meter_serial, last_reading, client_name
      FROM billing_meters_snapshot
      WHERE batch_id=$1 AND subscriber_code=$2 AND contract_nr=$3
      ORDER BY address_raw, meter_serial
    `, [batchId, subscriber, contract]);

    if (!q.rowCount) return res.json({ ok:true, found:false });

    const byAddr = new Map();
    for (const r of q.rows) {
      const addr = r.address_raw || '';
      if (!byAddr.has(addr)) byAddr.set(addr, []);
      byAddr.get(addr).push({
        meter_serial: r.meter_serial,
        last_reading: r.last_reading
      });
    }

    res.json({
      ok: true,
      found: true,
      batch_id: batchId,
      client_name: q.rows[0].client_name || null,
      addresses: Array.from(byAddr.entries()).map(([address, meters]) => ({ address, meters }))
    });
  } catch (e) {
    console.error('lookup error', e);
    res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    client.release();
  }
});

/* ===================== Submit: mode=lookup|manual ===================== */
app.post('/api/submit', submitLimiter, async (req, res) => {
  const originError = enforceSameOrigin(req, res);
  if (originError) return;

  if (!isWindowOpen()) {
    const info = getSubmissionWindow();
    return res.status(403).json({ ok: false, error: 'Submission window closed', window: info });
  }

  const hp = String(req.body.website || req.body.honeypot || '').trim();
  if (hp) return res.status(400).json({ ok: false, error: 'Rejected' });

  const mode = String(req.body.mode || 'manual').trim().toLowerCase(); // 'lookup' | 'manual'
  if (mode !== 'lookup' && mode !== 'manual') {
    return res.status(400).json({ ok:false, error:'Invalid mode' });
  }

  const subscriber_code = pickSubscriberCode(req.body);
  if (!subscriber_code) {
    return res.status(400).json({ ok: false, error: 'Invalid subscriber_code (must be 8 digits)' });
  }

  const rawLines = Array.isArray(req.body.lines) ? req.body.lines : [];
  if (!rawLines.length || rawLines.length > 400) {
    return res.status(400).json({ ok: false, error: 'Invalid lines' });
  }

  let client_submission_id = String(req.body.client_submission_id || req.body.clientSubmissionId || '').trim();
  if (client_submission_id) {
    if (!/^[0-9a-fA-F-]{36}$/.test(client_submission_id)) {
      return res.status(400).json({ ok: false, error: 'Invalid client_submission_id' });
    }
  } else {
    client_submission_id = uuidv4();
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
      const contract_nr = pickContractNr(req.body);
      if (!contract_nr) {
        await db.query('ROLLBACK');
        return res.status(400).json({ ok:false, error:'Invalid contract_nr' });
      }

      // Validate lines: meter_no + reading
      const cleanLines = [];
      for (const l of rawLines) {
        const meter_no = normalizeMeterNo(l.meter_no ?? l.skaititaja_numurs ?? l.skaititajaNr);
        if (!meter_no) {
          await db.query('ROLLBACK');
          return res.status(400).json({ ok:false, error:'Invalid meter_no (digits only)' });
        }
        const readingStr = parseReading(l.reading ?? l.radijums);
        if (readingStr == null) {
          await db.query('ROLLBACK');
          return res.status(400).json({ ok:false, error:'Invalid reading (max 2 decimals, >=0)' });
        }
        cleanLines.push({ meter_no, reading: readingStr });
      }

      const batchId = await getLatestBillingBatchId(db);
      if (!batchId) {
        await db.query('ROLLBACK');
        return res.status(503).json({ ok:false, error:'Billing data not uploaded (admin must upload XLSX)' });
      }

      const snap = await db.query(`
        SELECT
          meter_serial, address_raw, last_reading, last_reading_date, next_verif_date,
          period_from, period_to, meter_type, stage, notes, qty_type, client_name
        FROM billing_meters_snapshot
        WHERE batch_id=$1 AND subscriber_code=$2 AND contract_nr=$3
      `, [batchId, subscriber_code, contract_nr]);

      if (!snap.rowCount) {
        await db.query('ROLLBACK');
        return res.status(400).json({ ok:false, error:'Subscriber/contract not found' });
      }

      const snapByMeter = new Map();
      for (const r of snap.rows) snapByMeter.set(String(r.meter_serial), r);

      for (const x of cleanLines) {
        if (!snapByMeter.has(x.meter_no)) {
          await db.query('ROLLBACK');
          return res.status(400).json({ ok:false, error:'Meter mismatch' });
        }
      }

      const firstSnap = snap.rows[0];

      // Upsert submission (idempotent)
      const insertSubmissionSql = `
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
      `;

      const subRes = await db.query(insertSubmissionSql, [
        client_submission_id,
        subscriber_code,
        contract_nr,
        batchId,
        firstSnap.client_name || null,
        'MULTI',
        source_origin,
        ua,
        ip,
        JSON.stringify(clientMeta),
      ]);

      const submissionId = subRes.rows[0].id;

      // Replace lines for idempotency
      await db.query('DELETE FROM submission_lines WHERE submission_id = $1', [submissionId]);

      const insertLineSql = `
        INSERT INTO submission_lines (
          submission_id,
          meter_no,
          address,
          meter_type,
          period_from,
          period_to,
          next_verif_date,
          last_reading_date,
          previous_reading,
          reading,
          consumption,
          stage,
          notes,
          qty_type
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,
          $9::numeric, $10::numeric, $11::numeric,
          $12,$13,$14
        )
      `;

      for (const x of cleanLines) {
        const s = snapByMeter.get(x.meter_no);
        const prev = s.last_reading == null ? null : Number(s.last_reading);
        const cur = Number(String(x.reading));
        const cons = (prev == null) ? null : (cur - prev);

        await db.query(insertLineSql, [
          submissionId,
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

    // ===== manual mode =====
    // expects lines with {adrese, skaititaja_numurs, radijums}
    const cleanLines = [];
    for (const l of rawLines) {
      const address = String(l.adrese ?? l.address ?? '').trim();
      if (!address || address.length < 2 || address.length > 200) {
        await db.query('ROLLBACK');
        return res.status(400).json({ ok:false, error:'Invalid address' });
      }

      const meter_no = normalizeMeterNo(l.meter_no ?? l.skaititaja_numurs ?? l.skaititajaNr);
      if (!meter_no) {
        await db.query('ROLLBACK');
        return res.status(400).json({ ok:false, error:'Invalid meter_no (digits only)' });
      }

      const readingStr = parseReading(l.reading ?? l.radijums);
      if (readingStr == null) {
        await db.query('ROLLBACK');
        return res.status(400).json({ ok:false, error:'Invalid reading (max 2 decimals, >=0)' });
      }

      cleanLines.push({ address, meter_no, reading: readingStr });
    }

    // submission address: if multiple, store MULTI
    const addrSet = new Set(cleanLines.map(x => x.address));
    const submissionAddress = addrSet.size === 1 ? cleanLines[0].address : 'MULTI';

    const insertSubmissionSql = `
      INSERT INTO submissions (client_submission_id, subscriber_code, address, source_origin, user_agent, ip, client_meta)
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
      ON CONFLICT (client_submission_id)
      DO UPDATE SET
        subscriber_code = EXCLUDED.subscriber_code,
        address = EXCLUDED.address
      RETURNING id
    `;

    const subRes = await db.query(insertSubmissionSql, [
      client_submission_id,
      subscriber_code,
      submissionAddress,
      source_origin,
      ua,
      ip,
      JSON.stringify(clientMeta),
    ]);

    const submissionId = subRes.rows[0].id;

    await db.query('DELETE FROM submission_lines WHERE submission_id = $1', [submissionId]);

    // Note: requires submission_lines.address column (from migration)
    const insertLineSql = `
      INSERT INTO submission_lines (submission_id, meter_no, address, previous_reading, reading, consumption)
      VALUES ($1, $2, $3, $4::numeric, $5::numeric, $6::numeric)
    `;

    for (const l of cleanLines) {
      await db.query(insertLineSql, [submissionId, l.meter_no, l.address, null, l.reading, null]);
    }

    await db.query('COMMIT');
    return res.json({ ok:true, submission_id: submissionId, client_submission_id });
  } catch (err) {
    try { await db.query('ROLLBACK'); } catch (_) {}
    console.error('submit error', err);
    return res.status(500).json({ ok:false, error:'Internal error' });
  } finally {
    db.release();
  }
});

/* ===================== Admin UI ===================== */

app.get('/admin', requireBasicAuth, async (req, res) => {
  try {
    const months = await listAvailableMonths();
    const latest = await getLatestBillingBatchInfo();

    const optionsHtml = months.length
      ? months.map((m, i) => `<option value="${m}" ${i === 0 ? 'selected' : ''}>${m}</option>`).join('')
      : `<option value="" disabled selected>Nav datu</option>`;

    const latestHtml = latest
      ? `<div class="muted"><b>Billing XLSX:</b> pēdējais batch #${latest.id} (${latest.source_filename || 'file'}) — ${String(latest.uploaded_at)}</div>`
      : `<div class="muted"><b>Billing XLSX:</b> nav ielādēts (lookup nestrādās).</div>`;

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(`
<!doctype html>
<html lang="lv">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Admin</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }
    .card { max-width: 740px; border: 1px solid #ddd; border-radius: 12px; padding: 16px; }
    label { display:block; margin: 10px 0 6px; font-weight: 800; }
    select, input, button { width: 100%; padding: 10px; font-size: 16px; }
    button { margin-top: 12px; font-weight: 900; cursor: pointer; }
    .muted { color:#666; font-size: 13px; margin-top: 10px; }
    .hr { border-top: 1px solid #eee; margin: 16px 0; }
    .danger { margin-top: 18px; border-top: 1px solid #eee; padding-top: 14px; }
    .danger h3 { margin: 0 0 8px; color: #b00020; }
    .danger small { color:#666; display:block; margin-top: 6px; }
    .danger button { background:#b00020; color:#fff; border:none; border-radius:10px; }
    .ok { background:#1f5f86; color:#fff; border:none; border-radius:10px; }
    code { background:#f3f5f8; padding:2px 6px; border-radius:8px; }
  </style>
</head>
<body>
  <div class="card">
    <h2>Admin</h2>
    ${latestHtml}

    <div class="hr"></div>

    <h3>1) Ielādēt billing XLSX (jaunākais)</h3>
    <form method="POST" action="/admin/billing/upload" enctype="multipart/form-data">
      <label for="file">XLSX fails</label>
      <input id="file" name="file" type="file" accept=".xlsx" required />
      <button type="submit" class="ok">Ielādēt XLSX</button>
      <div class="muted">Pēc ielādes /api/lookup izmantos šo pēdējo batch automātiski.</div>
    </form>

    <div class="hr"></div>

    <h3>2) Eksports</h3>

    <form method="GET" action="/admin/export.xlsx">
      <label for="monthX">XLSX eksports pēc veidnes</label>
      <select id="monthX" name="month" ${months.length ? '' : 'disabled'}>
        ${optionsHtml}
      </select>
      <button type="submit" ${months.length ? '' : 'disabled'} class="ok">Lejupielādēt export.xlsx</button>
      <div class="muted">Izmanto veidni: <code>data/billing_template.xlsx</code></div>
    </form>

    <form method="GET" action="/admin/export.csv">
      <label for="month">CSV (debug)</label>
      <select id="month" name="month" ${months.length ? '' : 'disabled'}>
        ${optionsHtml}
      </select>
      <button type="submit" ${months.length ? '' : 'disabled'}>Eksportēt</button>
    </form>

    <div class="danger">
      <h3>Dzēst visus iesniegumus</h3>
      <div class="muted">Šī darbība neatgriezeniski izdzēsīs visus iesniegumus no DB.</div>
      <form method="POST" action="/admin/clear">
        <label for="confirm">Ieraksti <b>DELETE</b>, lai apstiprinātu</label>
        <input id="confirm" name="confirm" autocomplete="off" />
        <button type="submit">Dzēst visu</button>
        <small>Drošībai: bez “DELETE” ievades dzēšana nenotiks.</small>
      </form>
    </div>
  </div>
</body>
</html>
    `);
  } catch (e) {
    console.error('admin page error', e);
    res.status(500).send('Admin page error');
  }
});

/* ===== Admin: billing XLSX upload -> snapshot tables ===== */
app.post('/admin/billing/upload', requireBasicAuth, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).send('No file');
  const filename = req.file.originalname || 'billing.xlsx';

  const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true });

  // header row contains "Klienta kods" and "Līg. Nr."
  const headerRowIndex = rows.findIndex(r => Array.isArray(r) && r.includes('Klienta kods') && r.includes('Līg. Nr.'));
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
        stage,
        notes,
        qty_type
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
      ON CONFLICT ON CONSTRAINT uq_billing_row DO NOTHING
    `;

    for (const r of dataRows) {
      if (!Array.isArray(r) || r.length === 0) continue;

      const subscriber = String(r[idx.subscriber] ?? '').trim();
      const contract = String(r[idx.contract] ?? '').trim();
      const meter = String(r[idx.meter] ?? '').trim();
      if (!subscriber || !contract || !meter) continue;

      const lastRaw = r[idx.last_val];
      const lastNum = (lastRaw == null || lastRaw === '') ? null : Number(lastRaw);

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
        idx.stage >= 0 ? (String(r[idx.stage] ?? '').trim() || null) : null,
        idx.notes >= 0 ? (String(r[idx.notes] ?? '').trim() || null) : null,
        idx.qty_type >= 0 ? (String(r[idx.qty_type] ?? '').trim() || null) : null
      ]);
    }

    await client.query('COMMIT');

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.end(`
<!doctype html>
<html lang="lv">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>OK</title>
<style>body{font-family:system-ui;margin:24px}a{display:inline-block;margin-top:12px}</style>
</head>
<body>
  <h2>OK — XLSX ielādēts</h2>
  <div>Izveidots billing batch: <b>#${batchId}</b></div>
  <a href="/admin">Atpakaļ uz admin</a>
</body>
</html>
    `);
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('billing upload error', e);
    res.status(500).send('Upload failed');
  } finally {
    client.release();
  }
});

app.post('/admin/clear', requireBasicAuth, async (req, res) => {
  const confirm = String(req.body.confirm || '').trim();
  if (confirm !== 'DELETE') {
    res.status(400);
    return res.send('Nepareizs apstiprinājums. Ieraksti DELETE.');
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('TRUNCATE TABLE submission_lines, submissions RESTART IDENTITY CASCADE;');
    await client.query('COMMIT');

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.end(`
<!doctype html>
<html lang="lv">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>OK</title>
<style>body{font-family:system-ui;margin:24px}a{display:inline-block;margin-top:12px}</style>
</head>
<body>
  <h2>OK — visi iesniegumi dzēsti</h2>
  <div>DB tabulas ir iztīrītas (submissions + submission_lines).</div>
  <a href="/admin">Atpakaļ uz admin</a>
</body>
</html>
    `);
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('admin clear error', e);
    return res.status(500).send('Dzēšana neizdevās.');
  } finally {
    client.release();
  }
});

/* ===================== Export CSV (debug) ===================== */
async function exportCsv(res, req) {
  const month = String(req?.query?.month || '').trim(); // YYYY-MM

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="export.csv"');

  res.write(toCSVRow([
    'submission_id',
    'client_submission_id',
    'subscriber_code',
    'contract_nr',
    'address',
    'submitted_at_utc',
    'meter_no',
    'previous_reading',
    'reading',
    'consumption'
  ]));

  const client = await pool.connect();
  try {
    let sql = `
      SELECT
        s.id AS submission_id,
        s.client_submission_id,
        s.subscriber_code,
        s.contract_nr,
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

app.get('/api/export.csv', requireAdminBearer, async (req, res) => {
  await exportCsv(res, req);
});

/* ===================== Export XLSX (template) ===================== */
const TEMPLATE_PATH = path.join(__dirname, 'data', 'billing_template.xlsx');

function toExcelDate(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  const s = String(v).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  return new Date(s + 'T00:00:00Z');
}

function findHeaderMap(ws, headersWanted) {
  // scan first 15 rows for header row
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

      if (headersWanted.includes(t)) {
        map.set(t, c);
        hit++;
      }
    }

    if (hit >= Math.min(5, headersWanted.length)) {
      return { headerRow: r, map };
    }
  }
  return null;
}

app.get('/admin/export.xlsx', requireBasicAuth, async (req, res) => {
  const month = String(req?.query?.month || '').trim(); // YYYY-MM

  if (!fs.existsSync(TEMPLATE_PATH)) {
    res.status(500);
    return res.send('Template missing: data/billing_template.xlsx');
  }

  const client = await pool.connect();
  try {
    let sql = `
      SELECT
        s.contract_nr,
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
      'Skait. veids', 'Līg. Nr.', 'Klients', 'Klienta kods', 'NĪO adrese',
      'Periods no', 'Periods līdz', 'Skait. eks. Nr',
      'Nāk. verifikācijas datums', 'Pēdējā rādījuma datums',
      'Pēdējais rādījums', 'Daudzums iev.', 'Rādījums',
      'Stadija', 'Piezīmes', 'Daudzuma tips'
    ];

    const headerInfo = findHeaderMap(ws, headersWanted);
    if (!headerInfo) {
      res.status(500);
      return res.send('Template headers not found (check billing_template.xlsx).');
    }

    const headerRow = headerInfo.headerRow;
    const map = headerInfo.map;
    const startRow = headerRow + 1;

    // Clear old data below header (soft clear only mapped columns)
    const maxClear = Math.max(ws.rowCount, startRow + 1500);
    for (let i = startRow; i <= maxClear; i++) {
      const row = ws.getRow(i);
      for (const h of headersWanted) {
        const col = map.get(h);
        if (col) row.getCell(col).value = null;
      }
      row.commit();
    }

    // Fill
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

/* ===================== start ===================== */
app.listen(PORT, () => {
  console.log(`server listening on :${PORT} (enforceWindow=${ENFORCE_WINDOW}, tz=${TZ})`);
});
