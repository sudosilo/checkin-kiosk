// RED PHONE: Check-In Kiosk API. Vercel serverless function, Upstash Redis over REST, no packages.
// RED PHONE: Nothing is ever thrown away. Reservations change status, they are never deleted.
// RED PHONE: Keys use the ck: prefix so this can share a Redis database with other apps safely.

// CLAUDE: two ways to reach Redis. Upstash REST if those variables are set, otherwise a plain
// CLAUDE: Redis address in REDIS_URL (Railway Redis public address), spoken over a raw socket.
const net = require('net');
const tls = require('tls');
const RURL = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const RTOK = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const TCPURL = process.env.REDIS_URL || process.env.REDIS_PUBLIC_URL || '';
const USE_REST = !!(RURL && RTOK);
const P = 'ck:';
const DEF = {
  name: '', pin: '1234', tables: 4, perParty: 12, perRes: 8, window: 60, full: false, fullBump: 15,
  bigParty: 6, bigBump: 10, grace: 5, minWait: 5, openHour: 6, closeHour: 22, halfBelow: 8, fullAt: 20
};
const LIMITS = {
  tables: [1, 50], perParty: [0, 60], perRes: [0, 60], window: [0, 240], fullBump: [0, 90], bigParty: [2, 20],
  bigBump: [0, 60], grace: [0, 30], minWait: [0, 60], openHour: [0, 23], closeHour: [1, 24], halfBelow: [0, 500], fullAt: [1, 500]
};

// CLAUDE: raw Redis protocol client. Opens one socket per request, sends every command, reads every reply.
function encode(args) {
  const parts = [Buffer.from('*' + args.length + '\r\n')];
  for (const a of args) {
    const b = Buffer.from(String(a));
    parts.push(Buffer.from('$' + b.length + '\r\n'), b, Buffer.from('\r\n'));
  }
  return Buffer.concat(parts);
}
function parseOne(buf, i) {
  if (i >= buf.length) return null;
  const nl = buf.indexOf('\r\n', i);
  if (nl < 0) return null;
  const t = String.fromCharCode(buf[i]), line = buf.toString('utf8', i + 1, nl), next = nl + 2;
  if (t === '+') return { v: line, i: next };
  if (t === '-') return { v: new Error(line), i: next };
  if (t === ':') return { v: Number(line), i: next };
  if (t === '$') {
    const n = Number(line);
    if (n < 0) return { v: null, i: next };
    if (buf.length < next + n + 2) return null;
    return { v: buf.toString('utf8', next, next + n), i: next + n + 2 };
  }
  if (t === '*') {
    const n = Number(line);
    if (n < 0) return { v: null, i: next };
    const out = []; let p = next;
    for (let k = 0; k < n; k++) { const r = parseOne(buf, p); if (!r) return null; out.push(r.v); p = r.i; }
    return { v: out, i: p };
  }
  throw new Error('Unreadable reply from Redis');
}
function tcpRun(cmds) {
  return new Promise((resolve, reject) => {
    let u;
    try { u = new URL(TCPURL); } catch (e) { return reject(new Error('REDIS_URL is not a valid address.')); }
    const pre = [];
    if (u.password) pre.push(u.username && u.username !== 'default' ? ['AUTH', decodeURIComponent(u.username), decodeURIComponent(u.password)] : ['AUTH', decodeURIComponent(u.password)]);
    const db = u.pathname && u.pathname.length > 1 ? u.pathname.slice(1) : '';
    if (db) pre.push(['SELECT', db]);
    const all = pre.concat(cmds), want = all.length;
    const opts = { host: u.hostname, port: Number(u.port) || 6379 };
    const sock = u.protocol === 'rediss:' ? tls.connect(Object.assign({ servername: u.hostname }, opts)) : net.connect(opts);
    let buf = Buffer.alloc(0), done = false;
    const finish = (err, val) => { if (done) return; done = true; sock.destroy(); err ? reject(err) : resolve(val); };
    sock.setTimeout(10000, () => finish(new Error('Redis did not answer in time.')));
    sock.on('error', e => finish(new Error('Cannot reach Redis: ' + e.message)));
    sock.on(u.protocol === 'rediss:' ? 'secureConnect' : 'connect', () => sock.write(Buffer.concat(all.map(encode))));
    sock.on('data', d => {
      buf = Buffer.concat([buf, d]);
      const out = []; let p = 0;
      try {
        while (out.length < want) { const r = parseOne(buf, p); if (!r) return; out.push(r.v); p = r.i; }
      } catch (e) { return finish(e); }
      const bad = out.slice(0, pre.length).find(x => x instanceof Error);
      if (bad) return finish(new Error('Redis rejected the login: ' + bad.message));
      finish(null, out.slice(pre.length));
    });
  });
}

// RED PHONE: Redis helpers
async function cmd(...c) {
  if (!USE_REST) { const [r] = await tcpRun([c]); if (r instanceof Error) throw r; return r; }
  const r = await fetch(RURL, { method: 'POST', headers: { Authorization: 'Bearer ' + RTOK }, body: JSON.stringify(c.map(String)) });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
async function pipe(cmds) {
  if (!cmds.length) return [];
  if (!USE_REST) return (await tcpRun(cmds)).map(x => { if (x instanceof Error) throw x; return x; });
  const r = await fetch(RURL + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + RTOK }, body: JSON.stringify(cmds.map(c => c.map(String))) });
  const j = await r.json();
  if (!Array.isArray(j)) throw new Error(j.error || 'Redis pipeline failed');
  return j.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}
async function getSettings() {
  const raw = await cmd('GET', P + 'settings');
  return Object.assign({}, DEF, raw ? JSON.parse(raw) : {});
}
async function range(kind, from, to) {
  const ids = await cmd('ZRANGEBYSCORE', P + kind + 'z', from, to);
  if (!ids || !ids.length) return [];
  const vals = await cmd('HMGET', P + kind, ...ids);
  return vals.filter(Boolean).map(v => JSON.parse(v));
}
async function getOne(kind, id) {
  const v = await cmd('HGET', P + kind, id);
  return v ? JSON.parse(v) : null;
}
function put(kind, obj, score) {
  return pipe([['HSET', P + kind, obj.id, JSON.stringify(obj)], ['ZADD', P + kind + 'z', score, obj.id]]);
}
const uid = p => p + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
const num = v => (Number.isFinite(Number(v)) ? Number(v) : 0);
const clean = (v, n) => String(v == null ? '' : v).trim().slice(0, n);
const clamp = (v, a, b) => Math.min(b, Math.max(a, Math.round(num(v))));
const PREFS = ['any', 'booth', 'table', 'counter'];

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!USE_REST && !TCPURL) return res.status(500).json({ error: 'The database is not connected. Add the Redis environment variables in Vercel and redeploy.' });
  const a = (req.query && req.query.a) || '';
  const b = req.body && typeof req.body === 'object' ? req.body : {};
  try {
    const s = await getSettings();
    const staff = String(req.headers['x-pin'] || '') === String(s.pin);

    // RED PHONE: open to the kiosk
    if (a === 'state') {
      const from = num(req.query.from), to = num(req.query.to);
      const [visits, resv] = await Promise.all([range('v', from, to), range('r', from, to)]);
      const pub = Object.assign({}, s); delete pub.pin;
      return res.json({ settings: pub, visits, res: resv });
    }
    if (a === 'login') return res.json({ ok: String(b.pin || '') === String(s.pin) });

    if (a === 'checkin') {
      const v = b.visit || {};
      const day = clean(b.dayKey, 10) || new Date().toISOString().slice(0, 10);
      const visit = {
        id: uid('v'), name: clean(v.name, 30), size: clamp(v.size, 1, 20), pref: PREFS.includes(v.pref) ? v.pref : 'any',
        phone: clean(v.phone, 14), type: v.type === 'res' ? 'res' : 'walkin', est: clamp(v.est, 0, 600),
        createdAt: Date.now(), status: 'waiting'
      };
      if (visit.type === 'res') {
        const r = await getOne('r', clean(v.resId, 40));
        if (!r) return res.status(404).json({ error: 'That reservation was not found.' });
        if (r.status !== 'booked') return res.status(409).json({ error: 'That reservation is already checked in.' });
        visit.resId = r.id; visit.name = r.name; visit.size = r.size; visit.resAt = r.at;
        r.status = 'arrived'; r.visitId = visit.id; r.updatedAt = Date.now();
        await put('r', r, r.at);
      }
      if (!visit.name) return res.status(400).json({ error: 'A name is required.' });
      const [ticket] = await pipe([['INCR', P + 'tix:' + day], ['EXPIRE', P + 'tix:' + day, 172800]]);
      visit.ticket = ticket;
      await put('v', visit, visit.createdAt);
      return res.json({ visit });
    }

    // RED PHONE: everything below needs the staff PIN
    if (!staff) return res.status(401).json({ error: 'Staff PIN required.' });

    if (a === 'visit-status') {
      const v = await getOne('v', clean(b.id, 40));
      if (!v) return res.status(404).json({ error: 'Visit not found.' });
      const st = ['seated', 'left', 'waiting'].includes(b.status) ? b.status : null;
      if (!st) return res.status(400).json({ error: 'Unknown status.' });
      v.status = st;
      if (st === 'seated') v.seatedAt = Date.now();
      if (st === 'left') v.endedAt = Date.now();
      await put('v', v, v.createdAt);
      if (v.resId && st === 'seated') {
        const r = await getOne('r', v.resId);
        if (r) { r.status = 'seated'; r.updatedAt = Date.now(); await put('r', r, r.at); }
      }
      return res.json({ visit: v });
    }
    if (a === 'res-add') {
      const at = num(b.at);
      const r = {
        id: uid('r'), name: clean(b.name, 30), size: clamp(b.size, 1, 20), at, phone: clean(b.phone, 14),
        notes: clean(b.notes, 80), status: 'booked', createdAt: Date.now()
      };
      if (!r.name || !at) return res.status(400).json({ error: 'A name, date, and time are required.' });
      await put('r', r, at);
      return res.json({ res: r });
    }
    if (a === 'res-status') {
      const r = await getOne('r', clean(b.id, 40));
      if (!r) return res.status(404).json({ error: 'Reservation not found.' });
      if (!['booked', 'noshow', 'cancelled'].includes(b.status)) return res.status(400).json({ error: 'Unknown status.' });
      r.status = b.status; r.updatedAt = Date.now();
      await put('r', r, r.at);
      return res.json({ res: r });
    }
    if (a === 'settings') {
      const inc = b.settings || {}, next = Object.assign({}, s);
      for (const k in LIMITS) if (k in inc) next[k] = clamp(inc[k], LIMITS[k][0], LIMITS[k][1]);
      if ('full' in inc) next.full = !!inc.full;
      if ('name' in inc) next.name = clean(inc.name, 30);
      if ('pin' in inc) {
        if (!/^\d{4,6}$/.test(String(inc.pin))) return res.status(400).json({ error: 'PIN must be 4 to 6 digits.' });
        next.pin = String(inc.pin);
      }
      if (next.closeHour <= next.openHour) return res.status(400).json({ error: 'Closing hour must be after opening hour.' });
      await cmd('SET', P + 'settings', JSON.stringify(next));
      const pub = Object.assign({}, next); delete pub.pin;
      return res.json({ settings: pub });
    }
    if (a === 'history') {
      const to = num(req.query.to) || Date.now();
      const from = Math.max(num(req.query.from), to - 400 * 86400000);
      const [visits, resv] = await Promise.all([range('v', from, to), range('r', from, to)]);
      return res.json({ visits, res: resv });
    }
    if (a === 'seed') {
      const list = Array.isArray(b.visits) ? b.visits.slice(0, 1000) : [];
      if (!list.length) return res.json({ added: 0 });
      const h = ['HSET', P + 'v'], z = ['ZADD', P + 'vz'], d = ['SADD', P + 'demo'];
      list.forEach((v, i) => {
        const createdAt = num(v.createdAt);
        if (!createdAt) return;
        const o = {
          id: 'd' + createdAt.toString(36) + i + Math.random().toString(36).slice(2, 5), name: clean(v.name, 30) || 'Guest',
          size: clamp(v.size, 1, 20), pref: 'any', phone: '', type: v.type === 'res' ? 'res' : 'walkin', est: clamp(v.est, 0, 600),
          createdAt, status: v.status === 'left' ? 'left' : 'seated', ticket: 0, demo: true
        };
        if (v.seatedAt) o.seatedAt = num(v.seatedAt);
        h.push(o.id, JSON.stringify(o)); z.push(createdAt, o.id); d.push(o.id);
      });
      if (h.length > 2) await pipe([h, z, d]);
      return res.json({ added: (h.length - 2) / 2 });
    }
    if (a === 'clear-demo') {
      const ids = (await cmd('SMEMBERS', P + 'demo')) || [];
      for (let i = 0; i < ids.length; i += 800) {
        const part = ids.slice(i, i + 800);
        await pipe([['HDEL', P + 'v', ...part], ['ZREM', P + 'vz', ...part]]);
      }
      await cmd('DEL', P + 'demo');
      return res.json({ removed: ids.length });
    }
    return res.status(400).json({ error: 'Unknown action.' });
  } catch (e) {
    return res.status(500).json({ error: e.message || 'Server error' });
  }
};
