const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const DATA_DIR = path.join(__dirname, 'data');
const MAX_STORAGE = 20 * 1024 * 1024 * 1024; // 20GB
const MAX_AGE = 3 * 24 * 60 * 60 * 1000; // 3 days
const ROOMS = {};

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.mp4': 'video/mp4',
  '.webm': 'video/webm'
};

// --- Storage ---

function roomDir(room) {
  const d = path.join(DATA_DIR, room);
  fs.mkdirSync(d, { recursive: true });
  fs.mkdirSync(path.join(d, 'files'), { recursive: true });
  return d;
}

function saveMessage(room, msg) {
  const entry = { ts: Date.now(), ...msg };
  const file = path.join(roomDir(room), 'messages.jsonl');
  fs.appendFileSync(file, JSON.stringify(entry) + '\n');
}

function getHistory(room) {
  const file = path.join(roomDir(room), 'messages.jsonl');
  if (!fs.existsSync(file)) return [];
  try {
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean);
    return lines.map(l => JSON.parse(l));
  } catch { return []; }
}

function clearRoom(room) {
  const d = path.join(DATA_DIR, room);
  if (fs.existsSync(d)) fs.rmSync(d, { recursive: true });
}

function totalStorage() {
  if (!fs.existsSync(DATA_DIR)) return 0;
  let total = 0;
  function walk(d) {
    const entries = fs.readdirSync(d, { withFileTypes: true });
    for (const e of entries) {
      if (e.isDirectory()) walk(path.join(d, e.name));
      else total += fs.statSync(path.join(d, e.name)).size;
    }
  }
  try { walk(DATA_DIR); } catch {}
  return total;
}

// --- File chunk buffering ---

const chunks = {}; // { fileId: { room, fileName, fileType, fileSize, parts: [] } }

function bufferChunk(data) {
  if (data.length < 16) return;
  const header = data.slice(0, 16);
  const fileId = header.slice(0, 12).toString().trimEnd();
  const index = (header[12] << 24) | (header[13] << 16) | (header[14] << 8) | header[15];
  if (chunks[fileId]) {
    chunks[fileId].parts[index] = data.slice(16);
  }
  return fileId;
}

function saveChunkedFile(fileId) {
  const c = chunks[fileId];
  if (!c) return null;
  delete chunks[fileId];
  const total = c.parts.reduce((s, p) => s + (p ? p.length : 0), 0);
  if (total !== c.fileSize) return null;
  const buf = Buffer.alloc(total);
  let offset = 0;
  for (let i = 0; i < c.parts.length; i++) {
    if (c.parts[i]) {
      buf.set(c.parts[i], offset);
      offset += c.parts[i].length;
    }
  }
  const dir = path.join(roomDir(c.room), 'files');
  const fname = fileId + '_' + c.fileName;
  fs.writeFileSync(path.join(dir, fname), buf);
  fs.writeFileSync(path.join(dir, fileId + '.meta'), JSON.stringify({
    name: c.fileName, type: c.fileType, size: c.fileSize, ts: Date.now()
  }));
  return { name: c.fileName, type: c.fileType, size: c.fileSize, room: c.room };
}

// --- Cleanup ---

function cleanup() {
  if (!fs.existsSync(DATA_DIR)) return;
  const now = Date.now();
  const cutoff = now - MAX_AGE;
  const allFiles = [];

  function scan(d) {
    if (!fs.existsSync(d)) return;
    const entries = fs.readdirSync(d, { withFileTypes: true });
    for (const e of entries) {
      const full = path.join(d, e.name);
      if (e.isDirectory()) {
        scan(full);
      } else {
        try {
          const st = fs.statSync(full);
          allFiles.push({ path: full, mtime: st.mtimeMs, size: st.size });
        } catch {}
      }
    }
  }
  scan(DATA_DIR);

  const expired = allFiles.filter(f => f.mtime < cutoff);
  for (const f of expired) { try { fs.unlinkSync(f.path); } catch {} }

  let remaining = allFiles.filter(f => f.mtime >= cutoff);
  remaining.sort((a, b) => a.mtime - b.mtime);
  let used = remaining.reduce((s, f) => s + f.size, 0);
  for (const f of remaining) {
    if (used <= MAX_STORAGE) break;
    try { fs.unlinkSync(f.path); } catch {}
    used -= f.size;
  }

  // Remove empty directories
  function rmdirs(d) {
    if (!fs.existsSync(d) || !fs.statSync(d).isDirectory()) return;
    const entries = fs.readdirSync(d, { withFileTypes: true });
    for (const e of entries) {
      if (e.isDirectory()) rmdirs(path.join(d, e.name));
    }
    try { if (fs.readdirSync(d).length === 0) fs.rmdirSync(d); } catch {}
  }
  rmdirs(DATA_DIR);

  console.log('[cleanup] expired:', expired.length, 'storage:', (used / 1024 / 1024).toFixed(1) + 'MB');
}

setInterval(cleanup, 60 * 60 * 1000);
cleanup();

// --- HTTP Server ---

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  const url = u.pathname;

  // API: download file
  if (url.startsWith('/file/')) {
    const parts = url.split('/').filter(Boolean);
    if (parts.length >= 3) {
      const [room, fileId] = [parts[1], parts[2]];
      const dir = path.join(DATA_DIR, room, 'files');
      try {
        if (fs.existsSync(dir)) {
          const match = fs.readdirSync(dir).find(f => f.startsWith(fileId + '_'));
          if (match) {
            const fpath = path.join(dir, match);
            const metaPath = path.join(dir, fileId + '.meta');
            let mime = 'application/octet-stream';
            if (fs.existsSync(metaPath)) {
              const meta = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
              mime = MIME[path.extname(meta.name)] || mime;
            }
            res.writeHead(200, { 'Content-Type': mime, 'Cache-Control': 'max-age=86400' });
            fs.createReadStream(fpath).pipe(res);
            return;
          }
        }
      } catch {}
    }
    res.writeHead(404); res.end('not found');
    return;
  }

  // API: storage info
  if (url === '/storage') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ total: totalStorage(), max: MAX_STORAGE }));
    return;
  }

  // API: clear room
  if (url.startsWith('/clear/')) {
    const room = url.split('/').pop();
    clearRoom(room);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // Static files
  const safe = path.normalize(url).replace(/^(\.\.[\/\\])+/, '');
  const filePath = path.join(__dirname, safe === '/' ? 'index.html' : safe);
  try {
    if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
      const ext = path.extname(filePath);
      res.writeHead(200, { 'Content-Type': MIME[ext] || 'text/plain', 'Cache-Control': 'no-cache' });
      fs.createReadStream(filePath).pipe(res);
    } else {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
    }
  } catch {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  }
});

// --- WebSocket ---

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  let room = null;
  const cid = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

  ws.on('message', (data) => {
    const kind = Buffer.isBuffer(data) ? 'buffer' : typeof data;
    const len = Buffer.isBuffer(data) ? data.length : typeof data === 'string' ? data.length : data.byteLength || 0;
    console.log('[msg] room=' + room + ' kind=' + kind + ' len=' + len);
    const isBinary = Buffer.isBuffer(data) || data instanceof ArrayBuffer;
    const raw = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (isBinary) {
      relay(room, cid, raw);
      const fileId = bufferChunk(raw);
      return;
    }

    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === 'file-start') {
        chunks[msg.fileId] = {
          room, fileName: msg.fileName, fileType: msg.fileType,
          fileSize: msg.fileSize, parts: []
        };
        relay(room, cid, raw);
        saveMessage(room, msg);
        return;
      }

      if (msg.type === 'file-end') {
        relay(room, cid, raw);
        const info = saveChunkedFile(msg.fileId);
        if (info) {
          const storeMsg = {
            type: 'file-ready', fileId: msg.fileId,
            fileName: info.name, fileType: info.type, fileSize: info.size,
            url: '/file/' + room + '/' + msg.fileId
          };
          saveMessage(room, storeMsg);
          relay(room, cid, JSON.stringify(storeMsg));
        }
        return;
      }

      if (msg.type === 'room-join') {
        const code = msg.room;
        if (!ROOMS[code]) ROOMS[code] = [];
        ROOMS[code] = ROOMS[code].filter(c => c.id !== cid);

        if (ROOMS[code].length >= 2) {
          ws.send(JSON.stringify({ type: 'room-full' }));
          return;
        }

        room = code;
        ROOMS[code].push({ ws, id: cid });
        ws.send(JSON.stringify({ type: 'room-joined' }));

        if (ROOMS[code].length === 2) {
          relay(room, cid, JSON.stringify({ type: 'peer-joined' }));
          ws.send(JSON.stringify({ type: 'peer-joined' }));
        }

        // Send history
        const history = getHistory(code);
        if (history.length > 0) {
          ws.send(JSON.stringify({ type: 'history', messages: history }));
        }
        return;
      }

      // text and other messages: relay + save
      relay(room, cid, raw);
      if (msg.type === 'text') saveMessage(room, msg);
    } catch {
      relay(room, cid, raw);
    }
  });

  ws.on('close', () => {
    if (room && ROOMS[room]) {
      ROOMS[room] = ROOMS[room].filter(c => c.id !== cid);
      relay(room, cid, JSON.stringify({ type: 'peer-left' }));
      if (ROOMS[room].length === 0) delete ROOMS[room];
    }
  });

  ws.on('error', () => {});
});

function relay(room, senderId, data) {
  if (!room || !ROOMS[room]) return;
  const peer = ROOMS[room].find(c => c.id !== senderId);
  if (peer && peer.ws.readyState === 1) {
    peer.ws.send(data, { binary: Buffer.isBuffer(data) });
  }
}

server.listen(PORT, () => console.log('ChatLink on port ' + PORT));
