const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const ROOMS = {};

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.png': 'image/png'
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x').pathname;
  const safe = path.normalize(url).replace(/^(\.\.[\/\\])+/, '');
  const filePath = path.join(__dirname, safe === '/' ? 'index.html' : safe);

  try {
    if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
      const ext = path.extname(filePath);
      res.writeHead(200, { 'Content-Type': MIME[ext] || 'text/plain' });
      fs.createReadStream(filePath).pipe(res);
    } else {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
    }
  } catch {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  }
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  let room = null;
  const cid = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

  ws.on('message', (data) => {
    const isBinary = Buffer.isBuffer(data) || data instanceof ArrayBuffer;
    const raw = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (isBinary) {
      relay(room, cid, raw);
      return;
    }

    try {
      const msg = JSON.parse(raw.toString());

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
        return;
      }

      relay(room, cid, raw);
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

server.listen(PORT, () => console.log('ChatLink relay on port ' + PORT));
