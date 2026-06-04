const rooms = {};

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript",
  ".json": "application/json",
  ".svg": "image/svg+xml"
};

function relay(room, senderId, data) {
  if (!room || !rooms[room]) return;
  const peer = rooms[room].find(c => c.id !== senderId);
  if (peer) {
    try { peer.socket.send(data); } catch {}
  }
}

Deno.serve(async (req) => {
  const url = new URL(req.url);

  // WebSocket
  if (url.pathname === "/ws") {
    try {
      const { socket, response } = Deno.upgradeWebSocket(req);
      let room = null;
      const cid = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

      socket.addEventListener("message", (e) => {
        const data = e.data;
        if (data instanceof ArrayBuffer) {
          relay(room, cid, data);
          return;
        }
        try {
          const msg = JSON.parse(data);
          if (msg.type === "room-join") {
            const code = msg.room;
            if (!rooms[code]) rooms[code] = [];
            rooms[code] = rooms[code].filter(c => c.id !== cid);
            if (rooms[code].length >= 2) {
              socket.send(JSON.stringify({ type: "room-full" }));
              return;
            }
            room = code;
            rooms[code].push({ socket, id: cid });
            socket.send(JSON.stringify({ type: "room-joined" }));
            if (rooms[code].length === 2) {
              relay(room, cid, JSON.stringify({ type: "peer-joined" }));
              socket.send(JSON.stringify({ type: "peer-joined" }));
            }
            return;
          }
          relay(room, cid, data);
        } catch {
          relay(room, cid, data);
        }
      });

      socket.addEventListener("close", () => {
        if (room && rooms[room]) {
          rooms[room] = rooms[room].filter(c => c.id !== cid);
          relay(room, cid, JSON.stringify({ type: "peer-left" }));
          if (rooms[room].length === 0) delete rooms[room];
        }
      });

      return response;
    } catch (e) {
      return new Response("WS upgrade failed: " + e.message, { status: 500 });
    }
  }

  // Health check
  if (url.pathname === "/ping") {
    return new Response("ok", { headers: { "content-type": "text/plain" } });
  }

  // Serve static files
  let filePath = url.pathname === "/" ? "/index.html" : url.pathname;
  try {
    const ext = filePath.slice(filePath.lastIndexOf("."));
    const mime = MIME[ext] || "text/plain";
    const content = ext === ".json" || ext === ".html" || ext === ".js"
      ? await Deno.readTextFile("." + filePath)
      : await Deno.readFile("." + filePath);
    return new Response(content, { headers: { "content-type": mime } });
  } catch {
    const html = await Deno.readTextFile("./index.html");
    return new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } });
  }
});
