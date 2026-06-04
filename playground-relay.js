const rooms = {};

function relay(room, senderId, data) {
  if (!room || !rooms[room]) return;
  const peer = rooms[room].find(c => c.id !== senderId);
  if (peer) {
    try { peer.socket.send(data); } catch {}
  }
}

Deno.serve((req) => {
  const url = new URL(req.url);

  if (url.pathname === "/ws") {
    const { socket, response } = Deno.upgradeWebSocket(req);
    let room = null;
    const cid = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

    socket.addEventListener("message", (e) => {
      const data = e.data;
      if (data instanceof ArrayBuffer) { relay(room, cid, data); return; }
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
      } catch { relay(room, cid, data); }
    });

    socket.addEventListener("close", () => {
      if (room && rooms[room]) {
        rooms[room] = rooms[room].filter(c => c.id !== cid);
        relay(room, cid, JSON.stringify({ type: "peer-left" }));
        if (rooms[room].length === 0) delete rooms[room];
      }
    });

    return response;
  }

  return new Response("ok");
});
