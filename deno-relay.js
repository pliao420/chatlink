const rooms = {};

function relay(room, senderId, data) {
  if (!room || !rooms[room]) return;
  const peer = rooms[room].find(c => c.id !== senderId);
  if (peer) {
    try { peer.socket.send(data); } catch {}
  }
}

Deno.serve((req) => {
  if (req.headers.get("upgrade") === "websocket") {
    const { socket, response } = Deno.upgradeWebSocket(req);
    let room = null;
    const cid = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

    socket.addEventListener("open", () => console.log("[open]", cid));

    socket.addEventListener("message", (e) => {
      const data = e.data;
      const isBinary = data instanceof ArrayBuffer;

      if (isBinary) {
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

    socket.addEventListener("error", () => {});

    return response;
  }

  return new Response("ChatLink Relay", { status: 200 });
});
