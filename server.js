const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;

const rooms = new Map(); // room name -> { sockets: Map<id, ws> }

const wss = new WebSocketServer({ port: PORT });

wss.on('connection', (ws) => {
    let roomName = null;
    let myId = null;

    ws.on('message', (raw) => {
        let msg;
        try {
            msg = JSON.parse(raw);
        } catch {
            return; // ignore malformed messages, no crash
        }

        if (msg.type === 'host' || msg.type === 'join') {
            if (roomName !== null) return; // already paired, ignore

            roomName = msg.room;
            let room = rooms.get(roomName);

            if (msg.type === 'host') {
                if (room) {
                    ws.send(JSON.stringify({ type: 'error', message: 'room already exists' }));
                    roomName = null;
                    return;
                }
                room = { sockets: new Map() };
                rooms.set(roomName, room);
                myId = 0;
            } else {
                if (!room) {
                    ws.send(JSON.stringify({ type: 'error', message: 'room not found' }));
                    roomName = null;
                    return;
                }
                if (room.sockets.size >= MAX_PLAYERS) {
                    ws.send(JSON.stringify({ type: 'error', message: 'room full' }));
                    roomName = null;
                    return;
                }
                // Assign the lowest free id in 1..3
                for (let i = 1; i < MAX_PLAYERS; i++) {
                    if (!room.sockets.has(i)) { myId = i; break; }
                }
            }

            room.sockets.set(myId, ws);
            ws.send(JSON.stringify({ type: 'assigned', id: myId }));
            return;
        }

        if (msg.type === 'data' && roomName !== null) {
            const room = rooms.get(roomName);
            if (!room) return;
            const out = JSON.stringify({ type: 'data', from: myId, payload: msg.payload });
            for (const [id, sock] of room.sockets) {
                if (id !== myId && sock.readyState === sock.OPEN) {
                    sock.send(out);
                }
            }
        }
    });

    ws.on('close', () => {
        if (roomName === null) return;
        const room = rooms.get(roomName);
        if (!room) return;
        room.sockets.delete(myId);
        if (room.sockets.size === 0) {
            rooms.delete(roomName); // clean up empty rooms
        }
    });
});

console.log(`NetLink relay listening on port ${PORT}`);
