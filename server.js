const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;

const rooms = new Map(); // room name -> { sockets: Map<id, ws>, game: string }

const wss = new WebSocketServer({ port: PORT });
wss.on('error', (err) => console.error('WebSocketServer error:', err));

wss.on('connection', (ws) => {
    let roomName = null;
    let myRoom = null;
    let myId = null;

    ws.on('error', (err) => console.error('Client socket error:', err));

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }

        if (msg.type === 'list') {
            const list = [];
            for (const [name, room] of rooms) {
                if (room.game === msg.game && room.sockets.size < MAX_PLAYERS) {
                    list.push({ name, players: room.sockets.size });
                }
            }
            ws.send(JSON.stringify({ type: 'roomlist', rooms: list }));
            return;
        }

        if (msg.type === 'host' || msg.type === 'join') {
            if (myRoom !== null) return; // already in a room
            if (typeof msg.room !== 'string' || msg.room.length === 0 || msg.room.length > 32) {
                ws.send(JSON.stringify({ type: 'error', message: 'invalid room name' }));
                return;
            }

            let room = rooms.get(msg.room);

            if (msg.type === 'host') {
                if (room) {
                    ws.send(JSON.stringify({ type: 'error', message: 'room already exists' }));
                    return;
                }
                room = { sockets: new Map(), game: msg.game };
                rooms.set(msg.room, room);
                myId = 0;
            } else {
                if (!room) {
                    ws.send(JSON.stringify({ type: 'error', message: 'room not found' }));
                    return;
                }
                if (room.game !== msg.game) {
                    ws.send(JSON.stringify({ type: 'error', message: 'room is for a different game' }));
                    return;
                }
                if (room.sockets.size >= MAX_PLAYERS) {
                    ws.send(JSON.stringify({ type: 'error', message: 'room full' }));
                    return;
                }
                for (let i = 1; i < MAX_PLAYERS; i++) {
                    if (!room.sockets.has(i)) { myId = i; break; }
                }
            }

            roomName = msg.room;
            myRoom = room;
            room.sockets.set(myId, ws);
            ws.send(JSON.stringify({ type: 'assigned', id: myId }));
            return;
        }

        if (msg.type === 'data' && myRoom !== null) {
            const out = JSON.stringify({ type: 'data', from: myId, payload: msg.payload });
            for (const [id, sock] of myRoom.sockets) {
                if (id !== myId && sock.readyState === sock.OPEN) {
                    sock.send(out);
                }
            }
        }
    });

    ws.on('close', () => {
        if (myRoom === null) return;
        myRoom.sockets.delete(myId);

        // Host left: on a real link cable the session ends when the parent
        // unplugs, so end it for everyone instead of leaving a headless room.
        if (myId === 0) {
            for (const sock of myRoom.sockets.values()) {
                sock.close(1000, 'host left');
            }
            myRoom.sockets.clear();
        }

        // Only delete the room if it's still ours (a new room may have reused the name).
        if (myRoom.sockets.size === 0 && rooms.get(roomName) === myRoom) {
            rooms.delete(roomName);
        }
    });
});

console.log(`NetLink relay listening on port ${PORT}`);
