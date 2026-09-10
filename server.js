/**
 * Star Grid — Online Server
 * Node.js + Socket.io matchmaking + game relay
 * Deploy: Railway / Render / Fly.io (free tier)
 */
const express  = require('express');
const http     = require('http');
const { Server } = require('socket.io');
const path     = require('path');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: '*', methods: ['GET','POST'] }
});

// Serve the game from /public
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ── State ──────────────────────────────────────
let queue  = [];          // waiting players
const rooms = {};         // roomId → Room

function Room(p1, p2) {
  return {
    players: [p1, p2],
    ready:   [false, false],
    turn:    0,           // index of who shoots now
    shots:   0,
    startedAt: Date.now(),
  };
}

function roomOf(socketId) {
  return Object.entries(rooms).find(([, r]) =>
    r.players.some(p => p.id === socketId));
}

function broadcast(roomId, event, data, exclude = null) {
  const room = rooms[roomId];
  if (!room) return;
  room.players.forEach(p => {
    if (p.id !== exclude) p.emit(event, data);
  });
}

// ── Queue heartbeat: broadcast queue size every 3s ──
setInterval(() => {
  queue.forEach(s => s.emit('queue_size', { n: queue.length }));
}, 3000);

// ── Cleanup stale rooms (> 60min) ──
setInterval(() => {
  const now = Date.now();
  for (const [id, room] of Object.entries(rooms)) {
    if (now - room.startedAt > 60 * 60 * 1000) delete rooms[id];
  }
}, 10 * 60 * 1000);

// ── Connection ─────────────────────────────────
io.on('connection', socket => {
  console.log('+ connect', socket.id);

  // ── MATCHMAKING ────────────────────────────
  socket.on('find_match', ({ name = 'Пилот' } = {}) => {
    socket.playerName = name;

    // Don't double-queue
    if (queue.some(s => s.id === socket.id)) return;

    if (queue.length > 0) {
      const opponent = queue.shift();
      const roomId   = 'r_' + Date.now().toString(36);

      rooms[roomId] = Room(opponent, socket);
      [opponent, socket].forEach((s, i) => {
        s.join(roomId);
        s.roomId = roomId;
        s.role   = i;
        s.emit('match_found', {
          roomId,
          role: i,
          opponentName: i === 0 ? socket.playerName : opponent.playerName,
        });
      });
      console.log('match', roomId, opponent.id, socket.id);
    } else {
      queue.push(socket);
      socket.emit('waiting', { n: queue.length });
    }
  });

  socket.on('cancel_search', () => {
    queue = queue.filter(s => s.id !== socket.id);
    socket.emit('search_cancelled');
  });

  // ── PLACEMENT ──────────────────────────────
  socket.on('placement_ready', () => {
    const rid  = socket.roomId;
    const room = rooms[rid];
    if (!room) return;

    room.ready[socket.role] = true;
    socket.to(rid).emit('opponent_ready');

    if (room.ready[0] && room.ready[1]) {
      // Role 0 always shoots first
      io.to(rid).emit('battle_start', { first: 0 });
      console.log('battle', rid);
    }
  });

  // ── SHOT RELAY ─────────────────────────────
  // Shooter → server → defender
  socket.on('shoot', ({ x, y, z }) => {
    const rid  = socket.roomId;
    const room = rooms[rid];
    if (!room) return;

    const defRole = 1 - socket.role;
    const defender = room.players[defRole];
    defender.emit('incoming_shot', { x, y, z });
  });

  // Defender validates → server → shooter
  socket.on('shot_result', ({ x, y, z, result, sunkCells, gameOver }) => {
    const rid      = socket.roomId;
    const room     = rooms[rid];
    if (!room) return;

    const shooterRole = 1 - socket.role;
    const shooter     = room.players[shooterRole];

    shooter.emit('shot_result', { x, y, z, result, sunkCells, gameOver });

    if (gameOver) {
      io.to(rid).emit('game_over', { winner: shooter.id });
      delete rooms[rid];
      console.log('game over', rid, 'winner', shooter.id);
    }
  });

  // ── DISCONNECT ─────────────────────────────
  socket.on('disconnect', () => {
    console.log('- disconnect', socket.id);
    queue = queue.filter(s => s.id !== socket.id);

    const rid = socket.roomId;
    if (rid && rooms[rid]) {
      socket.to(rid).emit('opponent_left');
      delete rooms[rid];
    }
  });
});

// ── Start ──────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () =>
  console.log(`🚀 Star Grid server on :${PORT}`));
