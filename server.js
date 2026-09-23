const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

const PORT = process.env.PORT || 8080;

let queue = [];          // waiting sockets: [{socket, name}]
const rooms = {};        // roomId -> {players:[socketId,socketId], ready:[bool,bool], createdAt}

function broadcastQueueSize() {
  queue.forEach(q => q.socket.emit('queue_size', { n: queue.length }));
}

io.on('connection', (socket) => {
  socket.on('find_match', ({ name }) => {
    // Remove any stale entry for this socket first
    queue = queue.filter(q => q.socket.id !== socket.id);

    if (queue.length > 0) {
      const opponent = queue.shift();
      const roomId = opponent.socket.id + '_' + socket.id;
      rooms[roomId] = { players: [opponent.socket.id, socket.id], ready: [false, false], createdAt: Date.now() };

      opponent.socket.join(roomId);
      socket.join(roomId);

      opponent.socket.data.roomId = roomId;
      opponent.socket.data.role = 0;
      socket.data.roomId = roomId;
      socket.data.role = 1;

      opponent.socket.emit('match_found', { roomId, role: 0, opponentName: name || 'Пилот' });
      socket.emit('match_found', { roomId, role: 1, opponentName: opponent.name || 'Пилот' });
    } else {
      queue.push({ socket, name });
      socket.emit('waiting', { n: queue.length });
      broadcastQueueSize();
    }
  });

  socket.on('cancel_search', () => {
    queue = queue.filter(q => q.socket.id !== socket.id);
    broadcastQueueSize();
  });

  socket.on('placement_ready', () => {
    const roomId = socket.data.roomId;
    const room = rooms[roomId];
    if (!room) return;
    room.ready[socket.data.role] = true;
    if (room.ready[0] && room.ready[1]) {
      const first = Math.random() < 0.5 ? 0 : 1;
      io.to(roomId).emit('battle_start', { first });
    } else {
      socket.to(roomId).emit('opponent_ready');
    }
  });

  socket.on('shoot', ({ x, y, z }) => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    socket.to(roomId).emit('incoming_shot', { x, y, z });
  });

  socket.on('shot_result', ({ x, y, z, result, sunkCells, gameOver }) => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    socket.to(roomId).emit('shot_result', { x, y, z, result, sunkCells, gameOver });
    if (gameOver) {
      io.to(roomId).emit('game_over', { winner: socket.id });
      delete rooms[roomId];
    }
  });

  socket.on('disconnect', () => {
    queue = queue.filter(q => q.socket.id !== socket.id);
    broadcastQueueSize();
    const roomId = socket.data.roomId;
    if (roomId && rooms[roomId]) {
      socket.to(roomId).emit('opponent_left');
      delete rooms[roomId];
    }
  });
});

// Periodic queue-size ping (covers clients that missed the change event)
setInterval(broadcastQueueSize, 3000);

// Clean up stale rooms (>60 min old)
setInterval(() => {
  const now = Date.now();
  Object.keys(rooms).forEach((id) => {
    if (now - rooms[id].createdAt > 60 * 60 * 1000) delete rooms[id];
  });
}, 10 * 60 * 1000);

server.listen(PORT, () => {
  console.log(`🚀 Star Grid server on :${PORT}`);
});
