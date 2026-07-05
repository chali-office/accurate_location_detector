const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const { Server } = require("socket.io");
const { nanoid } = require("nanoid");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 4096 });

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, "data");
const DB_FILE = path.join(DATA_DIR, "sessions.json");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(DB_FILE)) fs.writeFileSync(DB_FILE, JSON.stringify({}));

// --- tiny JSON-file store, keyed by trackToken ---
let db = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
// secondary index: viewToken -> trackToken (rebuilt on load, kept in sync)
const viewIndex = {};
for (const t of Object.keys(db)) viewIndex[db[t].viewToken] = t;

let saveTimer = null;
function persist() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    fs.writeFile(DB_FILE, JSON.stringify(db), () => {});
  }, 200);
}

const SESSION_TTL_MS = 1000 * 60 * 60 * 24; // 24h
const MAX_SESSIONS = 10000; // hard cap so the store can't be flooded onto disk

function cleanExpired() {
  const now = Date.now();
  for (const t of Object.keys(db)) {
    if (now - db[t].createdAt > SESSION_TTL_MS) {
      delete viewIndex[db[t].viewToken];
      delete db[t];
    }
  }
}
setInterval(cleanExpired, 1000 * 60 * 30);

// --- naive in-memory rate limit for session creation: 20/hour per IP ---
const createHits = new Map();
function rateLimited(ip) {
  const now = Date.now();
  const hits = (createHits.get(ip) || []).filter((t) => now - t < 3600_000);
  if (hits.length >= 20) return true;
  hits.push(now);
  createHits.set(ip, hits);
  return false;
}
setInterval(() => {
  const now = Date.now();
  for (const [ip, hits] of createHits) {
    const fresh = hits.filter((t) => now - t < 3600_000);
    if (fresh.length === 0) createHits.delete(ip);
    else createHits.set(ip, fresh);
  }
}, 600_000);

app.set("trust proxy", 1); // Coolify/Traefik sits in front
app.use(express.json({ limit: "2kb" }));
app.use(express.static(path.join(__dirname, "public")));

// Create a new session. Returns two SEPARATE secrets:
//  - trackToken: authorizes PUBLISHING location (goes to the person tracked)
//  - viewToken:  authorizes WATCHING only (stays with the creator)
// Neither can be derived from the other.
app.post("/api/session", (req, res) => {
  if (rateLimited(req.ip)) return res.status(429).json({ error: "too many sessions, slow down" });
  cleanExpired();
  if (Object.keys(db).length >= MAX_SESSIONS) {
    return res.status(503).json({ error: "session store full, try later" });
  }
  const trackToken = nanoid(12);
  const viewToken = nanoid(12);
  db[trackToken] = {
    trackToken,
    viewToken,
    createdAt: Date.now(),
    lastUpdate: null,
    location: null, // { lat, lng, accuracy, ts } — ts is SERVER time
    active: false,
  };
  viewIndex[viewToken] = trackToken;
  persist();
  res.json({ trackToken, viewToken });
});

// Track page: confirms the session exists. Never reveals the viewToken.
app.get("/api/session/track/:token", (req, res) => {
  const s = db[req.params.token];
  if (!s) return res.status(404).json({ error: "not found" });
  res.json({ ok: true });
});

// View page: current state. Never reveals the trackToken.
app.get("/api/session/view/:token", (req, res) => {
  const t = viewIndex[req.params.token];
  const s = t && db[t];
  if (!s) return res.status(404).json({ error: "not found" });
  res.json({ location: s.location, active: s.active, lastUpdate: s.lastUpdate, now: Date.now() });
});

function validLocation(loc) {
  if (!loc || typeof loc !== "object") return false;
  const { lat, lng, accuracy } = loc;
  if (typeof lat !== "number" || !Number.isFinite(lat) || lat < -90 || lat > 90) return false;
  if (typeof lng !== "number" || !Number.isFinite(lng) || lng < -180 || lng > 180) return false;
  if (typeof accuracy !== "number" || !Number.isFinite(accuracy) || accuracy < 0 || accuracy > 100000) return false;
  return true;
}

// socket.id -> trackToken, so we can detect the publisher dropping off
const publisherSockets = new Map();

io.on("connection", (socket) => {
  // Viewers join with the VIEW token; internally rooms are named by trackToken.
  socket.on("join", (viewToken) => {
    if (typeof viewToken !== "string" || viewToken.length > 64) return;
    const t = viewIndex[viewToken];
    if (!t || !db[t]) return;
    socket.join(t);
    if (db[t].location) socket.emit("location:update", db[t].location);
    if (!db[t].active && db[t].lastUpdate) socket.emit("track:stopped");
  });

  // Publisher sends updates with the TRACK token. Payload is validated and
  // the timestamp is replaced with server time (client clocks can't be trusted).
  socket.on("track:update", (msg) => {
    if (!msg || typeof msg !== "object") return;
    const { token, location } = msg;
    if (typeof token !== "string" || token.length > 64) return;
    const s = db[token];
    if (!s || !validLocation(location)) return;

    publisherSockets.set(socket.id, token);
    const clean = {
      lat: location.lat,
      lng: location.lng,
      accuracy: Math.round(location.accuracy),
      ts: Date.now(),
    };
    s.location = clean;
    s.lastUpdate = clean.ts;
    s.active = true;
    persist();
    io.to(token).emit("location:update", clean);
  });

  socket.on("track:stopped", (token) => {
    if (typeof token !== "string" || token.length > 64) return;
    const s = db[token];
    if (!s) return;
    s.active = false;
    persist();
    io.to(token).emit("track:stopped");
  });

  // If the publisher's socket drops (tab closed, network lost, phone locked),
  // tell viewers — beforeunload/pagehide are unreliable on mobile.
  socket.on("disconnect", () => {
    const token = publisherSockets.get(socket.id);
    if (!token) return;
    publisherSockets.delete(socket.id);
    const s = db[token];
    if (!s) return;
    s.active = false;
    persist();
    io.to(token).emit("track:stopped");
  });
});

app.get("/track/:token", (req, res) => res.sendFile(path.join(__dirname, "public", "track.html")));
app.get("/view/:token", (req, res) => res.sendFile(path.join(__dirname, "public", "view.html")));

server.listen(PORT, () => console.log(`live-tracker running on :${PORT}`));
