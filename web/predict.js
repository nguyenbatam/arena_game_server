// Client-side prediction + reconciliation, and entity interpolation.
//
// The movement math below is a line-by-line port of applyInputs/movePlayers in
// internal/sim/world.go. It has to stay a port: the server is fixed-point
// int32, so every operation here truncates toward zero exactly like Go integer
// division does. Drift between the two is what makes prediction feel wrong.

const MAP_W = 2000000;
const MAP_H = 2000000;
const PLAYER_RADIUS = 18000;
const PLAYER_SPEED = 280000; // milli-units per second

// sim.NewWorld: speedTick = PlayerSpeed / Milli(tickRate), integer division.
function speedTickFor(tickRate) {
  return Math.trunc(PLAYER_SPEED / (tickRate > 0 ? tickRate : 20));
}

function clampDir(v) {
  if (v < -1) return -1;
  if (v > 1) return 1;
  return v | 0;
}

function clamp(v, lo, hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

// One simulation step for a single player's own movement. Mirrors the movement
// branch of World.applyInputs. Projectiles are deliberately not predicted —
// they are server-spawned, and predicting them paints ghost bullets that pop.
function stepMove(x, y, mx, my, speedTick) {
  const dx = clampDir(mx), dy = clampDir(my);
  if (dx === 0 && dy === 0) return { x, y };
  if (dx !== 0 && dy !== 0) {
    // Diagonal: 707/1000 ≈ 1/√2, integer-exact on both sides.
    x += Math.trunc(speedTick * dx * 707 / 1000);
    y += Math.trunc(speedTick * dy * 707 / 1000);
  } else {
    x += speedTick * dx;
    y += speedTick * dy;
  }
  return {
    x: clamp(x, PLAYER_RADIUS, MAP_W - PLAYER_RADIUS),
    y: clamp(y, PLAYER_RADIUS, MAP_H - PLAYER_RADIUS)
  };
}

// Predictor moves the local player immediately and corrects when the server
// disagrees, instead of waiting a full round-trip to see the key press land.
class Predictor {
  constructor(tickRate) {
    this.speedTick = speedTickFor(tickRate);
    this.pending = [];   // inputs sent but not yet acknowledged
    this.x = 0; this.y = 0;
    this.ready = false;  // no authoritative position seen yet
    this.error = 0;      // last correction distance, for the HUD
    this.replays = 0;
  }

  setTickRate(tickRate) { this.speedTick = speedTickFor(tickRate); }

  // Called the moment an input is sent. Returns the predicted position.
  record(seq, mx, my, alive) {
    if (!this.ready) return { x: this.x, y: this.y };
    this.pending.push({ seq, mx, my });
    if (alive) {
      const p = stepMove(this.x, this.y, mx, my, this.speedTick);
      this.x = p.x; this.y = p.y;
    }
    return { x: this.x, y: this.y };
  }

  // Called with the authoritative position and the last input seq the server
  // applied for us. Everything newer is replayed on top.
  reconcile(serverX, serverY, ackSeq, alive) {
    const before = this.ready ? { x: this.x, y: this.y } : null;

    this.x = serverX; this.y = serverY;
    this.ready = true;

    // Drop what the server has already folded in.
    while (this.pending.length && this.pending[0].seq <= ackSeq) this.pending.shift();

    if (alive) {
      for (const inp of this.pending) {
        const p = stepMove(this.x, this.y, inp.mx, inp.my, this.speedTick);
        this.x = p.x; this.y = p.y;
      }
    }
    this.replays = this.pending.length;

    if (before) {
      const dx = this.x - before.x, dy = this.y - before.y;
      this.error = Math.round(Math.sqrt(dx * dx + dy * dy));
    }
  }

  reset() { this.pending.length = 0; this.ready = false; this.error = 0; this.replays = 0; }

  pos() { return { x: this.x, y: this.y }; }
}

// Interpolator renders other players slightly in the past, so their motion is
// smooth between the 20 Hz snapshots instead of teleporting once per tick.
class Interpolator {
  constructor(delayMs = 100) {
    this.delay = delayMs;
    this.buf = []; // { at, tick, players: Map<id, {x,y,aim,hp,score,bot}> }
  }

  push(snapshot, now) {
    const players = new Map();
    for (const p of snapshot.players || []) players.set(p.id, p);
    this.buf.push({ at: now, tick: snapshot.tick, players });
    // Keep a little over a second at 20 Hz.
    while (this.buf.length > 30) this.buf.shift();
  }

  // Positions to draw at wall-clock `now`, excluding `skipId` (the local player,
  // who is predicted rather than interpolated).
  at(now, skipId) {
    if (!this.buf.length) return [];
    const target = now - this.delay;

    let a = null, b = null;
    for (let i = this.buf.length - 1; i >= 0; i--) {
      if (this.buf[i].at <= target) { a = this.buf[i]; b = this.buf[i + 1] || null; break; }
    }
    if (!a) a = this.buf[0];

    const out = [];
    const alpha = b && b.at > a.at ? Math.min(1, (target - a.at) / (b.at - a.at)) : 0;
    for (const [id, pa] of a.players) {
      if (id === skipId) continue;
      const pb = b && b.players.get(id);
      if (!pb || alpha === 0) { out.push(pa); continue; }
      out.push({
        ...pa,
        x: pa.x + (pb.x - pa.x) * alpha,
        y: pa.y + (pb.y - pa.y) * alpha
      });
    }
    return out;
  }

  reset() { this.buf.length = 0; }
}

// globalThis so the browser and `node -e` (the parity test) both see these.
globalThis.Predictor = Predictor;
globalThis.Interpolator = Interpolator;
globalThis.simStepMove = stepMove;
globalThis.simSpeedTickFor = speedTickFor;
