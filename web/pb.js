const MsgType = {
  HELLO: 1, WELCOME: 2, JOIN_QUEUE: 3, QUEUED: 4, MATCH_FOUND: 5,
  JOIN_ROOM: 6, INPUT: 7, SNAPSHOT: 8, PING: 9, PONG: 10, ERROR: 11, REDIRECT: 12,
  TURN_JOIN: 13, TURN_PLAY: 14, TURN_SYNC: 15, TURN_UPDATE: 16
};

// Mirrors enum TurnEventKind in the .proto.
const TurnKind = { DEALT: 1, PLAYED: 2, TRICK: 3, TIMEOUT: 4, ENDED: 5 };
window.TurnKind = TurnKind;
window.MsgType = MsgType;

function encVarint(n) {
  n = BigInt(n);
  if (n < 0n) n += 1n << 64n;
  const out = [];
  while (n > 0x7fn) { out.push(Number(n & 0x7fn) | 0x80); n >>= 7n; }
  out.push(Number(n));
  return out;
}
function zig(n) { return (n << 1) ^ (n >> 31); }
function key(f, wt) { return encVarint((f << 3) | wt); }
function encLen(f, bytes) { return [...key(f, 2), ...encVarint(bytes.length), ...bytes]; }
function encStr(f, s) {
  if (!s) return [];
  return encLen(f, Array.from(new TextEncoder().encode(s)));
}
function encU(f, n) { if (!n) return []; return [...key(f, 0), ...encVarint(n)]; }
function encS(f, n) { if (!n) return []; return [...key(f, 0), ...encVarint(zig(n >>> 0))]; }
function encB(f, v) { if (!v) return []; return [...key(f, 0), 1]; }

function encode(type, payloadBytes) {
  const body = [...encU(1, type), ...encLen(type + 1, payloadBytes)];
  return new Uint8Array(body);
}

// The wire contract this client was built against. The server turns away a
// client whose version it does not accept, with an error the page can show —
// rather than letting an incompatible build desync halfway into a match.
const PROTOCOL_VERSION = 1;
window.PROTOCOL_VERSION = PROTOCOL_VERSION;

window.pbEncode = {
  hello: (name, sessionId, accessToken) => encode(MsgType.HELLO, [
    ...encStr(1, name), ...encStr(2, sessionId), ...encStr(3, accessToken || ''),
    ...encU(6, PROTOCOL_VERSION)
  ]),
  joinQueue: () => encode(MsgType.JOIN_QUEUE, []),
  joinRoom: (roomId, yourId, lastTick, sessionId) => encode(MsgType.JOIN_ROOM, [
    ...encStr(1, roomId), ...encU(2, yourId), ...encU(3, lastTick), ...encStr(4, sessionId)
  ]),
  // interpMs is how far behind the newest snapshot this client draws everyone
  // else. The server owes it on top of the round trip when it compensates a
  // shot — see lagFor — and cannot know it from here, so it is declared. It is
  // clamped server-side, so inflating it buys nothing; understating it only
  // costs this client hits it should have landed.
  input: (seq, mx, my, fire, aim, ackTick, interpMs) => encode(MsgType.INPUT, [
    ...encU(2, seq), ...encS(3, mx), ...encS(4, my), ...encB(5, fire), ...encS(6, aim),
    ...encU(7, ackTick || 0), ...encU(8, Math.round(interpMs) || 0)
  ]),
  ping: (nonce) => encode(MsgType.PING, encU(1, nonce)),

  turnJoin: (matchId) => encode(MsgType.TURN_JOIN, encStr(1, matchId || '')),
  turnPlay: (matchId, card, turnNumber, idemKey) => encode(MsgType.TURN_PLAY, [
    ...encStr(1, matchId), ...encU(2, card), ...encU(3, turnNumber), ...encStr(4, idemKey || '')
  ]),
  turnSync: (matchId, sinceSeq) => encode(MsgType.TURN_SYNC, [
    ...encStr(1, matchId), ...encU(2, sinceSeq || 0)
  ]),
};

function decVarint(buf, i) {
  let x = 0n, s = 0n;
  while (i < buf.length) {
    const b = BigInt(buf[i++]);
    x |= (b & 0x7fn) << s;
    if ((b & 0x80n) === 0n) break;
    s += 7n;
  }
  return [x, i];
}
function unzig(n) { n = Number(n); return (n >>> 1) ^ -(n & 1); }

function decodeFields(buf) {
  const f = {};
  let i = 0;
  while (i < buf.length) {
    let k; [k, i] = decVarint(buf, i);
    const field = Number(k >> 3n), wt = Number(k & 7n);
    if (wt === 0) {
      let v; [v, i] = decVarint(buf, i);
      f[field] = v;
    } else if (wt === 2) {
      let n; [n, i] = decVarint(buf, i);
      const nNum = Number(n);
      f[field] = buf.slice(i, i + nNum);
      i += nNum;
    } else if (wt === 5) {
      i += 4;
    } else if (wt === 1) {
      i += 8;
    } else break;
  }
  return f;
}

function str(bytes) { return bytes ? new TextDecoder().decode(bytes) : ""; }

// Walk a message and hand every occurrence of one length-delimited field to fn.
// decodeFields keeps only the last occurrence, which loses repeated fields.
function scanRepeatedIn(b, field, fn) {
  let i = 0;
  while (i < b.length) {
    let k; const start = i; [k, i] = decVarint(b, i);
    const f = Number(k >> 3n), wt = Number(k & 7n);
    if (wt === 2) {
      let n; [n, i] = decVarint(b, i);
      const nNum = Number(n);
      const slice = b.slice(i, i + nNum);
      i += nNum;
      if (f === field) fn(slice);
    } else if (wt === 0) {
      [, i] = decVarint(b, i);
    } else break;
    if (i <= start) break;
  }
}

function decodeTurnState(buf) {
  const q = decodeFields(buf);
  return {
    match_id: str(q[1]), seq: Number(q[2] || 0),
    turn: str(q[3]), turn_number: Number(q[4] || 0),
    your_hand: decPacked(q[5]),
    opponent_hand_count: Number(q[6] || 0),
    your_score: Number(q[7] || 0), opponent_score: Number(q[8] || 0),
    table_card: Number(q[9] || 0), table_owner: str(q[10]),
    ended: !!q[11], winner: str(q[12]),
    deadline_unix_ms: Number(q[13] || 0),
    your_id: str(q[14])
  };
}

function decodeTurnEvent(buf) {
  const q = decodeFields(buf);
  return {
    seq: Number(q[1] || 0), kind: Number(q[2] || 0), player_id: str(q[3]),
    card: Number(q[4] || 0), hand_count: Number(q[5] || 0),
    hand: decPacked(q[6]), winner: str(q[7]),
    deadline_unix_ms: Number(q[8] || 0)
  };
}

// proto3 packs repeated scalars: one length-delimited field holding varints.
function decPacked(bytes) {
  const out = [];
  if (!bytes) return out;
  let i = 0;
  while (i < bytes.length) {
    let v; [v, i] = decVarint(bytes, i);
    out.push(Number(v));
  }
  return out;
}

// Bits of PlayerSnap.changed, mirroring enum PlayerField in the .proto.
const F = { X: 1, Y: 2, AIM: 4, HP: 8, SCORE: 16, BOT: 32, SEQ: 64 };

function decodePlayers(buf) {
  if (!buf) return [];
  const p = decodeFields(buf);
  return [{
    id: Number(p[1] || 0), x: unzig(p[2] || 0n), y: unzig(p[3] || 0n),
    aim: unzig(p[4] || 0n), hp: unzig(p[5] || 0n), score: Number(p[6] || 0), bot: !!p[7],
    seq: Number(p[9] || 0)
  }];
}

// GameEventKind mirrors the proto enum. Named here so the client reads
// `GameEventKind.KILL` rather than a 2 nobody can check.
const GameEventKind = { UNSPECIFIED: 0, HIT: 1, KILL: 2, DEPART: 3 };
window.GameEventKind = GameEventKind;

window.pbDecode = function (raw) {
  const buf = new Uint8Array(raw);
  const top = decodeFields(buf);
  const type = Number(top[1] || 0);
  const payload = top[type + 1] || new Uint8Array();
  const p = decodeFields(payload);
  const msg = { t: type };
  if (type === MsgType.WELCOME) {
    msg.your_id = Number(p[1] || 0); msg.tick_rate = Number(p[2] || 0); msg.session_id = str(p[5]);
  } else if (type === MsgType.QUEUED) {
    msg.queued = true;
  } else if (type === MsgType.MATCH_FOUND || type === MsgType.REDIRECT) {
    msg.room_id = str(p[1]); msg.host = str(p[2]); msg.your_id = Number(p[3] || 0); msg.tick_rate = Number(p[5] || 0);
  } else if (type === MsgType.SNAPSHOT) {
    msg.tick = Number(p[1] || 0); msg.room_id = str(p[2]); msg.ended = !!p[5]; msg.winner = Number(p[6] || 0);
    msg.baseline_tick = Number(p[7] || 0);
    msg.removed_projectiles = decPacked(p[8]);
    msg.players = []; msg.projectiles = [];
    const scanRepeated = (field, fn) => scanRepeatedIn(payload, field, fn);
    scanRepeated(3, (s) => {
      const q = decodeFields(s);
      msg.players.push({
        id: Number(q[1] || 0), x: unzig(q[2] || 0n), y: unzig(q[3] || 0n),
        aim: unzig(q[4] || 0n), hp: unzig(q[5] || 0n), score: Number(q[6] || 0), bot: !!q[7],
        changed: Number(q[8] || 0),
        // Field 9, and it is not decoration: this is the last input of ours the
        // server has folded in, and Predictor.reconcile drops everything up to
        // it from the pending queue before replaying the rest. Skipping it left
        // ackSeq at 0 for the whole match, so nothing was ever dropped — the
        // queue grew without bound and every tick replayed the match from its
        // first input. See the note on F.SEQ.
        seq: Number(q[9] || 0)
      });
    });
    scanRepeated(4, (s) => {
      const q = decodeFields(s);
      msg.projectiles.push({ id: Number(q[1] || 0), x: unzig(q[2] || 0n), y: unzig(q[3] || 0n) });
    });
    // Field 9: what happened between baseline_tick and tick.
    //
    // A snapshot replicates state, and state cannot carry a discrete fact: two
    // hits inside one delta window are a single HP change, and a death followed
    // by a respawn is no change at all. Without these there is no way to draw a
    // hitmarker or a killfeed — the client can see that HP fell, never that it
    // fell twice or who did it.
    msg.events = [];
    scanRepeated(9, (s) => {
      const q = decodeFields(s);
      msg.events.push({
        tick: Number(q[1] || 0), kind: Number(q[2] || 0),
        actor: Number(q[3] || 0), target: Number(q[4] || 0), hp: unzig(q[5] || 0n)
      });
    });
  } else if (type === MsgType.TURN_UPDATE) {
    msg.full_resync = !!p[1];
    msg.current_seq = Number(p[4] || 0);
    msg.state = p[2] ? decodeTurnState(p[2]) : null;
    msg.events = [];
    scanRepeatedIn(payload, 3, (s) => msg.events.push(decodeTurnEvent(s)));
  } else if (type === MsgType.PONG) {
    msg.nonce = Number(p[1] || 0);
  } else if (type === MsgType.ERROR) {
    msg.error = str(p[2]);
  }
  return msg;
};

function cloneEvents(evs) {
  return (evs || []).map(e => ({ tick: e.tick, kind: e.kind, actor: e.actor, target: e.target, hp: e.hp }));
}

function applyPlayer(base, d) {
  if (!d) return { id: base.id, x: base.x, y: base.y, aim: base.aim, hp: base.hp, score: base.score, bot: base.bot, seq: base.seq };
  const out = base
    ? { id: d.id, x: base.x, y: base.y, aim: base.aim, hp: base.hp, score: base.score, bot: base.bot, seq: base.seq }
    : { id: d.id, x: 0, y: 0, aim: 0, hp: 0, score: 0, bot: false, seq: 0 };
  const c = d.changed | 0;
  if (c & F.X) out.x = d.x;
  if (c & F.Y) out.y = d.y;
  if (c & F.AIM) out.aim = d.aim;
  if (c & F.HP) out.hp = d.hp;
  if (c & F.SCORE) out.score = d.score;
  if (c & F.BOT) out.bot = d.bot;
  if (c & F.SEQ) out.seq = d.seq;
  return out;
}

// Every id list in a snapshot the server sends is strictly ascending: the
// simulation keeps players sorted by id for the life of a match, projectiles
// are appended with increasing ids and compacted in place, and the delta
// encoder builds its removal list by walking the baseline in that order.
//
// Nothing guarantees that about what comes off a socket. A snapshot naming the
// same id twice used to be copied straight through — the full-snapshot path
// maps the list as it stands, and the baseline walk below emitted one entry per
// occurrence — and this client was left holding a state with one player in it
// twice. Which of the two `players.find(p => p.id === me)` reconciles against
// is then whichever happened to come first. The server's fuzz test found the
// same hole in the Go implementation; this is the same guard.
//
// Refusing beats repairing: a message this malformed is corrupt or hostile, and
// null already means "cannot apply — keep acking the last good tick and wait",
// which is a path the caller handles. Mirrors room.wellOrdered in Go.
function wellOrdered(s) {
  const ascending = a => {
    for (let i = 1; i < a.length; i++) if (a[i] <= a[i - 1]) return false;
    return true;
  };
  // Events are non-decreasing rather than strictly ascending: a tick can
  // produce several, and a hit and the kill it caused share one. What we cannot
  // be handed is a feed that goes backwards — these are rendered in order, and
  // an out-of-order pair puts a kill above the shot that made it. Mirrors the
  // same clause in room.wellOrdered.
  const nonDecreasing = a => {
    for (let i = 1; i < a.length; i++) if (a[i] < a[i - 1]) return false;
    return true;
  };
  return ascending((s.players || []).map(p => p.id))
    && ascending((s.projectiles || []).map(q => q.id))
    && ascending(s.removed_projectiles || [])
    && nonDecreasing((s.events || []).map(e => e.tick));
}

// Rebuild the full state at d.tick from the baseline it was encoded against.
// Returns null when we no longer hold that baseline — the caller must then keep
// acking its last good tick and wait for the server to catch up or send a full
// snapshot. Mirrors room.ApplyDelta in Go.
window.pbApplyDelta = function (base, d) {
  if (!d) return null;
  // Both halves are checked before either is used, the same order Go does it.
  if (!wellOrdered(d)) return null;
  if (!d.baseline_tick) {
    return {
      t: d.t, tick: d.tick, room_id: d.room_id, ended: d.ended, winner: d.winner,
      // Every field is authoritative in a full snapshot, so every bit is set —
      // all seven of them. This read 0x3f while seq was not decoded, which
      // quietly made a full snapshot the one message that could not correct a
      // client's idea of what the server had acknowledged.
      players: (d.players || []).map(p => applyPlayer(null, { ...p, changed: 0x7f })),
      projectiles: (d.projectiles || []).map(q => ({ id: q.id, x: q.x, y: q.y })),
      // Carried, never merged. Events describe the span between the baseline
      // and this tick, so a rebuilt state used as the next baseline carries a
      // span that has already been consumed — which is why nothing reads them
      // back off a baseline. Mirrors room.ApplyDelta.
      events: cloneEvents(d.events)
    };
  }
  if (!base || base.tick !== d.baseline_tick) return null;
  if (!wellOrdered(base)) return null;

  const out = {
    t: d.t, tick: d.tick, room_id: d.room_id, ended: d.ended, winner: d.winner,
    players: [], projectiles: [], events: cloneEvents(d.events)
  };

  // A sorted merge rather than "walk the baseline, then append the rest".
  // Both lists are ascending — wellOrdered just said so — and the result has to
  // be ascending too, because it becomes the baseline for the next delta and is
  // checked again on the way back in. Appending a player the delta introduces
  // on the end would have put the client's own state out of order and made it
  // refuse its next snapshot. Go's ApplyDelta merges for the same reason.
  const bp = base.players || [], dp = d.players || [];
  let i = 0, j = 0;
  while (i < bp.length || j < dp.length) {
    if (j >= dp.length || (i < bp.length && bp[i].id < dp[j].id)) {
      out.players.push(applyPlayer(bp[i], null)); i++;          // unchanged since the baseline
    } else if (i >= bp.length || dp[j].id < bp[i].id) {
      out.players.push(applyPlayer(null, dp[j])); j++;          // new: the delta carries it in full
    } else {
      out.players.push(applyPlayer(bp[i], dp[j])); i++; j++;    // patched
    }
  }

  const removed = new Set(d.removed_projectiles || []);
  const pp = new Map((d.projectiles || []).map(q => [q.id, q]));
  for (const b of base.projectiles) {
    if (removed.has(b.id)) continue;
    const q = pp.get(b.id);
    if (q) { out.projectiles.push({ id: q.id, x: q.x, y: q.y }); pp.delete(b.id); }
    else out.projectiles.push({ id: b.id, x: b.x, y: b.y });
  }
  for (const q of d.projectiles || []) if (pp.has(q.id)) out.projectiles.push({ id: q.id, x: q.x, y: q.y });
  return out;
};

window.SNAPSHOT_HISTORY = 64;
