# Arena Game Server Demo

Dedicated-server demo for a **2D deathmatch arena** (WASD + mouse to shoot). It exists to work through the architecture a real game server needs — authoritative simulation, delta replication, matchmaking, placement, graceful shutdown — rather than to be a fun game.

Run locally:

```bash
cd backend && go run ./cmd/server
# open http://localhost:8080
```

With the platform tier as well — accounts, wallet, store, match history and a
ladder, all in RAM and none of it durable:

```bash
make run-platform
# open http://localhost:8080/platform.html
```

## Layout

```
backend/     Go module — the whole backend (cmd, internal, gen)
web/         frontend — canvas client, prediction, hand-written protobuf codec
proto/       contract shared by frontend and backend
docs/        architecture notes + design decisions
deploy/      k8s manifests
scripts/     gen-proto.sh
```

`proto/` sits at the root because both sides read it: the backend generates Go into `backend/gen/pb`, the frontend has a hand-written codec in `web/pb.js`. The Go module lives in `backend/`, so import paths are **unchanged** — `github.com/nguyenbatam/arena_game_server/internal/...`.

## Two game modes

| | Arena (realtime) | Turn-based |
|---|---|---|
| Package | `internal/room` + `internal/sim` | `internal/turn` |
| Sync | delta snapshots, cursor = `ack_tick` | event log, cursor = `seq` |
| Loop | 20 Hz tick, state in RAM | no tick, state in a store |
| Timeouts | tick counting | ZSET scheduler + atomic pop |
| Hidden information | none | yes — hands, projected per viewer |

Shared by both: gateway, auth, presence, session, metrics, shutdown.

Solo queueing still gets you a match: the server fills bots up to `ROOM_SIZE`.

Wire format: **protobuf** (`proto/arena/v1/arena.proto`). Hot config: `PUT /admin/config`, no restart.

## Architecture (the industry shape)

```
Client (WS/TCP)
    │
    ▼
Gateway          Matchmaker           GameServer (N instances)
 session/hub      queue → form         room actor (1 goroutine / match)
    │                 │                      │
    └──────── Redis (presence, queue Lua, registry, placement jobs, pubsub)
    └──────── Kafka  (match.started / match.ended — async, never on the tick path)
    └──────── Postgres (accounts, wallet, inventory, history, ladder — the platform tier)
```

The same model as Fortnite / Valorant / Agones: **the lobby is not the dedicated game server**.

| Role | Job |
|---|---|
| `gateway` | hold connections, hello/queue/input, fan in to a room |
| `matchmaker` | group N players, pick the least-loaded GS, enqueue a job |
| `gameserver` | run the tick loop, broadcast snapshots, report heartbeat |
| `all` | one process — enough to learn with and to load test |

Redis and Kafka hold things that can be rebuilt from play, and everything in
them has a TTL. Postgres holds the things that cannot: see [the platform
tier](#platform-tier-accounts-wallet-store-ladder).

## The problems a game server has to solve

### 1. 10k+ CCU
CCU is total connections, **not** 10k players in one room. 10k CCU is roughly 1250 rooms × 8. One goroutine per room; Go handles 10k connection goroutines comfortably.

```bash
ulimit -n 65535
go run ./cmd/loadtest -n 10000 -ramp 20s -dur 30s
```

Watch `/metrics` (`arena_ccu`, `arena_rooms_active`) and `/debug/pprof/goroutine`.

### 2. Rooms and matchmaking
At `ROOM_SIZE` a match forms immediately. At `MIN_PLAYERS` plus a `QUEUE_TIMEOUT` wait it forms with bots.

Redis **Lua ZSET atomic pop** — two matchmaker replicas cannot claim the same player. The queued blob is protobuf.

**Rating, and a window that widens.** Every player carries an Elo rating (`internal/rating`), read when they queue and written back when their match ends. Without the second half the first half is a search around a constant — which is what `Skill: 1000`, hard-coded on every queue entry, used to be.

The match is anchored on **the player who has waited longest**, not on the densest cluster of ratings: the anchor is the person the queue is failing, so the window is drawn around *their* rating and widens with *their* wait.

```
window = SKILL_WINDOW + SKILL_WIDEN x seconds_waited   (capped at SKILL_MAX_WINDOW)
```

That trade — match quality for queue time — is the part that makes skill matchmaking work at all. A fixed window is unusable in both directions: tight, and the players at the edges of the distribution never play; loose, and it may as well not be there. `SKILL_WINDOW=0` is the old pure-FIFO queue, which is what the load test uses.

One pass looks **8 x `ROOM_SIZE`** players down the queue, not the whole of it. The Redis side runs inside a script that blocks the server for as long as it runs, and the matchmaker fires every 50 ms — walking ten thousand queued players would cost everything else. The price is that a match may not form while in-window players sit deeper than that; the window widens with the anchor's wait, so it forms shortly after instead. Both implementations scan the same depth in the same order, and a test pins it: an in-window partner one place past the depth is not matched, and is matched as soon as they move inside it.

Elo rather than Glicko or TrueSkill on purpose: one number per player and a dozen lines of arithmetic, where the others want a deviation and a volatility each and earn that only against a real population. The plumbing — read before the queue, write after the match — is identical either way, so the formula can be swapped in one file.

Two numbers say whether it works, and they move against each other:

```
arena_match_skill_spread        # the rating gap inside formed matches
arena_matchmaking_wait_seconds  # what that quality costs in queue time
```

Queue wait alone cannot tell you anything: a queue that matches everybody instantly looks perfect right up until you notice who it matched them with.

### 3. Game state and client-side prediction
`internal/sim` is the source of truth. The client sends input only; the server simulates and snapshots. Client positions are never trusted.

The client **does not wait for the round trip**: `web/predict.js` applies the move locally straight away, then when a snapshot arrives it takes the server position and replays the inputs that have not been acknowledged yet (`PlayerSnap.seq` says how far the server has got).

The movement maths in JS is a line-by-line port of `applyInputs`. A port drifting from its original is the classic way to make prediction feel wrong, so a test runs both and compares:

```bash
cd backend && go test ./internal/sim -run WebPrediction -v
```

Other players are **interpolated** — rendered 100 ms behind, between two snapshots — rather than drawn straight at 20 Hz.

### 4. Fixed tick
Gaffer-on-Games timestep. Default **20 Hz** (50 ms). `TICK_RATE=20|30|60`.

If `Step()` runs over budget you get `arena_tick_overruns_total`. An overrun means the simulation is not keeping up; in production the answer is to cut density or split the instance, not to "catch up" by skipping physics at random.

### 4b. The input jitter buffer
A client at the tick rate produces exactly one input per tick. The server consumes exactly one per tick. Those two facts do not add up to one input per tick arriving, because the network does not deliver on a metronome: one window gets two, the next gets none.

The room used to keep **one slot per player**, last write wins. So the window that received two threw one away — a frame of movement the server already had, discarded — and the window that received none left the player standing still. The client had predicted moving through both, so it took a correction backwards. It reads as a bad connection; it was the server dropping input it was holding.

Now each player has a small queue, and the tick takes **one input out of it**. The surplus from a crowded window is spent on the empty one. Three properties make it a buffer rather than a delay:

- **Nothing is held when there is nothing to absorb.** A steady stream is drained every tick, so with no jitter there is no added latency at all.
- **A client running ahead is trimmed, not queued.** A clock that runs fast would otherwise build a backlog that never drains, and every input it sent would be simulated further into the past. Past `INPUT_BUFFER`, the oldest go: the player loses frames and stays in the present, which is the trade a player would pick.
- **Lag compensation is measured when the input is simulated**, not when it arrived, and against the tick it is *about to be* simulated at rather than the one already broadcast. An input held for a tick was produced one tick further in the past, and it is compensated as such. Still capped at `MaxLagCompTicks`.

Measured over a 15-second run, 200 bots, 8 per room, on loopback (`arena_inputs_total` 65,499):

| `INPUT_BUFFER` | inputs dropped | underruns |
|---:|---:|---:|
| 1 (the old behaviour) | 132 | 132 |
| **2 (default)** | **0** | **0** |
| 3 | 0 | 14 |

The two columns moving together is the whole phenomenon: every input dropped because two arrived at once is a tick that later had none. Over loopback with a Go ticker as the only jitter source that is 0.2% of input — a floor, not a typical figure. Over a real network it is the thing players describe as the server feeling unresponsive.

```
arena_input_buffer_dropped_total   # clients producing faster than we consume
arena_input_underruns_total        # active players with nothing to simulate
```

### 5. WebSocket + TCP + UDP
- WS: browser `/ws`, binary protobuf
- TCP: length-prefixed protobuf (`uint32` BE + payload)
- UDP: `:8082`, **one datagram is one envelope** — a datagram already has a boundary, so no length prefix

UDP changes three things and `internal/net/udp` deals with all three: no framing, no connection (it keeps its own peer table and evicts on silence, because nothing else will tell it a client left), and no delivery guarantee — which is exactly what a snapshot wants.

**Source-address spoofing.** UDP has no handshake, so anyone can put a victim's address in the source field and have the server stream snapshots at them. Before committing any state the server replies with **an HMAC cookie bound to the address** (`Hello.addr_token`); only a sender that genuinely receives at that address can echo it back. This is DTLS's `HelloVerifyRequest` and QUIC's `Retry`. The key is random per process, cookies expire after 30 s, and the server keeps **no state at all** for an unverified address.

**Amplification.** A 36-byte cookie answering a 13-byte HELLO is a ~3× amplifier. So the first HELLO must be **padded to `MinInitial` (512 B)** before it is answered — the reply is then always smaller than the request. QUIC pads its Initial packets to 1200 B for exactly this reason.

Also: a datagram over `MaxDatagram` (1200 B) is **dropped rather than fragmented** — losing one fragment loses the whole packet.

**One peer cannot stall the rest.** The socket has a single receive queue shared by every client, so running a handler on the read loop makes the slowest handler the arrival rate for everyone — one HELLO doing its presence lookups stalls the loop, the kernel buffer fills, and datagrams are dropped for peers that did nothing wrong. Each peer gets a bounded inbox and its own goroutine, which keeps the read loop doing nothing but reading and still serialises each peer against itself. Measured with one peer parked: 0 of 90 datagrams handled before, 90 of 90 after.

### 6. Player sync — delta snapshots
The Quake 3 / Source model. A room keeps a **64-tick** ring buffer (`room.SnapshotHistory`). The client acknowledges the tick it managed to *apply* (`Input.ack_tick`, 20 times a second); the server encodes `diff(ring[ack], now)` and sends it with `Snapshot.baseline_tick`.

**The baseline only advances on an acknowledgement** — never on the assumption a packet arrived. Packet loss makes the next delta bigger; it can never desync the client.

It falls back to a full snapshot (`baseline_tick = 0`) when the client is new, is reconnecting, or its acknowledgement has aged out of the ring (> 64 ticks, about 3.2 s at 20 Hz). Full snapshots do not go away — they are the delta scheme's escape hatch.

`PlayerSnap.changed` is a bitmask: proto3 elides zero values from the wire, so without the mask a client cannot tell "moved to x=0" from "did not move".

The client keeps its own history and only acknowledges what it could apply — `web/pb.js` (`pbApplyDelta`) and `room.ApplyDelta` are two implementations of one algorithm.

Measured (`go test ./internal/room -run Bandwidth -v`): **a delta is about 80% of a full snapshot** in deathmatch. Less of a saving than you might expect, because everybody moves every tick so `x,y` always change; what is saved comes from `aim/hp/score/bot` and from stationary projectiles. A game with many idle entities does far better.

A delta is a pure function of (baseline, current), and current is shared, so two clients sitting on the same acknowledged tick are owed byte-identical messages — which in the steady state is every client in the room. Encoding once per distinct baseline rather than once per client keeps the tick cost linear in room size instead of quadratic: at 24 players, 31.1 µs and 775 allocations per tick become 5.3 µs and 64.

When the send buffer fills, the **oldest snapshot is dropped** (latest wins). Slow clients still see monotonic ticks. Joining the wrong node gets a redirect.

`/metrics`: `arena_snapshots_full_total`, `arena_snapshots_delta_total`, `arena_snapshot_bytes_total{kind}`.

### 7b. Lag compensation
Server-side hitbox rewind (CS2/Valorant) **does not transfer** to a bullet that travels for many ticks — there is no single instant to rewind to. Instead this is **projectile catch-up**, as Overwatch does it: the bullet is advanced to where it would already be had it been fired when the client saw it.

How far to advance it is Valve's formula, both halves of it:

```
Command Execution Time = Current Server Time - Packet Latency - Client View Interpolation
```

The **round trip** is the tick the input is about to be simulated at minus `ack_tick` — note *about to be*, not the last tick broadcast, which is one tick earlier and was worth 50 ms of missing compensation. The **interpolation** term is the half that is easy to forget because it is not latency: a client draws everyone else ~100 ms in the past so their motion is smooth, so it aimed at a world that much older again than the tick it acked. Ignore it and you compensate a world the shooter was never shown.

Three conditions are non-negotiable. The round trip is **derived server-side** from `ack_tick` rather than taken as a latency field, the total is **capped** (`MaxLagCompTicks = 10` — the cap is on the *sum*, since capping each term would let them add up past it), and the bullet's **TTL is charged for the catch-up** so compensation does not quietly extend its range.

The interpolation delay is the one figure the client does declare, because only the client knows how it renders — so it is **clamped at the wire** (`maxInterpMs = 150`), the way Source clamps it with `sv_client_max_interp_ratio`. It crosses in milliseconds, not ticks: the room's rate is configurable and a client's render delay is not a function of it. The load-test bot declares zero and that is honest — it draws nothing, so it holds no interpolation buffer.

The catch-up is **swept tick by tick, not teleported**: if someone is standing in the way, the shot hits them. Jumping straight to the end would pass through — the shooter sees a hit and the server records a miss. Both paths, the sweep and the per-tick collision pass, go through one `damage` function, so scoring and respawn rules cannot drift apart.

**What the derivation does not buy.** It removes one dial — a client cannot hand the server a millisecond count and have it believed — and it leaves the one underneath: `ack_tick` still comes off the wire. `recordAck` refuses an ack *ahead* of what the room has broadcast, and nothing can refuse one *behind* it, because that is exactly what a client losing packets legitimately sends. A client reporting an old tick while rendering a fresh one is claiming a worse connection than it has. So the **cap is the defence**, not the derivation, and `arena_lag_compensation_ticks_total` / `arena_lag_compensated_shots_total` / `arena_lag_compensation_capped_total` are where an implausible claim becomes visible: a population on bad links spreads out below the cap, a client turning the dial sits on it. Counters rather than a histogram, accumulated per room and flushed once a tick — a histogram `Observe` costs 11 ns uncontended and **275 ns at `-cpu 10`** (measured), and a player holding the fire button produces one per tick, so at 10k CCU it was 200k contended writes a second charged to the goroutines with a tick budget to keep. Telling a liar from a bad link needs a latency the server measures itself — a server-initiated ping the client echoes — which is a protocol change and is not built.

### 6b. Hit registration: swept, not sampled

A projectile covers 36 000 world units per tick at 20 Hz. A player and a projectile are 18 000 and 6 000 across, so the hit radius is 24 000 — **the step is wider than the target**. Testing the end point of each step therefore skips a gap wider than the thing it is looking for. Measured across every phase and offset inside the radius: of 432 shots fired straight at a target, **40 recorded a miss**, several of them through the middle of the body.

The fix is the one every engine that fires anything fast already applies: test the **segment the projectile travelled**, not the point it landed on. `sim.segDist2` is a point-to-segment distance in integers (the projection is truncated to whole units before the distance is taken — carrying it through algebraically overflows `int64` at map scale, which is the shape of bug that surfaces as a desync months later). The nearest target along the path is taken, so a shot crossing two players in one step stops at the one it reaches first rather than at whichever the roster listed first.

Discrete collision is only safe while the moving thing is slower than what it can hit is wide. `TestAProjectileStepIsWiderThanTheHitboxItMustNotSkip` says so out loud and skips itself if that ever stops being true, so the regression test below cannot start passing for the wrong reason.

### 6c. Spawn rules

Two rules that every arena shooter converges on, and for the same reasons.

**Respawn is on the safest free point**, not the one you opened on. A fixed point plus a visible respawn countdown is a free, repeatable kill: stand on it and shoot. The slot is chosen by distance to the *nearest* living player — averaging lets a crowd on the far side of the map outvote the one person standing on your head. Deterministic: the ring is walked in index order, the roster in its sorted order, ties go to the lowest index, and a slot is claimed as it is taken so two simultaneous respawns cannot collide.

**Spawn protection lasts `SpawnProtectTicks` (500 ms)** and **ends the moment you shoot**. Protection is there to get off the point, not to take a free shot — Halo, Team Fortress and Quake all break it on the same event. A protected player does not *absorb* the bullet either: it passes through, because a shield standing on the spawn point is worse than no protection at all.

### 6d. Leaving a match

Unsubscribing stops the snapshots. It does not stop the match paying the player out — and that turned out to matter once results reached a ladder.

A disconnected avatar used to stand in the world for the rest of the match: motionless, full HP, worth a kill every `RespawnTicks` to anyone who shot it. `recordResult` writes those kills into Elo and cannot tell a farmed one from a real one. So `Room.Leave` hands the seat to `sim.World.Depart`, which **lingers for `DepartLingerTicks` (3 s) and then despawns it** — the linger so that pulling the network cable is not the cheapest dodge in the game, the despawn so that nobody farms a body.

Reconnecting inside the grace window calls `Rejoin`: inside the linger the avatar is simply still there, and after it the player comes back through the ordinary respawn rather than reappearing where the body fell. The player is still rated for the match either way, so quitting while behind is not an escape.

**A match everyone has left ends.** With the avatar despawned, a room whose humans have all gone still has bots in it, and bots will happily play out the remaining minute and a half for an audience of nobody — counted against `MAX_ROOMS` the whole time, so placement keeps refusing matches that *do* have players on account of one that does not. `sim.abandoned()` ends it once every human seat has despawned, which means the linger doubles as the grace: drop and come straight back and the match is still there. The scores stand, so walking out is not a way to avoid the rating.

This is a change to the simulation that **no input carries**, which is why the replay format went to **v2**: a frame now records roster events alongside inputs. Without them every replay of a match somebody left would re-simulate a different world and report a desync that never happened. v1 files still load — they were written by a build where leaving changed nothing.

### 6e. Draws

`leader()` returns **0 for a tie**, and no seat holds that id. It used to return the lowest player id, which is not a tie-break so much as a coin permanently weighted towards seat one — and it disagreed with `internal/rating`, which has always scored an equal result as a draw for both players. The scoreboard said one thing and the ladder said another about the same match.

### 6f. Simulation rate is not send rate

`SNAPSHOT_RATE` decouples them. Raising `TICK_RATE` buys hit resolution and input latency; raising the send rate buys nothing but egress, and egress grows with the **square** of the room size (see the table below). Tied together, pushing `TICK_RATE` from 20 to 60 through `/admin/config` tripled every room's outbound traffic as a side effect of asking for a better simulation. Source calls the two tick and `sv_updaterate`; Overwatch simulates at 60 Hz and sends at 20.

The default is 0 — one snapshot per tick, exactly as before — so the measured table below still describes the shipped configuration.

The room keeps two clocks for this: `lastWorld` is where the simulation has got to, `lastTick` is the newest tick anyone was actually sent. An ack is checked against the second, the input path asks the first, and the final tick of a match always goes out whatever the interval, because `ended` is the one thing a client cannot wait for the next snapshot to learn.

### 6g. Warmup

The match clock used to start when the placement job was taken — before anybody had been told the match existed. In one process that is milliseconds. Across nodes it is a handoff (`MATCH_FOUND`, a new socket, a `HELLO`, a `JOIN_ROOM`), and every second of it came out of the match, unevenly, so whoever arrived first got a head start on an empty map.

`WARMUP_TIMEOUT` holds the room at tick zero until every seated connection has joined, or the budget runs out — a budget rather than a condition, because a seat whose player closed the tab must not hold the other seven. Bots are not waited for. `arena_match_warmup_seconds` and `arena_match_warmup_timeouts_total` say how it is going.

A seat still empty when the budget runs out is **departed**, which is the other half of the same idea. A player who never arrived has not *left*, so abandonment cannot see them, and a room nobody joined would otherwise run its full length against the node's ceiling — the worst case of the failure the warmup exists to notice. Arriving late still works: `joinRoom` calls `Rejoin`.

### 6h. Gameplay events: what happened, not just what is true

A snapshot replicates **state**, and state cannot carry a discrete fact. Two hits inside one delta window are a single HP change; a death followed by a respawn is no change at all. So a client watching `PlayerSnap` can see that HP fell — never that it fell twice, or to whom. That is the difference between a health bar and a hitmarker, a killfeed or a damage number, and it is why every shooter carries a second kind of message.

The usual way to carry one is a **reliable sub-channel**: its own sequence numbers, its own acks, its own retransmit. This does not need any of that, because the delta scheme already has exactly the right window.

> A delta is encoded against a tick the client **acknowledged**. So the same window that decides which *state* to resend decides which *events* it has not seen: `Room.eventsSince` walks the ring from the acked tick to now and carries everything in between. **Losing a packet makes the next message carry more events, exactly as it makes it carry more state.**

`sim.Event` is produced where the fact is known — `damage()` is the single place a hit resolves, so the catch-up sweep and the per-tick pass cannot disagree — and is an *output* of a tick rather than part of the world, which is why it is deliberately absent from `Checksum`. Replaying the same inputs reproduces it.

Two consequences worth stating, because both are load-bearing:

**It is capped.** `MaxSnapshotEvents = 8`, and the number is measured rather than chosen: at `config.MaxRoomSize` a full snapshot is 932 B against `udp.MaxDatagram`'s 1200, leaving 268, and an event costs at most 28 of them. `TestSnapshotWithAFullEventBurstFitsOneDatagram` fails if either side moves. Overflow keeps the newest — a killfeed missing its oldest lines is a killfeed; a message that does not arrive is neither.

**The client must consume by tick, not by event.** An ack rides on the next input and lands a round trip later, so until it does the server keeps encoding against the same baseline and keeps resending the same events. `web/index.html` therefore tracks the newest tick it has consumed and skips whole ticks at or below it. Deduplicating on the event's own fields would be *wrong* rather than merely repetitive: two shots from one player can land on one target in one tick, and they are two hits.

The bug this cost, found by the end-to-end test and nothing else: the despawn branch re-entered on every tick once a player's linger had run out, so one disconnect put a `DEPART` on the wire twenty times a second — a killfeed nobody can read, and a flood that pushed real hits out past the cap. The unit test meant to cover it stopped at the first event it saw.

### 7. Deterministic simulation
- Fixed-point milli (`int32`), no `float64` in gameplay
- splitmix64 RNG, seeded per match
- Spawn slots assigned from the roster sorted by player id, and respawn slots chosen by a walk in index order — never a map

`go test ./internal/sim` — same seed plus same inputs gives the same snapshot.

### 8. Redis
Presence as protobuf with a TTL, matchmaking ZSET, GS registry, room directory, placement jobs, `match_found` pub/sub, dynamic config.

Leave `REDIS_ADDR` unset locally and everything runs in memory behind the same interfaces.

Everything in there has a TTL, because everything in there is derived. What is
not derived — an account, a wallet, a ladder position — is in Postgres instead;
see [the platform tier](#platform-tier-accounts-wallet-store-ladder) for why
that is a different store and not a bigger Redis.

### 9. Kafka
`match.started` / `match.ended`, keyed by `room_id`. Asynchronous, and **never** on the tick path. Analytics and replay consumers are separate processes.

### 10. Graceful shutdown, capacity and draining
SIGTERM → **leave the placement pool** → `/readyz=503` → stop taking new room jobs → wait out `DRAIN_TIMEOUT` → stop accepting HTTP/TCP → `rooms.StopAll()` → close the hub → flush the event bus.

The first and third steps are what make the rest mean anything.

**Leaving the pool has to be explicit.** Draining is advertised in the heartbeat, but a node that is shutting down stops heartbeating, and the record it left behind stays valid for its full 15-second TTL. That is fifteen seconds of a matchmaker sending players to a process on its way out. `registry.Unregister` removes it on the first line of the shutdown path instead.

**A draining node must stop taking jobs.** `jobLoop` used to `BRPOP` throughout the drain, so the same process went on opening matches while it counted down — the drain never reached zero, and the players in those brand-new rooms were cut off seconds later.

**Placement balances; it never refused.** `PickLeastLoaded` always returns the least loaded node, which is the right answer right up to the point where every node is past what it can simulate — and then it keeps handing matches to whichever server is missing its tick budget by the smallest margin. `MAX_ROOMS` is the declared ceiling: placement skips a node that is at it, the node itself refuses as a backstop (its own view is fresher than a two-second-old heartbeat), and the players go back in the queue keeping their original `queued_at`, so this shows up as queue wait rather than as a silent loss.

```
arena_placements_refused_total   # the fleet is out of room — add capacity
```

### 11. Metrics and profiling
- Prometheus: `http://localhost:8080/metrics`
- pprof: `/debug/pprof/profile|heap|goroutine|mutex|block`
- Grafana is optional; Prometheus scrapes inside compose

The ones that mean something is wrong rather than something is busy:

| Metric | What it says |
|---|---|
| `arena_panics_total{where}` | a room or a connection died of a bug. Page on it — the recovery kept the process up, and this is the only evidence left |
| `arena_tick_overruns_total` | the simulation is not keeping up |
| `arena_placements_refused_total` | the fleet is out of capacity; players are going back into the queue |
| `arena_match_skill_spread` | what matchmaking is actually producing, against `arena_matchmaking_wait_seconds` |
| `arena_snapshots_dropped_total` | slow clients |
| `arena_replay_errors_total` | recordings are failing, which nothing else would ever notice |
| `arena_events_dropped_total` | the broker is refusing lifecycle events — and these are not retried |
| `arena_input_buffer_dropped_total` / `arena_input_underruns_total` | clients whose input the server cannot use, and ticks with none to use |
| `arena_platform_ops_total{op,result}` | watch `result="error"`. `declined` is an overdrawn wallet or a cosmetic somebody already owns — the system working, and a counter that treated it as a failure would page on people shopping. `unauthorized` climbing on its own is somebody working through a list of tokens |
| `arena_platform_match_rows_total` | player results written. It sits below `arena_matches_ended_total` by exactly the bots and the unauthenticated sessions; a wider gap means results are being lost between the room and the database |

`arena_platform_op_duration_seconds` is deliberately a separate histogram from
the tick one. A tick is measured against a 50 ms budget and a purchase against a
player's patience; in one histogram the interesting tail of each is buried in
the other's bulk. Failures are recorded in it as well as successes — a call that
timed out still spent the two seconds, and a histogram of successes only stays
flat and healthy right through the outage you are trying to see.

### 12. Load testing
`cmd/loadtest`: ramps WebSocket connections, queues, sends input at 20 Hz, prints CCU / matches / snapshots per second.

The bots do not log in, so `backend.loadtest.env` blanks `JWT_SECRET` for the run — the same reasoning as the zeroed rate limiters, and the same reason it belongs in that file and nowhere else.

### 12b. Replays
The simulation is deterministic, so a replay is not a recording of what happened — it is **the seed and the inputs**, and the match is re-simulated from them. About 6 KB for a 100-tick match, and it reproduces positions, hits and scores exactly.

That is also what makes it usable as evidence. A recording of what the server *said* happened could not be used to check the server; a recording of what went in can.

```bash
REPLAY_DIR=../replays make run          # record
make replay                             # re-simulate and compare checksums
go run ./cmd/replay -v ../replays/r-gs-local-123.arnr
```

```
r-gs-local-1789445108907217000.arnr   OK   seed=1789445108907225000 tick_rate=20 players=4 ticks=100 frames=100 checksum=0xe1d0bc7e0b4418e7
```

`World.Checksum` is an FNV-1a fingerprint of the whole world, stamped into the file when the match ends. A mismatch on replay means this build no longer reproduces the match that was played — a desync bug, or a gameplay change nobody meant to make. It is the same device a lockstep fighting game runs every tick, used here after the fact.

What it buys, in the order it gets used: a bug report that is a file rather than a story; a cheat review that can re-run the inputs the server actually accepted; and the thing every game eventually wants — spectating, highlights, esports.

**Sampled, not wholesale.** A 90-second match at 20 Hz is around 200 KB held in memory until it is written, which across a thousand rooms is more than the simulations cost. `MAX_REPLAYS` matches record at once and the rest run untouched, which is how live games do it too.

### 13. Races
**One room, one goroutine.** Network I/O must never touch `World`. Input goes through a channel and into a per-player queue that only the tick goroutine touches; one input comes out per tick (see 4b — it used to be last-wins, and that cost a frame of movement on every jittery window).

Connection state that more than one goroutine touches — the acknowledged tick, the room binding, the session id — is unexported behind accessors, because leaving it bare is a data race the tick loop hits in normal operation.

```bash
cd backend && go test -race -count=1 ./...   # data races
# goleak in TestMain (room/matchmaking) catches goroutine leaks
```

### 13b. Crash isolation
A room is the unit of parallelism. It has to be the unit of *failure* as well, and in Go that is not free: one unrecovered panic anywhere takes the process with it, so a single out-of-range index in gameplay code ends **every match on the node** — about 1250 of them at 8 players and 10k CCU — on every node the bad input reaches.

`internal/safe` is what confines it. Every goroutine the server starts and every callback it runs for one client goes through it: the tick loop, the transports' read and write pumps, the matchmaker and job loops, the pub/sub delivery callbacks, and the per-message handler.

- A room that panics **ends like a match that finished**: same `OnEnd` path, so the room directory, the lifecycle event and the clients all behave as they do at any other end. The players reconnect and queue again.
- A background loop that panics **restarts**. Recovery alone is not enough there: a matchmaker loop that dies leaves the queue filling up with nobody forming matches, and the panic counter ticks once while the server goes quiet.
- The per-message recover is written out by hand rather than wrapped in a closure — it runs twenty times a second per connection, and the hot path should not pay an allocation for it.

This is a blast-radius limiter, not error handling. `arena_panics_total{where}` is a **paging** metric: a room dying this way is a bug that reached production, and the counter is the only evidence left.

### 13c. What one connection may spend
The per-IP limiters (`HELLO_RATE_LIMIT` and friends) guard the doors a stranger knocks on. They do nothing about a client that is already inside: an authenticated connection could send inputs as fast as its link allowed, and every one of them cost a `proto.Unmarshal` before the room dropped it — the parse being the expensive half.

So every connection carries its own token bucket (`ratelimit.Budget`), charged **before** the payload is decoded, in two dimensions: messages and bytes. They fail differently — a flood of tiny inputs burns CPU, and a slow trickle of 64 KB frames burns bandwidth without ever tripping a message counter. Over budget, the connection is closed.

Per-connection is also the only fair place for this. A per-IP limit tight enough to stop one abusive client would throw out everyone behind a carrier NAT, which on mobile is a real population.

### 13d. Nothing waits on Redis forever
Presence, the queue, the room directory and the turn log are all off the tick path — and every one of them used to be called with `context.Background()`, no deadline at all. A Redis that stops answering then parks the goroutine of every player logging in, joining or disconnecting, and the process runs out of goroutines long before anything reports a problem.

`REDIS_TIMEOUT` is the ceiling on one such call. The blocking job queue (`BRPOP`) and the pub/sub subscriptions deliberately keep the process lifetime instead: they are supposed to wait.

### 14. Horizontal scaling
The matchmaker's `PickLeastLoaded` works off heartbeats, counting reserved-but-not-yet-started rooms so a burst of placements does not all land on one node. Job lists are per `NODE_ID`. The gateway receives `match_found` (the GS's public WS host) and the client connects to the right instance.

```bash
docker compose -f docker-compose.yml -f docker-compose.scale.yml up --build
kubectl apply -k deploy/k8s/overlays/prod   # see deploy/k8s/README.md
```

CI: `.github/workflows/ci.yml` (`gofmt` + `vet` + `golangci-lint` + tests + `-race` + proto drift).  
CD: `.github/workflows/cd.yml` pushes an image to `ghcr.io/<repo>`.

## Protocol (protobuf)

A `MsgType` enum plus a `oneof payload`. Client: `hello` → `join_queue` → `join_room` (resuming with `last_ack_tick`) → `input` (carrying `ack_tick`) / `ping`.  
Server: `welcome` / `queued` / `match_found` / `redirect` / `snapshot` / `pong` / `error`.

**`Hello.protocol_version`.** Checked before anything else the client says is believed, and answered with `ERROR_CODE_VERSION_MISMATCH` — a reason the client can show the player, instead of an incompatible build desyncing halfway into a match. Newer is refused as firmly as older: a client built against a later contract may send fields this build silently ignores, and silently ignoring an input is worse than refusing the connection. A client that sends nothing is from before the field existed and is read as version 1.

There is no cheaper time to add this than before there is a fleet of old clients to stay compatible with.

## Environment

Config lives in **`backend.env`** — `make env` copies it from the committed
`backend.env.example`, whose defaults are the production-safe ones.
`docker-compose.yml` reads it; the service's own `environment:` block holds only
topology (`REDIS_ADDR`, `KAFKA_BROKERS`, `PUBLIC_ADDR`), which wins over the file.

Load testing needs several limits relaxed, and those live in
**`backend.loadtest.env`**, layered on top — never by editing the base file:

```sh
make load-docker        # stack up with the load-test layer applied
make load-10k           # 10k arena bots
make load-turn-10k      # 10k turn-based bots
make load-docker-down
```

Every bot dials from one client IP, so the per-IP limiters cap the **whole
fleet**: left on, a 10k run reports `connections_ok=60` — exactly
`HELLO_RATE_LIMIT` — and reads like the server buckling. That is the only reason
those limits are zero in the load-test file, and `ENV=production` logs a warning
for each limiter left at 0 so the setting cannot ride quietly into a deployment.

| Variable | Default | Meaning |
|---|---|---|
| `ROLE` | `all` | `all` / `gateway` / `matchmaker` / `gameserver` |
| `TICK_RATE` | `20` | 20 / 30 / 60 |
| `UDP_ADDR` | `:8082` | empty disables UDP — and now actually does: an explicitly blank value used to fall back to the default, so a process told not to open a UDP socket opened one, then failed to bind and exited if the port was taken |
| `TURN_LIMIT` | `20s` | per-turn limit in turn-based mode |
| `TURN_RATE_LIMIT` | `120` | turn-based requests per minute per IP |
| `TURN_ENDED_TTL` | `30m` | how long a finished match stays in Redis; the knob that sets the memory bill |
| `TURN_LIVE_TTL` | `24h` | backstop for abandoned matches; the deadline sweeper normally ends them first |
| `WEB_DIR` | auto-detected | frontend directory (`web` or `../web`) |
| `ROOM_SIZE` | `8` | players + bots per match, capped at `config.MaxRoomSize` |
| `MIN_PLAYERS` | `1` | timeout fill threshold |
| `REDIS_ADDR` | empty | set it to go distributed |
| `KAFKA_BROKERS` | empty | set it to publish events |
| `INPUT_BUFFER` | `2` | ticks of input held in reserve per player, absorbing jitter |
| `SNAPSHOT_RATE` | `0` | snapshots per second, independent of `TICK_RATE`; 0 sends one per tick |
| `WARMUP_TIMEOUT` | `5s` | how long a room waits for its seats before the match clock starts; 0 starts at once |
| `MAX_ROOMS` | `0` | matches this node will host; 0 declares no ceiling, and placement then never refuses |
| `REDIS_TIMEOUT` | `1s` | ceiling on one coordination call made on a player's behalf |
| `CONN_MSG_RATE` | `120` | messages per second **per connection** (0 disables) |
| `CONN_BYTE_RATE` | `131072` | bytes per second per connection (0 disables) |
| `SKILL_WINDOW` | `200` | rating spread a match may open with; 0 is pure FIFO |
| `SKILL_WIDEN` | `100` | how much that window grows per second of waiting |
| `SKILL_MAX_WINDOW` | `1200` | cap on the widening |
| `DISCONNECT_GRACE` | `15s` | reconnect window, clamped to `config.MaxDisconnectGrace` — past that the presence record expires first and the extra window does nothing |
| `PLATFORM_DSN` | empty | empty = platform tier off; `memory` = on with no database; a postgres URL = on and durable |
| `PLATFORM_MAX_CONNS` | `16` | pool size **per replica** — it multiplies by the fleet |
| `PLATFORM_TIMEOUT` | `2s` | ceiling on one database call; wider than `REDIS_TIMEOUT` because a purchase is a four-table transaction |
| `PLATFORM_RATE_LIMIT` | `60` | `/platform/*` requests per minute per IP |
| `REPLAY_DIR` | empty | set it to record matches |
| `MAX_REPLAYS` | `64` | how many matches record at once — recording is sampled, not wholesale |

`ENV=production` **refuses to start without `JWT_SECRET`.** The rate limiters only warn when switched off, because there are real deployments that terminate abuse upstream; authentication is not like that. With the secret unset the server accepts any name from anyone and hands out a session on the spot, and there is no deployment where that is the intent. A warning would ship; a failed boot is a rollback.

Hot reload, no restart: `GET|PUT /admin/config` — `tick_rate`, `room_size`, `min_players`, `queue_timeout_ms`, `match_seconds`, `disconnect_grace_ms`, `skill_window`, `skill_widen`, `skill_max_window`. The skill knobs live there rather than in the static config because the right width depends on how many people are queueing *right now*, which is the one thing a build cannot know. Rooms already running keep the tick rate they were created with. Room size is clamped on every path, including this one, so a config push cannot walk past what fits in one datagram — see "What is deliberately missing" below.

## Code map

| Package | Responsibility |
|---|---|
| `internal/sim` | deterministic physics / combat |
| `internal/room` | actor tick loop + delta snapshots (`delta.go`) |
| `internal/turn` | turn-based mode: event log, seq cursor, deadline scheduler |
| `internal/matchmaking` | queue + Lua form |
| `internal/cluster` | GS registry |
| `internal/placement` | job queue → GS |
| `internal/notify` | match_found fanout |
| `internal/presence` | online status |
| `internal/events` | Kafka / log bus |
| `internal/net/ws`, `net/tcp`, `net/udp` | transports |
| `internal/rating` | Elo: read on queue, written on match end |
| `internal/platform` | accounts, wallet, inventory, store, match history, ladder — Postgres and its memory twin, plus the node port test for `web/platform.js` |
| `internal/replay` | seed + inputs, and the checksum that verifies them |
| `internal/safe` | panic containment: one room dies, not the process |
| `internal/app` | wiring + shutdown |

The whole design in one line: the simulation is single-threaded per room, network threads only push commands, and you scale by adding game servers and allocating stickily — never by sharing a mutable world.

## Turn-based mode (sync family C)

Same gateway, different match model. The game: three cards each, players alternate, the higher card takes the trick.

**The cursor is `seq`**, not a tick. The client holds a `seq`, calls `TURN_SYNC{since_seq}`, and the server returns the events in between. A cursor that falls outside the `LogWindow=500` window gets `full_resync` — Telegram's `updates.tooLong`.

**Hidden information.** The log holds the whole truth; what goes on the wire is **a projection for one viewer**. From the same `DEALT` event you see your three cards and your opponent sees only `hand_count: 3`. This is why turn-based cannot broadcast one shared buffer — like family D, but for informational rather than spatial reasons.

**Timeouts.** With no tick loop, nothing is counting down. Deadlines live in a ZSET and a worker pops them **atomically in Lua** — the same technique as matchmaking's `formMatchLua`, because two replicas seeing one expired match would auto-play it twice.

The auto-action is the **lowest card**, deliberately the worst legal move. A fallback that played well would make walking away a winning strategy; it is the same reasoning as poker's auto-fold.

**Two real bugs the tests caught while this was being built:**

1. *A timeout on turn N consumed turn N+1 as well.* A player moves just before the buzzer, the old deadline fires late, the sweeper reads the new state and auto-plays for the next player — who loses a card seconds into a timer that had barely started. Fix: **a deadline carries the turn it was armed for**, and is ignored if that turn has passed.
2. *Phantom events in the log.* Appending before the compare-and-swap meant a move that lost the race still left events behind. Fix: append only after the CAS wins.

**The dual-write window is closed.** State and log started out as two separate writes — crash in between and the state advances while the log is missing an event. The fix was not an outbox but **collapsing the two into one store**: `Store.Commit` writes state and events inside **a single Lua script**. Redis runs a script to completion with nothing interleaved, so "compare the version, write the state, append the events" is one indivisible step.

The more general lesson: if two data stores must be written together, either merge them or accept an outbox. Do not pretend the problem is not there.

### What production needed

**Player ids are account/session strings, not numbers the gateway mints.** Two gateways counting independently hand the same id to two different people, and with a shared Redis store they end up in one match. That is a correctness bug in multi-node mode, not a tidiness preference.

**Redis keys have TTLs.** `turn:state:*`, `turn:log:*` and `turn:seq:*` previously **never expired** — every match ever played stayed forever. Live matches now keep 24 h (long enough to reconnect), finished ones 30 minutes. All three keys expire **together** in the same script: a surviving cursor pointing into a log that is gone would report a permanent gap. The in-memory implementation evicts on the same TTLs, done inline on writes rather than by feeding a janitor goroutine.

**`turnHub` cleans up on disconnect.** The map used to only grow, and the `waiting` slot still pointed at someone who had left — the next arrival was paired with a ghost.

**Rate limiting on turn messages.** `TURN_SYNC` reads a range out of the event log: cheap to send, not cheap to serve.

```bash
cd backend && go test ./internal/turn -v
```

## Platform tier (accounts, wallet, store, ladder)

Everything above this line dies with the process, and is supposed to. A room
lives in one goroutine's RAM, presence expires in 45 seconds, a turn match is
gone half an hour after it ends. None of it is a record of anything — it can all
be rebuilt by playing again.

An account cannot. Neither can what a player was awarded, what they paid for, or
where they stand on a ladder. `internal/platform` is where those live: one
relational schema, one transaction per fact, and a durable id the gateway then
carries around in a token.

```sh
make run-platform    # PLATFORM_DSN=memory — everything works, nothing survives
make docker          # PLATFORM_DSN=postgres://… — the compose stack
# then open http://localhost:8080/platform.html
```

`PLATFORM_DSN` decides the whole mode, the way `REDIS_ADDR` does:

| value | meaning |
|---|---|
| empty | off. `/auth/login` hands a token to whoever asks, as it did before this existed |
| `memory` | on, no database. Every rule enforced, nothing durable. `ENV=production` says so loudly on startup |
| a postgres URL | on, durable |

A DSN that cannot be parsed fails the boot in milliseconds rather than after the
six-second connection budget. The distinction is `platform.ErrBadDSN`, and it
exists because the two failures want opposite treatment: a database that is not
up yet is worth waiting for — `docker compose` starts this process and its
database at once — and a typo never will be. Retrying both means the process
spends its whole startup budget on a string that was never going to work and
then reports a timeout instead of the typo.

### The seam

The realtime tier touches this in exactly three places, which is the whole
reason it is a package and not SQL sprinkled through the gateway:

1. **Login** mints a token for an account id that already existed, instead of a
   fresh random one. `auth.JWT.IssueFor` is that split — `Issue` invents an id,
   and inventing identities is the bug this repo already spent effort removing.
2. **A queue join** reads that account's rating.
3. **A match ending** writes the result back.

Everything else is HTTP, off the tick path entirely. The two direct calls are
the ones that must never become network hops a player's match waits on.

The rating read is the seam the earlier draft of this README promised and did
not use: *"`rating.Store` is an interface with two implementations, and a third
backed by a real database changes one constructor."* `platform.Ratings` is that
third, and it is one constructor. With it in place the Redis `mm:rating` hash
goes unused — a 30-day cache and a record of the same number are two sources of
truth, and the cache wins every time they disagree.

What it costs: one primary-key read per queue join instead of one `HGET`. That
is off the tick path and inside `REDIS_TIMEOUT`, the same budget every other
lookup on the connection path carries — one rule there is worth more than a knob,
and the wider `PLATFORM_TIMEOUT` is for the write, which is a transaction. It is still the first thing to cache if the join rate ever justified
it — Redis in front, the database as the record — and the shape of that cache is
exactly the `rating.Store` it replaced.

### The schema

Five tables, in `internal/platform/schema.sql`, applied on startup with
`IF NOT EXISTS`. That is honest for a demo and wrong for a product: the first
column that has to change type is the day this wants `golang-migrate` and a
versioned history.

| Table | Holds | The one idea in it |
|---|---|---|
| `accounts` | identity, credentials | `username_key` is `lower(username)` and carries the UNIQUE — a key that depends on shift is a support ticket |
| `profiles` | rating, record, wallet | `CHECK (currency >= 0)`, and a partial index on `(rating DESC, account_id)` for the ladder |
| `inventory` | what a player owns | keyed `(account_id, item_id)`; never joined against the catalog, so an item can leave the store without taking anybody's copy |
| `ledger` | every currency/item movement | keyed `(account_id, idem_key)` — see below |
| `match_results` | one row per player per match | keyed `(match_id, account_id)`; it is both the history a player reads *and* the idempotency key the writer relies on |

The catalog is **in code**, not in a table. It is configuration that ships with a
build: read on every purchase, changed by a deploy. In the database it would buy
a join on the hot path and a migration every time a price moves.

### Idempotency, which is most of what this tier is

Every write is retryable, because every caller can crash between sending a
request and learning what happened.

**A client-chosen key on a purchase, scoped to the account.** The scoping is not
tidiness. Clients pick keys independently, so stored bare, one player's `buy-1`
collides with another's and the second player's purchase is swallowed as a
duplicate — no charge, no item, no error to look at. `internal/turn` learned
exactly this with move keys; the ledger's primary key is `(account_id, idem_key)`
for the same reason. `TestIdempotencyKeysAreScopedToTheAccount` pins it.

**The server-chosen match id on a result.** A placement job is redelivered when
an ack is slow; a turn match can be ended by the matchmaker's deadline sweeper
after a gateway already tried. Keying on `(match_id, account_id)` rather than on
the match alone is what makes a *partial* write completable: a writer that died
between two players' rows retries and writes exactly the one that is missing.

Four properties the tests hold down, each because the obvious implementation
gets it wrong:

- **A refused purchase does not burn the key.** The whole transaction rolls
  back, key included, so the same request can succeed once the wallet can
  afford it.
- **The balance check is in the `UPDATE`'s `WHERE` clause**, never a read
  followed by a write. Read-modify-write is the classic lost update: two
  requests both read 100, both decide 60 is affordable, both write 40.
  `TestConcurrentSpendingCannotOverdraw` races twelve purchases at a balance
  that affords three.
- **A rating is applied as a delta, not assigned.** The number in a result was
  read when the match started and is written when it ends, minutes later —
  assigning it silently undoes anything that finished in between. `GREATEST(
  rating + delta, floor)` composes instead, and it is what lets the turn-based
  mode record a match with a delta of zero.
- **A write cut off half-way leaves nothing behind.** `PLATFORM_TIMEOUT` is a
  deadline on exactly these calls and a loaded database is exactly when it
  fires, so the interrupted case is a normal one. The outcome that must not
  exist is half a purchase — a claimed idempotency key with no charge behind it
  — because the retry is then refused as a duplicate and the player has paid
  nothing, owns nothing, and sees no error.
  `TestAPurchaseCutOffMidTransactionLeavesNothingBehind` forces it
  deterministically: another connection holds a row lock, the guarded `UPDATE`
  blocks behind it, and the deadline fires with the ledger row already inserted.
  Both it and the match-result twin were checked by taking the transaction out
  and watching them fail.

### Two modes, one ladder

The arena moves ratings. The turn-based mode records its matches and pays for
them and **moves no rating at all**: one number cannot rank two different games,
and feeding a randomly-paired card game into the ladder that skill matchmaking
widens its search around would make that search a search around noise. A real
product gives each mode its own ladder, which here is a column and a key — the
turn-based mode does not have skill matchmaking to spend one on yet.

A match filled with bots pays nothing and moves nothing, for the reason it
already recorded no rating: there is no opponent to measure against, and a room
the matchmaker filled after a queue timeout would otherwise be the cheapest
currency in the game.

### HTTP surface

| Endpoint | Auth | |
|---|---|---|
| `POST /auth/register` | — | creates the account and signs it in, in one round trip |
| `POST /auth/login` | — | `{username, password}`. With the tier on it stops accepting a bare name: there is no anonymous door left, because every read below is authorised by this token |
| `GET /platform/profile` | bearer | account + rating, record, wallet |
| `GET /platform/inventory` | bearer | |
| `GET /platform/history?limit=` | bearer | |
| `POST /platform/store/buy` | bearer | `{item_id, qty, idempotency_key}` |
| `GET /platform/catalog` | — | a price list is the same for everybody, and a player should see what a game sells before signing up |
| `GET /platform/leaderboard?limit=` | — | ranks only accounts that have played: an unplayed 1000 is the absence of a ladder position, not a position on it |

The same JWT the WebSocket handshake accepts. A second credential for the HTTP
API would be a second thing to expire, revoke and get wrong.

Errors map onto status codes the client can act on rather than a generic 400:
`402` for a wallet that cannot cover a purchase — not a malformed request and
not a server fault — `409` for a username taken or a unique cosmetic bought
twice, `401` for bad credentials, and a generic `500` with the detail in the log
for anything else, because handing a client the text of a database error is how
a schema ends up in somebody's browser console.

`web/platform.html` drives all of it, and leaves the session in `localStorage`
where the two game pages pick it up — sign in there, play, and the match is in
your history.

Three client rules live in `web/platform.js` rather than inline in the page,
because each fails quietly in a browser and none of the Go tests would notice:

- **the idempotency key is minted once per attempt and kept on failure.** This
  is the half the server cannot enforce — it cannot tell a retry from a second
  purchase without being told. A key per *request* makes a double-click two
  purchases; a fresh key after a failure double-charges on the one case that
  matters, a response lost in flight where the purchase actually landed.
- **`expires_at` is seconds and `Date.now()` is milliseconds.** Compare them
  directly and every session looks expired, or none ever does and the page keeps
  presenting a token the server will refuse.
- **a rating that did not move renders as a dash, not `+0`.** Zero means "this
  mode has no ladder", not "you drew against it".

`internal/platform/webclient_port_test.go` runs the module under node and pins
all three from Go, the same arrangement as `cursor_port_test.go` and
`predict_port_test.go`. Each case was checked by breaking the JS and watching
the Go test fail.

### Why not Redis, which is already there

Every structure in Redis here carries a TTL because everything in it is derived.
An account is not derived from anything. What this needs is what a relational
store is for: a transaction spanning the wallet and the inventory, a uniqueness
constraint on a username, a check constraint that a balance cannot go negative,
and an index that answers a leaderboard without walking the player base.

### It is one process here, and would not be

The handlers live on the gateway's own mux, which is a demo's convenience.
Accounts and a store scale on request rate; a game server scales on tick budget;
pinning them to one deployment means scaling whichever is cheaper by whichever
is dearer. Splitting them is a small job precisely because `platform.Service`
knows nothing about HTTP, sessions or rooms — but the two direct calls above
would then be network hops, and *those* are the ones that must not be.

There is deliberately no `postgres.yaml` next to `redis.yaml` in the k8s base.
Redis there is a cache with a TTL on every key, so a single-replica StatefulSet
with a 1 Gi volume is an honest way to run it. An account store is the opposite,
and a single replica with no backups, no failover and no point-in-time recovery
is not a way to run one. `PLATFORM_DSN` lives in the Secret and points at a
managed instance.

### What this tier still does not have

- **An outbox.** `recordToPlatform` logs and gives up if the write fails. Same
  shape as the Kafka publishes below, now in a second place.
- **Refunds, chargebacks, real money.** The ledger records movements and has no
  notion of reversing one, which is the entire subject of a payments system.
- **Seasons, ladder decay, per-mode ratings.** All three are a column and a
  reset job on top of what is here.
- **Password reset, email, MFA.** No mail path exists, and an account-recovery
  flow without one is a support queue.
- **A work factor that meets the current guidance.** PBKDF2-HMAC-SHA256 at 64k
  iterations, against OWASP's 600k, because a demo whose login takes a third of
  a second is a demo nobody finishes. The cost is stored per credential, so
  raising it is a rehash on next login and not a migration. A product picks
  argon2id and pays for the module.

```bash
make test           # the memory half of the conformance suite
make test-platform  # starts a throwaway Postgres and runs both halves
```

## Testing the Redis layer

Every Redis implementation has an in-memory twin, selected by exactly one environment variable. Previously **none of the Redis implementations had tests** — not even `formMatchLua`, which this README makes a point of. They now run against `miniredis` (in-process, no Docker), and most tests run **both implementations through the same assertions**:

```go
func forEachQueue(t *testing.T, fn func(t *testing.T, q Queue)) {
    t.Run("memory", func(t *testing.T) { fn(t, NewMemory()) })
    t.Run("redis",  func(t *testing.T) { fn(t, NewRedis(redistest.Client(t))) })
}
```

That caught a real divergence immediately: `placement.Take` on an empty queue blocked for 2 s and returned `nil` on Redis, and blocked **forever** in memory. It caused no live bug because `jobLoop` handled both, but the interface documented neither, so the next caller would have been caught out. The two were reconciled as `TakeWait` and written into the interface.

It caught a second one while the skill window was being added. Redis orders a sorted set by score and then by member, so two players who queued in the same millisecond come back in id order — while the in-memory queue appended in arrival order. Different anchor, different match, and only ever in production. `MemoryQueue.Enqueue` now inserts in `(queued_at, conn_id)` order, which is Redis's comparison written out in Go.

`internal/platform` follows the same rule with one difference it cannot avoid:
there is no in-process Postgres the way there is an in-process Redis. So
`eachStore` runs the memory half always and the Postgres half only when
`PLATFORM_TEST_DSN` is set, and `make test-platform` starts a throwaway database
so the skip is a convenience rather than a hole. Both halves run every case,
including the twelve-way overdraw race — the one place where "the memory twin
enforces the same rule" has to mean under contention, not just in sequence.

## Fuzzing the decoders

Two functions take bytes straight off the internet: `protocol.UnmarshalEnv`, which every transport funnels into, and `room.ApplyDelta`, which is the client half of the delta scheme (the browser and the load bot run the same algorithm). Table tests only cover the shapes someone thought of.

```bash
make fuzz    # 60s each; CI runs the seed corpora with the normal test pass
```

The property is not "it parses" — most inputs are not valid protobuf and should be rejected. It is that **nothing panics**, and that what does parse survives a round trip. A panic in `UnmarshalEnv` is a remote crash of the gateway, which is the exact class of bug `internal/safe` now contains rather than fixes.

## What is deliberately missing, and why

**Interest management / AOI.** At 8 players per room this is speculative complexity. It earns its place only when a match holds many entities and each player sees a fraction of them — that is when family D starts to apply. Adding it now buys nothing and costs per-client encoding CPU.

The threshold is not guessed but **measured**, with tests guarding it (`go test ./internal/room -run SnapshotSize -v`):

| players | full/tick | delta/tick | B/player | egress/room |
|---:|---:|---:|---:|---:|
| 8 | 289 B | 225 B | 36 | 45 KB/s |
| 16 | 621 B | 499 B | 39 | 194 KB/s |
| 24 | 934 B | 757 B | 39 | 437 KB/s |
| **32** | **1296 B** | 1076 B | 40 | 810 KB/s |
| 64 | 2531 B | 2074 B | 40 | 3.2 MB/s |
| 100 | 3953 B | 3222 B | 40 | 7.7 MB/s |

**AOI becomes mandatory somewhere around 28–30 players** — where a snapshot outgrows `MaxDatagram`'s 1200 B. That is a hard wall, not a bandwidth economy: past it the UDP transport **drops rather than fragments**.

`config.MaxRoomSize` sits below that wall and is enforced on every path into the configuration, environment and hot push alike, so the wall cannot be crossed by editing a number at runtime. Raising it is an interest-management project, not a config change.

Notice the last column grows **quadratically**: every client receives everyone's state, multiplied by the number of clients. That, in the end, is what forces AOI.

Two tests guard this:

- `TestSnapshotFitsOneDatagramAtDefaultRoomSize` — fails if anyone raises `ROOM_SIZE` past the point a snapshot still fits in one packet.
- `TestSnapshotSizeByRoomSize` — fails if a new field on `PlayerSnap` drags the threshold below 24 players.

For reference: competitive shooters almost never go past 12 players (Valorant, CS2, LoL and Dota all run 10) and spend the budget on tick rate rather than entity count. Every 64–150 player game — Battlefield, Apex, Fortnite, Warzone — **requires** AOI.

**The platform tier — no longer missing, and worth saying what changed.** This
section used to read: no database, no accounts, no profile, inventory, currency,
store, leaderboard or match history; `/auth/login` issues a token to whoever asks
and everything dies with the process. It called that a boundary rather than an
oversight, and predicted the shape of the other half — "a Postgres schema, a set
of CRUD services and a long argument about idempotency."

That half is now built: [the platform tier](#platform-tier-accounts-wallet-store-ladder).
The prediction held up, including the proportions — the idempotency argument is
most of the code, and none of it taught anything about netcode. What it did
teach is where the two tiers actually touch, which turned out to be three calls
and not a layer: an id in a token, a rating read, a result written.

`PLATFORM_DSN` empty restores every word of the old paragraph exactly, which is
the property that made it safe to add.

**Regions.** One Redis, one pool, one `PUBLIC_ADDR`. No ping-based datacenter selection, no per-region queue. Global play needs it, and it is an architecture decision rather than a feature — it shards matchmaking and placement both — so it is called out here rather than half-built.

**Anti-cheat beyond server authority.** The server simulates everything and clamps every input, which is necessary and not sufficient. Statistical detection, a telemetry pipeline, report and review flows: none of that is here. The hooks are, though — Kafka carries match lifecycle off the hot path, and replays make a suspicious match re-runnable from the inputs the server actually accepted.

**Encryption on UDP.** The address cookie proves a sender can receive where it claims, which stops the reflector attack; it does nothing about reading or tampering with datagrams in flight. That wants DTLS 1.3 or a per-packet AEAD keyed from the handshake. WS and TCP are expected to be terminated by a proxy holding the certificate.

**Social and moderation.** Friends, parties, chat, voice, mute, report. All of it is a service alongside this one rather than inside it. It needed a durable identity to hang off, which now exists — a friend list is a table keyed on two account ids — so what is left is the service, not the prerequisite.

**At-least-once delivery for match events.** `match.started` / `match.ended` go to Kafka asynchronously and a broker outage loses them. That is a deliberate half-measure and it is worth being precise about which half.

What is fixed: the accounting no longer lies. The four call sites used to discard the error, and two of them then incremented `arena_events_published_total` — so a broker refusing every write produced a rising success graph and no log line. Delivery is now counted where the asynchronous result actually arrives, in the writer's completion callback, and `arena_events_dropped_total` says what did not make it.

What is not fixed: the events are still lost, just audibly. The real answer is the one this repo's own turn-based notes insist on — *if two stores must be written together, either merge them or accept an outbox; do not pretend the problem is not there*. Here they cannot be merged: the match ends in one process's RAM and the event belongs in Kafka. So it wants an outbox — the room end writes the event to Redis in the same step it unregisters the room, and a relay drains it to Kafka and deletes on success, keyed by `room_id` so a duplicate delivery is a no-op.

It is not built because nothing downstream consumes these events. That is still
true after the platform tier, and the reason is worth being precise about: a
reward *does* now hang off a match ending, but off the room's own `OnEnd`
callback and into a transaction, not off the Kafka event. The two tiers are
joined by a function call, so the event bus still has no consumer to lose
anything.

What the platform tier did add is a second instance of the same unfinished
thought. `recordToPlatform` logs and gives up if the database write fails; it
does not retry, because a retry loop there would hold a room's end-of-match
goroutine open against a database that is already struggling. The write is
idempotent rather than recoverable, which is exactly the precondition an outbox
needs — and the shape of the placement job queue, `LPUSH` / `BRPOP` / ack, is
still the shape both of them should take.

**Tracing and log correlation.** Prometheus is wired up and the logs are plain `log.Printf` lines with no request or match id threaded through them. Following one match across gateway, matchmaker and game server means grepping three processes for a room id that only some lines carry. OpenTelemetry spans across those three hops, and a structured logger with the room and player id attached, is the next honest step in observability — and it is a refactor of every log line, which is why it is called out rather than half-applied.

**Chaos and soak testing.** The suite covers units, races, goroutine leaks and the two decoders under fuzzing. It does not kill Redis mid-match, partition a game server from the matchmaker, or run for six hours watching for drift. Those find a different class of bug, and they need infrastructure this repo does not have.

**Consumers for the analytics stream.** Kafka carries the events; nothing reads them. There is no data warehouse, no funnel, no retention or DAU reporting. A game is a data business and this is the part of it that is missing, along with the schema-evolution policy that a topic with real consumers would need.
