# Design notes

Why the repo is shaped the way it is, and where each decision lives in the code.

## Designing for 10k CCU

Do not put 10k players behind one lock. Shard by room. The gateway is stateless (connections and routing). The game server is stateful per match and scales by room count. Redis is for the directory, never for physics.

The arithmetic: 10k connections × 20 inputs/s × ~40 B is roughly 8 MB/s inbound. Outbound is the side that bites — a snapshot for 8 players at 20 Hz across 10k connections is on the order of 100 MB/s if nothing is compressed. That is why the wire is binary protobuf with delta encoding, and why interest management becomes mandatory once rooms get large (see the measured table in the README).

## Tick loop, not request/response

Gameplay is not RPC. Fixed timestep: accumulate input, step, replicate. The parts worth knowing are the spiral of death, how catch-up is handled, and why sending a snapshot must never block on `conn.Write`.

Code: `internal/room/room.go`, `Run`.

## No floats

Cross-platform replay, checksums and desync debugging all need bit-identical arithmetic. Integer `Milli`, a seeded RNG, players sorted by id. Test: `TestDeterministicReplay`.

## Matchmaking races

Two matchmakers popping the same queue. Redis Lua makes the pop atomic; the in-memory implementation uses one mutex. A timeout fills bots so a solo player still gets a match and the queue does not stall for want of people.

## Disconnects mid-match

Unsubscribe, and take the avatar out of the world. The simulation keeps running — one disconnect does not kill the room — but the player leaving it does not stay in it.

Letting the avatar idle was the obvious thing and it was wrong in a way that only showed up once match results reached a ladder: a motionless body at full HP is worth a kill every `RespawnTicks` to anybody who shoots it, and `recordResult` writes those kills into Elo without being able to tell them from real ones. So the avatar **lingers for three seconds and then despawns**. The linger is deliberate: despawning instantly would make pulling the network cable the cheapest dodge in the game, because the shot already in the air would find nothing.

The departed player is still rated for the match. Leaving must not be an escape either.

And once *every* human has despawned the match ends rather than letting the bots run out the clock: the room is counted against the node's ceiling for as long as it lives, so a match nobody is in is capacity taken from matches somebody is.

Reconnecting within the grace period gets the seat back, because presence recorded the room, the seat and the last acknowledged tick — and `Rejoin` puts the avatar back, in place if the linger has not run out, through an ordinary respawn if it has.

None of this is derivable from the recorded inputs, so the replay format carries roster events from v2 on. Otherwise every replay of a match somebody left re-simulates a different world and reports a desync that never happened.

## Events over a state channel

A snapshot replicates state, and state cannot carry a discrete fact: two hits inside one delta window are one HP change, and a death plus a respawn is none at all. Hitmarkers, killfeeds and damage numbers all need the fact, not the state.

The usual answer is a reliable sub-channel with its own acks and retransmits. This does not need one, because the delta window already *is* the right window: a delta is encoded against a tick the client acknowledged, so carrying every event since that tick makes loss cost bandwidth and nothing else — the same property the state half already had.

Two things fall out of that and both matter. The span has to be **capped**, because it is the one part of a message whose size a client can grow by going quiet, and a message over `udp.MaxDatagram` is dropped rather than fragmented — costing the state as well as the events. And the client has to consume **by tick**, because its ack is a round trip behind and the same events arrive again until it lands; deduplicating on the event's fields would collapse two real hits that landed on one target in one tick.

## Scaling a match that has already started

You do not. A match is bound to one game server. Scale the **number of matches**, and do not split a single match unless you are building a seamless-world MMO, which is a different problem. Migration costs state transfer plus a reconnect, so it is only worth it when draining a node.

## Transport choice

Browsers get WebSocket. Dedicated clients and consoles get TCP or UDP. UDP is the one a shooter actually wants: unreliable snapshots, with reliability added only where it is needed. All three carry the same protobuf envelopes; `internal/net/udp` deals with the three things UDP changes — no framing, no connection, no delivery guarantee.

## Where Kafka sits

Off the hot path. Match started and ended, for analytics, economy, offline anti-cheat and match history. Keyed by `room_id` to preserve per-match order.

## Graceful shutdown under Kubernetes

`preStop` sleep plus SIGTERM, with readiness failing first so the pod stops receiving new traffic. Agones has dedicated `Allocated` / `Ready` / `Shutdown` states for game pods; this demo approximates them with `/readyz` and a Redis heartbeat.

## What breaks first at 10k

File descriptors, goroutines per connection, GC pressure from per-snapshot allocation, and slow-client buffers. What is already mitigated in the code: snapshots are dropped rather than queued without bound, buffers are small, pprof is wired up, and delta encoding is shared between clients on the same baseline so per-tick allocation stays linear in room size. The next steps would be object pooling, netpoll (gnet) and `SO_REUSEPORT` gateways.

## Five things the shape buys you

1. Room isolation is both the unit of parallelism and the unit of failure.
2. Latest-wins beats a queue of death.
3. The tick budget is an SLO; an overrun is worth paging for.
4. Redis for coordination, memory for simulation.
5. Least-loaded placement plus sticky reconnect gives horizontal scale without proxying snapshots.

## Choosing a cursor, not a mechanism

"How do you synchronise client state" has no single answer — the answer is **which cursor you pick**. Two families run side by side here:

| | `internal/room` | `internal/turn` |
|---|---|---|
| Cursor | `ack_tick` | `seq` |
| What is sent | a delta against the tick the client acknowledged | events since the cursor |
| Escape hatch | full snapshot (`baseline_tick=0`) | `full_resync` |

The point that matters: **every delta scheme needs an escape hatch**, and both families here have one.

## How delta snapshots work

A 64-tick ring per room, a per-client acknowledgement, `diff(ring[ack], now)`. Four details that only show up once you build it:

1. **The baseline only advances on an acknowledgement.** Packet loss makes the next delta bigger; it can never desync. No retransmission needed.
2. **A bitmask for zero values.** proto3 elides zeros from the wire; without the mask a client cannot tell "moved to x=0" from "did not move".
3. **A stale acknowledgement wedges the pair.** A reconnecting client has thrown its history away, an old acknowledgement still in flight arrives, and the server encodes from a tick the client cannot reconstruct — permanently stuck, with nothing reporting an error. `subscriber.since` blocks it.
4. **Deltas are shareable more often than they look.** Clients diverge only transiently, after a dropped packet; in the steady state they all sit on the same acknowledged tick and are owed identical bytes. Encoding once per distinct baseline instead of once per client is the difference between quadratic and linear cost in room size.

Measured: a delta is about **80% of a full snapshot** in deathmatch — a small saving, because everyone moves every tick. The real number is worth more than a promised 10×.

## Client prediction

`web/predict.js` ports `applyInputs` to JavaScript; `PlayerSnap.seq` says which input the server has processed, and the client replays everything after it.

The biggest risk is not the algorithm but **the port drifting from the original**. A test runs both Go and node and compares bit for bit, including the `speedTick` constant at 20/30/60 Hz — get that constant wrong and every step is wrong.

## Lag compensation

Hitbox rewind **does not** apply to a projectile that travels for many ticks: there is no single instant to rewind to. This uses projectile catch-up instead, as Overwatch does. Three conditions: the round trip is computed server-side from `ack_tick`, the total is capped, and TTL is charged for the catch-up. The trade-off is stated in the code — the catch-up is swept tick by tick precisely so the jump does not skip collisions.

Knowing *when a technique does not apply* is worth more than being able to name it.

Two off-by-ones lived here, both in the safe direction, which is exactly why they lasted:

1. **Measured against the wrong tick.** `fillPending` runs before `Advance`, and `Advance` increments the world before applying anything, so an input lifted out of the queue is simulated at `lastTick+1`. Measuring its age against `lastTick` cost every client one tick — a fifth to a third of a typical round trip, and proportionally most from the clients with the best connections, since a client acking the freshest snapshot in existence scored zero.
2. **The interpolation term was missing entirely.** Valve writes the compensation as `Server Time - Packet Latency - Client View Interpolation`. Only the first subtraction was here. A client renders others ~100 ms behind the newest snapshot so their motion is smooth between 20 Hz updates; it therefore aimed at a world older than the tick it acked, and got nothing for it.

The second one is the more interesting of the two, because fixing it means accepting a number *from the client* — and this codebase's standing rule is that a client-supplied latency figure is a dial for buying advantage. The resolution is the same one Source reached: take it, but clamp it (`maxInterpMs = 150`, their `sv_client_max_interp_ratio`), and keep the cap on the **sum** rather than per-term so the two cannot add up past `MaxLagCompTicks`. A client that inflates its declared delay buys nothing; one that understates it only loses hits it should have landed.

It crosses the wire in milliseconds rather than ticks because the room's rate is configurable (20/30/60) and a client's render delay is not a function of it — converting client-side would be converting against a guess about the server. The load-test bot sends zero, which is honest rather than an omission: it draws nothing, so it holds no interpolation buffer, and copying the browser's 100 ms would claim a handicap it never takes.

## What is different about turn-based

There is no tick loop, so nothing counts down. Deadlines live in a ZSET and a worker pops them atomically in Lua — the same technique as `formMatchLua`.

Two real bugs the tests caught, both worth recording:

1. **A timeout on turn N consumed turn N+1.** A player moves just before the buzzer, the old deadline fires late, the sweeper reads the new state and auto-plays for the next player. Pinning the turn number inside `ExpireTurn` is *not enough* — it reads the new state too. Fix: the deadline carries the turn it was armed for.
2. **Phantom events.** Appending before the CAS left events behind for a move that lost the race. Fix: append only after the CAS wins. The remaining window, a crash between CAS and append, is closed by putting both in one Lua script.

## UDP, concretely

Three things change and `internal/net/udp` handles all three: the length prefix goes away (a datagram already has a boundary), the server keeps its own peer table and evicts on silence (UDP reports no disconnect), and loss is accepted.

Three safety points that get less attention:

- A packet over the MTU is **dropped, not fragmented** — losing one fragment loses the whole datagram.
- An unknown address must prove it can receive before it gets any state, or the server becomes a **reflector** aimed at a victim the attacker chooses. That is the HMAC cookie, and the padding requirement on the first HELLO that stops the reply being an amplifier.
- One socket means one receive queue. Running handlers on the read loop makes the slowest handler the arrival rate for every client, so each peer gets a bounded inbox and its own goroutine.

## A panic is not allowed to be everyone's problem

The room model says the room is the unit of failure. Go says otherwise unless you do something about it: one unrecovered panic ends the process and every match in it.

`internal/safe` is that something. The parts worth keeping:

- **Recovery is a blast radius limiter, not error handling.** `arena_panics_total{where}` is a paging metric, because the alternative is a bug that now hides.
- **A background loop has to restart, not just survive.** A matchmaker goroutine that dies leaves the queue filling with nobody forming matches, and nothing in the process reports it — the counter ticks once and the server goes quiet.
- **The per-message recover is written out rather than wrapped in a closure.** It runs twenty times a second per connection; the hot path should not allocate to be safe.
- **A room that panics ends like a match that finished.** Same `OnEnd` path, so the directory, the event and the clients all see something they already understand.

## Two kinds of rate limit, for two kinds of attacker

The per-IP windows guard the doors: hello, login, queue. They are keyed by IP because before authentication there is nothing else to key on.

They are useless against a client that is already inside, which can send inputs as fast as its link allows — each one costing a `proto.Unmarshal` before the room drops it. So every connection also carries its own token bucket, charged before the decode, in messages *and* bytes: a flood of tiny inputs and a trickle of 64 KB frames are different attacks and a single counter misses one of them.

Per-connection is also the only fair place to put it. A per-IP limit tight enough to catch one abusive client throws out everyone behind a carrier NAT.

## Skill matchmaking is a widening window, not a threshold

The queue used to stamp `Skill: 1000` on every player, which made skill-based matchmaking a field on the wire. Two halves were missing: a rating that moves (`internal/rating`, Elo, read on queue and written on match end), and a rule for using it.

The rule that works — and the one every shipped matchmaker ends up with — is not a threshold but a trade:

- anchor on **the player who has waited longest**, because they are the one the queue is failing;
- draw the window around their rating, and widen it with their wait;
- cap it, and let the existing bot-fill timeout be the floor under everything.

A fixed window fails in both directions: tight, and the edges of the distribution never play; loose, and it does nothing. A perfect match nobody is in is worth nothing.

Measure it with two metrics or not at all: `arena_match_skill_spread` against `arena_matchmaking_wait_seconds`. Wait time alone says a queue is fast, never whether it is any good.

## Replays are free when the simulation is deterministic

The determinism was already there — fixed point, seeded RNG, sorted rosters. That makes a replay the seed plus the inputs, about 6 KB for a 100-tick match, and it reproduces positions, hits and scores exactly.

Storing inputs rather than outcomes is what makes it evidence. A recording of what the server said happened cannot be used to check the server; a recording of what went in can. `World.Checksum` is stamped in at the end and `cmd/replay` re-simulates and compares — the same desync check a lockstep game runs every tick, applied after the fact.

It is sampled. A 90-second match is ~200 KB held until it is written, and across a thousand rooms that is more memory than the simulations use. Live games sample too.

## Balancing is not refusing

`PickLeastLoaded` answers "which node is least loaded", which is the right question until every node is past what it can simulate. Then it keeps handing matches to whichever server is missing its tick budget by the smallest margin, and the players already in those rooms pay for it.

A declared ceiling (`MAX_ROOMS`) turns it into "spread the load, and stop when there is none left". Refused players go back in the queue with their original `queued_at`, so the pressure shows up as queue wait and as `arena_placements_refused_total` rather than as a silent loss.

The same field carries `draining`, which is what makes a rolling deploy stop cutting matches in half — together with unregistering on SIGTERM instead of waiting out a heartbeat TTL, and with `jobLoop` declining to take new work while the drain runs.

## A version on the wire, before it is needed

`Hello.protocol_version`. Without it there is no way to turn an incompatible client away: it connects, the first message it cannot parse looks like a bug, and the player gets a match that desyncs instead of a message telling them to update.

Newer is refused as firmly as older. A client built against a later contract sends fields this build silently ignores, and silently ignoring an input is worse than refusing the connection.

There is no cheaper moment to add this than before there is a fleet of old clients to stay compatible with.

## Production should refuse, not warn

The rate limiters warn when switched off, because there are deployments that terminate abuse upstream and genuinely want them off. `JWT_SECRET` is not like that: unset, the server accepts any name from anyone and hands out a session on the spot. There is no deployment where that is the intent.

So `ENV=production` without it refuses to boot. A warning ships; a failed boot is a rollback.

## One input per tick, not the last one that arrived

A client at the tick rate sends one input per tick. The server consumes one per tick. It is tempting to conclude that a slot per player is enough, and that is what the room did: `pending[id] = in`, last write wins.

The two rates line up on average and never in practice. Jitter puts two inputs in one window and none in the next, so the room threw away a frame of movement it had already received, and then had nothing to simulate for the frame after. The client predicted moving through both and got corrected backwards. The player blames their connection.

A queue per player, drawn one deep per tick, spends the crowded window's surplus on the empty one. What makes it a buffer rather than a delay:

- **Nothing is held when there is nothing to absorb** — a steady stream drains every tick, so the no-jitter case pays no latency.
- **A client running ahead is trimmed, not queued.** A fast clock would build a backlog that never drains, and each of its inputs would be simulated further into the past. Dropping the oldest keeps that player in the present at the cost of frames, which is the trade a player would make.
- **Lag compensation is measured at simulation time, not arrival time** — and against the tick about to be simulated, not the last one broadcast. An input held a tick was produced a tick further back and is compensated as such — still capped, because a full queue must not buy what refusing to acknowledge cannot.

Measured on loopback, where a Go ticker is the only jitter: 132 of 65,499 inputs dropped at depth 1, none at depth 2, and the underrun count moving in lockstep with the drops. Loopback is the floor. The interesting part is not the size of the number but its shape — every dropped input pairs with a tick that had none.

## The durable half is arranged around a transaction, not a deadline

Everything else in this file is paced by a clock — a tick budget, a TTL, a grace
window, a widening search. The platform tier (`internal/platform`) is not. An
account, a wallet and a ladder position cannot expire and cannot be recomputed
from the world, which rules out every store the realtime tier uses and makes the
unit of correctness a transaction rather than an interval.

Two calls join the halves: a rating read when a player queues, and a result
written when their match ends. Both are off the tick path. The read is bounded
by `REDIS_TIMEOUT`, like everything else on the connection path; the write by the
wider `PLATFORM_TIMEOUT`, and on a *fresh* deadline rather than the room's —
cancelling it on the Redis budget would lose a match that was played in full.

The part worth reading the code for is idempotency, because it is most of the
package and every one of its rules exists because the obvious version is wrong:
a client-chosen key scoped to the account (`internal/turn` learned the same
lesson with move keys), a result keyed `(match_id, account_id)` so a half-written
match can be completed, a balance check inside the `UPDATE` rather than before
it, and a rating applied as a delta so two overlapping matches compose instead of
clobbering. Tests: `TestIdempotencyKeysAreScopedToTheAccount`,
`TestRecordMatchCompletesAPartialWrite`, `TestConcurrentSpendingCannotOverdraw`,
`TestRatingsComposeAcrossOverlappingMatches`.

The other half of the same property is what happens when a write is cut off
part-way. `PLATFORM_TIMEOUT` is a deadline on these calls and a loaded database
is when it fires, so "interrupted" is a normal path, not an error path — and the
state that must never exist is half a purchase, a claimed idempotency key with
no charge behind it, because the retry is then refused as a duplicate and
nothing anywhere says so. The tests force it with a row lock held from another
connection rather than by racing a short timer, so the transaction is always cut
at the same statement.

Startup draws the same distinction in the opposite direction: a database that is
not up yet is worth waiting six seconds for, and a malformed DSN never will be.
`platform.ErrBadDSN` is what separates them, so a typo fails the boot at once
with a message about the string instead of a timeout.

Code: `internal/platform`, `internal/app/platform.go`. Off by default —
`PLATFORM_DSN` empty leaves the realtime tier exactly as it was.
