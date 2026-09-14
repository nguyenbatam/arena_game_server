# Architecture

## Why rooms, not one big world

10k CCU does not mean 10k players in one simulation. Arenas, MOBAs and battle royales all **shard by match**. This demo: 8 players per room, so 10k CCU is about 1250 rooms. One goroutine per room, no shared mutable state.

The gateway holds connections. The game server holds the simulation. The matchmaker holds neither.

## Authoritative server

The client does not send positions. It sends intent (`mx,my,fire,aim`). The server integrates per tick. The snapshot is the source of truth. Anti-cheat lives at this layer: out-of-range input is ignored (`clampDir`).

## Tick

Budget is `1 / TICK_RATE`. 20 Hz is 50 ms. Overruns are counted, never slept off negatively — `next` jumps back to now to avoid a spiral of death.

The network-side input channel is 1024 deep; when full it drops the oldest and keeps the latest. The snapshot send buffer is 32 with the same policy. Gameplay can afford to lose a frame; it cannot afford one slow client stalling the room.

Behind that channel each player has a **jitter buffer**: a queue the tick goroutine fills as input arrives and draws exactly one input from per tick. It exists because a client sending at the tick rate and a server consuming at the tick rate still do not line up — jitter puts two inputs in one window and none in the next — and the room used to answer that by overwriting one slot per player, which threw away the surplus and then had nothing for the gap. `INPUT_BUFFER` is the depth, and past it the oldest are dropped: a client whose clock runs fast is kept current rather than complete. Measured numbers are in README 4b.

## Delta snapshots

A 64-tick ring buffer per room, a per-client acknowledgement, `diff(ring[ack], now)`. Details are in README section 6; the code is in `internal/room/delta.go`.

Two things that are easy to get wrong:

- **The ring is shared, and so is the encode — as far as the baselines allow.** Every client sees the same world, so one ring is enough. A delta is a pure function of (baseline, current), and current is shared, so two clients on the same acknowledged tick are owed byte-identical bytes. In the steady state that is every client in the room, because they all acknowledge the previous tick. Encoding once per *client* rather than once per *distinct baseline* makes the tick cost quadratic in room size for nothing: at 24 players it measured 31.1 µs and 775 allocations per tick, against 5.3 µs and 64 when grouped by acknowledgement. Clients only diverge here transiently, after one of them drops a packet. `deltaSnapshot` sorts its removal list precisely so this reuse is sound.
- **A stale acknowledgement from a previous connection must be discarded.** `subscriber.since` is what stops it. Without it: a reconnecting client has thrown its history away, the server keeps encoding from the old tick, the client cannot apply it so never acknowledges anything newer — wedged permanently, with no error reported anywhere.

## Determinism

- `Milli int32` (1 unit = 0.001). No `float64` in the simulation.
- Players are sorted by id before `Step`.
- splitmix64 RNG, with the seed written into `match_found` — replaying twice gives the same snapshot (`internal/sim/world_test.go`).
- Spawn slots are assigned once from the sorted roster position, never derived from the player id. Deriving them put two players on one point whenever their ids collided modulo the ring size, which the default roster guarantees since seats count from 1 and bots from 1000.

Production lockstep (fighting games) also checksums each tick. FPS and IO games usually go server-authoritative plus client interpolation; this demo takes that route.

## Data stores

| Store | Role | Not used for |
|---|---|---|
| Process memory | World, WS buffers | Cross-node presence |
| Redis | Queue, presence TTL, GS heartbeat, room registry, pub/sub notify (assignments and turn pushes), turn pairing slot | The tick path (far too slow) |
| Kafka | Match lifecycle, analytics | Input/snapshot |
| Redis hash `mm:rating` | Elo, read on queue and written on match end | A durable record — it is a 30-day cache, not an account store. Unused when `PLATFORM_DSN` is set: the database becomes the record |
| Postgres (`PLATFORM_DSN`) | Accounts, profiles, wallet, inventory, purchase ledger, match history, ladder | The tick path, and anything derived. It holds only what cannot be rebuilt by playing again |
| Disk (`REPLAY_DIR`) | Sampled match recordings: seed + inputs | Anything on the tick path; the file is written once, after the match |

The tick path must stay in-process. Redis and gRPC are only there to **place a room** and **route a player**, never to simulate.

Every one of those calls carries a deadline (`REDIS_TIMEOUT`). They sit on a player's connection path and none of them is the simulation: without a ceiling, a Redis that stops answering parks the goroutine of everyone logging in, joining or disconnecting, and the process runs out of goroutines before anything reports a problem. The blocking job queue and the pub/sub subscriptions keep the process lifetime instead — they are supposed to wait.

The dividing line between the Redis rows and the Postgres one is expiry. Everything in Redis
carries a TTL because everything in it is derived from play and can be rebuilt
by playing again; an account cannot be rebuilt from anything, and a row with a
TTL on it is not a record. That is the whole reason the platform tier is a
different store rather than more keys in the one already running.

The platform tier touches the realtime tier in exactly two places on a player's
path, and each is bounded by the budget that fits it. The rating read on a queue
join takes `REDIS_TIMEOUT` like every other lookup on that path — it is a
primary-key read, and one rule for the connection path is worth more than a
knob. The match write takes `PLATFORM_TIMEOUT`, which is wider because a result
is a transaction across four tables, and it takes a *fresh* deadline rather than
inheriting the room's: `OnEnd` runs under the Redis budget, and cancelling a
result write on a busy database would lose a match that was played in full.

## Horizontal scaling

1. Each GS heartbeats `gs:{id}` with a 15 s TTL and its `rooms` count.
2. The matchmaker runs `PickLeastLoaded`, counting reserved-but-not-started rooms so a burst does not all land on one node, with a deterministic tie-break by node id. Inflight counters are read in one `MGet` rather than a round trip per candidate — placement runs on a 50 ms ticker, so a per-candidate query would grow the cost with the fleet.
3. A job goes to `LPUSH gs:jobs:{id}` / `BRPOP`.
4. The GS starts the room and publishes to `mm:results`.
5. The gateway still holding the connection forwards `match_found{host}`.
6. The client opens a WS to that GS and sends `join_room`.

Same shape as Agones: a dedicated process (or pod) hosts N matches and the client connects to the game pod directly. The gateway does not proxy snapshots, which would be a double hop.

Adding capacity means scaling the `gameserver` replicas; they register themselves in Redis.

## Race conditions

| Surface | Guard |
|---|---|
| World mutation | Tick goroutine only |
| Input from N readers | Channel into a per-player queue; both the fill and the one-per-tick draw run on the sim goroutine |
| Subscriber map | `sync.Mutex`, copy the slice then broadcast outside the lock |
| Matchmaking pop | Redis Lua — two matchmakers cannot claim one player |
| Connection close vs send | `atomic` closed flag, send channel closed exactly once |
| Connection metadata | Unexported behind accessors: the acknowledged tick is an atomic high-water mark, and the room binding is read and written as one pair. The tick goroutine writes these while the connection goroutine reads them, so bare fields are a race the server hits in normal operation, not a rare one |
| Presence | Key TTL, `DEL` on disconnect |
| Two clients spending one balance | The check is in the `UPDATE`'s `WHERE`, inside the transaction. Read-then-write is the lost-update bug: both read 100, both decide 60 is affordable, both write 40 |
| Two overlapping matches moving one rating | Applied as a delta (`rating + d`, floored), so they compose. Assigning a value read when a match started undoes anything that finished since |
| One match recorded twice | `(match_id, account_id)` primary key — the retry of a half-written match writes exactly the rows that are missing |
| A panic anywhere | `internal/safe` — the goroutine that panicked is the only thing that dies |

`go test -race ./...` covers the queue, room inputs, connection metadata and the UDP peer lifecycle.

## Shutdown

1. SIGTERM
2. `registry.Unregister` — out of the placement pool at once, rather than waiting out a 15-second heartbeat TTL
3. `readyz` returns 503, so the load balancer stops sending traffic
4. `jobLoop` stops taking room jobs, so the drain can reach zero
5. Wait up to `DRAIN_TIMEOUT` for matches in progress to finish
6. HTTP `Shutdown` — no new connections accepted
7. `Room.Stop` — the loop exits after the current tick; any recording is closed and written
8. WebSockets closed with a close frame
9. The Kafka writer's `Close` flushes async batches

Steps 2 and 4 are what make step 5 mean anything: a node that keeps being given matches, or keeps accepting them, never drains. Matches still running when `DRAIN_TIMEOUT` expires are cut off — migrating them would mean transferring world state plus a reconnect, which is only worth building when draining a node is a routine event.

## Metrics worth watching

- `arena_ccu` against `arena_rooms`
- `arena_tick_duration_seconds` p99 against the tick budget
- `arena_tick_overruns_total`
- `arena_snapshots_dropped_total` (slow clients)
- `arena_matchmaking_queue_depth`
- `arena_panics_total{where}` — page on it
- `arena_placements_refused_total` — the fleet is out of capacity
- `arena_match_skill_spread` against `arena_matchmaking_wait_seconds` — the two halves of what matchmaking is worth
- `arena_platform_ops_total{op,result="error"}` — the platform store is failing. `declined` is not a fault: it is an overdrawn wallet or an item somebody already owns. `unauthorized` rising on its own is somebody trying tokens
- `arena_platform_match_rows_total` against `arena_matches_ended_total` — results reaching the database, versus matches that ended. The gap should be exactly the bots and the unauthenticated sessions

pprof: `heap`, `goroutine`, `mutex`, `block`, `profile`. The server enables `SetMutexProfileFraction` and `SetBlockProfileRate`.

## Two sync families in one repo

`internal/room` (arena) and `internal/turn` (turn-based) are genuinely different match models sharing everything underneath. The comparison table is in the README; these are the differences that *have* to differ:

| | Arena | Turn-based |
|---|---|---|
| Packet loss | acceptable, the next tick patches it | **forbidden** — events must be complete and ordered |
| Buffer full | drop the oldest (latest wins) — `Conn.Send` | dropping is not allowed — `Conn.SendOrdered`, and the socket is closed rather than a message quietly deleted |
| State lives in | one goroutine's RAM | a versioned store |
| Routing | sticky — must be the right node | stateless, any node will do — so pushes have to be routed to the player, not assumed local |
| Concurrent writes | none (one goroutine) | CAS on a version |

Inverting the drop policy is the easiest mistake to make when copying code from one side to the other — and sharing one send path between the two families makes it automatically. `Conn.Send` throws away the oldest queued message to make room, which is right for a snapshot (it describes the whole world at a tick, so a newer one supersedes it) and wrong for an event (each carries a distinct fact, and losing one leaves a hole). Turn updates go through `Conn.SendOrdered`, which refuses rather than drops; a connection that cannot take an update has not drained in many moves' worth of buffer, so it is closed and the client replays from its cursor.

## Client prediction

`web/predict.js` ports `applyInputs` to JavaScript. The survival condition is that the two agree *bit for bit* — the simulation uses fixed-point `int32`, so the JS has to `Math.trunc` exactly where Go does integer division.

`internal/sim/predict_port_test.go` runs both and compares, and also checks `speedTick` at 20/30/60 Hz (get that constant wrong and every step is wrong). The test skips itself if `node` is not installed.

## Why state and the event log share one store

`turn.Store` owns both the state and the log rather than splitting them into two components. That is a choice, not laziness.

They are two halves of one fact — "this move happened". Writing them separately leaves a window: crash in between and the state advances while the log is missing an event, so a client diffing from its cursor never learns about it. `Commit` writes both or neither.

The Redis implementation manages it with Lua: Redis runs a script to completion with nothing interleaved, so "compare the version, write the state, XADD the events" is one atomic step. No outbox needed.

`Create` is one script for the same reason.

`Create` also clears the three keys before writing them. A match id that maps onto keys a previous match left behind would splice the two together: `HSET` resets the state to version 1 while the cursor keeps counting and the old events stay in the log, so a client syncing from zero replays a game that is already over. The gateway gives every match a nonce in its id as well — deriving the id from the pair of players alone meant the same two people meeting again reused it.

Which connection is the *current* one for a player is a question with exactly one authority: the session hub. Three places have to ask it, and each of them is a bug if it does not — `onClose` (a late close must not tear down the connection that replaced it), `turnHub.drop` (identity-checked deletion), and `bindTurn`. That last one is the least obvious: a frame already read when the reconnect landed arrives afterwards and, filed naively, re-registers the dead connection as where that player's updates go. Every later push then goes into a closed socket and is discarded without a sound — not even counted, since there is nothing left to close — while the live connection receives nothing. The message it carried still applies; only delivery belongs to the newer connection.

## Reaching a player who is somewhere else

Turn-based routing is stateless — any node can serve any match — but that cuts both ways: **the node that decides a move is routinely not the node the player is attached to.** Three cases, all normal:

- The two players hit different gateway replicas.
- A player reconnects and the load balancer sends them somewhere new.
- A turn runs out of time, and the auto-play is applied by the **matchmaker**, which holds no player connections at all.

So a turn update goes out the same way an `Assignment` does. The deciding node delivers locally if it happens to hold the player, and otherwise publishes `TurnPush{player_id, update}` on the bus.

It is **addressed**, not broadcast: presence already records which node a player is on, so the push goes to `turn:push:{node}` and only that gateway wakes for it. Broadcasting would cost Redis one send per gateway per message, and since the fleet grows with the player count, that factor makes egress grow with the *square* of it — every gateway also decoding every message to discard almost all of them. When presence cannot say where a player is, delivery falls back to the shared `turn:push` channel, which every gateway also subscribes to: a wasteful delivery beats a lost one. A push nobody claims means the player really is offline, which costs one dropped message — they catch up from their cursor on reconnect, which is the whole point of an event log.

Fire-and-forget is only safe because the client checks. `web/cursor.js` holds the rule: events are numbered consecutively, so the run a client is owed is exactly `seq+1 .. current_seq`. Anything else is a hole, and on a hole the browser applies the state — a projection is always complete — but leaves its cursor where it is and asks for the missing run with `TURN_SYNC{since_seq}`. Rendering a short run and then advancing the cursor past it would lose those events permanently. That is the inversion of the arena's policy in the table above, and the reason the cursor exists at all. `internal/turn/cursor_port_test.go` pins the cases from Go, the same way `predict_port_test.go` pins the prediction port.

`web/platform.js` is pinned the same way by `internal/platform/webclient_port_test.go`, and for the same class of reason: the idempotency key a purchase carries is chosen by the client, so the rule that a retry reuses it and a new purchase does not is a rule only the client can keep. The server sees two requests either way.

A socket that drops mid-match reconnects on its own and resyncs from that cursor. The match does not pause for a missing player — pausing would be an exploit, since anyone losing could pull their network cable to stop the game — so reconnecting and asking what was missed is the only way back in.

The pairing slot is shared for the same reason. Held per-node, two players who land on different gateways never see each other and both sit on a queue screen that resolves for nobody. `turn:waiting` holds at most one parked player and `Claim` reads-and-clears it in one script: two arrivals racing for one parked player must not both be told they matched, which would be three people in a two-seat game. The park carries a TTL because it outlives the node that made it — a tab closed between the two halves of a pairing leaves an entry no close handler will ever see. An arrival that is handed a park still checks the player is reachable before opening a match, local connection first and otherwise presence: matched against somebody who already left, the arrival plays a whole game against the timeout worker.

## The memory and Redis implementations must agree

Each store has two implementations, selected by one environment variable. Behavioural differences between them only surface in production.

Tests use `internal/redistest` (miniredis, in-process) and run **the same assertions against both**. That caught `placement.Take`: Redis blocked for 2 s and returned `nil`, memory blocked forever. The contract now lives in the interface, along with a `TakeWait` constant.

A few differences are **deliberate**, and are written into tests as well:

- `cluster.NewMemory` registers its own node as a placement candidate, which is right for single-process mode.
- `placement`'s inflight timestamps are in seconds, so a `maxAge` below one second is meaningless.

## Player identity

The arena uses `PlayerID uint32` as **a seat number within one match** — local to the room, not an identity.

Where that identity comes from depends on whether the platform tier is on. With
`PLATFORM_DSN` unset the gateway mints a random `p-…` per session, which is
honest about what it is: a label on a connection, meaningless tomorrow. With the
tier on it is an `a-…` account id that a login authenticated, and the prefixes
differ on purpose — the two end up in the same token field and the same string
column, so the id itself is the only place the difference can be visible, and it
has to be, because a row written against a `p-…` is a row nobody can ever claim.

Turn-based uses **an account/session id as a string** for real identity. It started out as a `uint32` the gateway counted up, and that was a bug: two gateways counting independently hand the same id to two different people, and with a shared Redis store those two people collide inside one match.

The rule that falls out: **a seat number may be local, an identity may not.** Anything written into a shared store must carry an identity sourced from auth, not from one process's counter.

The same reasoning applies to idempotency keys, which are chosen by clients and therefore collide between them. They are scoped to the submitting player; stored bare, one player's key matched another's and the second player's legal move was swallowed as a duplicate — no events, no error, the move simply never happened.

## Data lifetimes

| Data | How it expires |
|---|---|
| Arena room | Dies with the process; `UnregisterRoom` when the match ends |
| Presence | TTL of 45 s / 2 minutes |
| Turn match | TTL of 24 h while live, 30 minutes once finished (`TURN_LIVE_TTL` / `TURN_ENDED_TTL`) |
| Turn event log | Same TTL as the state, `MAXLEN ~ 500` |
| Deadline ZSET | Self-cleaning on pop |
| Turn pairing slot | TTL of 2 minutes; cleared on claim or on disconnect |
| Rate-limit windows | Swept amortised on write; the key space is the internet, not the player base, so entries that are never reclaimed are a leak that looks fine for a day |
| Accounts, wallets, inventory, match history | **They do not.** That is what makes them a different store — see "Data stores" above |

`TURN_ENDED_TTL` is where the turn-based memory bill is set, and it is the first wall this mode hits — well before the push fanout. A match measures about **5.6 KB** across its three keys, and since a finished match sets the short window on its final write, finished matches are nearly everything stored at any moment. Steady state is roughly `matches/sec x TURN_ENDED_TTL x 5.6 KB`: a measured 10k-bot run left **1.54 GB** across 272k matches, and a thousand matches a second would cost another 336 MB for every extra minute of the window. Both windows are refreshed on every move, so each is measured from the last one rather than from the deal.

The 24 h window is a backstop, not a real cost — as long as a matchmaker role is running. Its deadline sweeper plays an abandoned match out within a few turn limits, which drops it onto the short window. Deploy without a matchmaker and abandoned matches really do sit for a day.

The three keys of a turn match must expire **together**. A surviving cursor pointing into a log that is gone means the client reports a permanent gap — expiring at different times is worse than not expiring at all.

## Crash isolation

The room is the unit of parallelism. Go does not make it the unit of *failure* for free: one unrecovered panic ends the process, and with it every other match on the node — around 1250 at 8 players and 10k CCU.

`internal/safe` wraps every goroutine the server starts and every callback it runs on behalf of one client.

| Unit | What a panic costs | How it recovers |
|---|---|---|
| Room tick loop | that match | ends through the normal `OnEnd` path; players reconnect and requeue |
| Background loop (matchmaker, jobs, heartbeat, deadlines) | one pass | restarted after a second — a dead matchmaker is silent, which is worse than a busy log |
| Connection read/write pump | that connection | the client reconnects |
| One message | that message | the connection stays up |
| Pub/sub delivery | that message | the subscription stays up, which is how assignments and turn pushes keep flowing |

`arena_panics_total{where}` is a paging metric. Recovery is a blast-radius limiter, not error handling: the counter is the only evidence the room ever existed.

## Capacity and draining

Balancing is not refusing. `PickLeastLoaded` returns the least loaded node, which is right until every node is past what it can simulate — then it keeps feeding whichever one is failing its tick budget by the smallest margin.

1. A game server advertises `capacity` and `draining` in its heartbeat.
2. Placement skips a node that is draining or at its ceiling. With no eligible node it returns nil, and the matchmaker puts the players back in the queue with their original `queued_at`.
3. The node itself refuses as a backstop — its own room count is fresher than a two-second-old heartbeat, and a burst of placements can land inside that window.
4. On SIGTERM the node **unregisters** before anything else, rather than waiting for its heartbeat key to expire: a stale record is 15 seconds of players being sent to a process that is shutting down.
5. `jobLoop` stops taking work while draining, so the drain can actually reach zero.

Agones expresses the same thing with pod states; this is the same idea in a heartbeat field.

## The platform tier is a different kind of store

`internal/platform` holds accounts, wallets, inventory, purchases, match history
and the ladder. Everything else in this document is arranged around a deadline —
a tick budget, a TTL, a grace window. This one is arranged around a transaction.

Three rules, each of which the obvious implementation breaks:

1. **Every write that a caller can retry carries a key, and a client-chosen key
   is scoped to the account.** Clients choose keys independently, so a bare key
   means one player's purchase is swallowed as another's duplicate. `internal/turn`
   found this with move idempotency keys; the ledger is keyed the same way.
2. **A result is keyed per player, not per match** — `(match_id, account_id)`.
   A writer that died halfway through a match's rows has to be able to retry and
   write exactly the missing ones, which a single "seen this match?" flag cannot
   express.
3. **A balance check belongs in the `UPDATE`'s `WHERE` clause and a rating is
   applied as a delta.** Read-modify-write loses concurrent spends, and assigning
   a rating read minutes earlier silently undoes any match that finished in
   between.

The two implementations must agree, the same rule the Redis stores follow, with
one difference that cannot be avoided: there is no in-process Postgres the way
miniredis is an in-process Redis. `eachStore` runs the memory half always and
the Postgres half when `PLATFORM_TEST_DSN` is set; `make test-platform` starts a
throwaway database so that skip is a convenience and not a hole.

Recording a match is where the two tiers meet, and it is one function call — not
a network hop and not a Kafka consumer. The lifecycle events on the bus still
have nobody downstream, which is why the outbox this document's turn-based notes
argue for is still unbuilt: the reward hangs off the room's `OnEnd`, inside a
transaction, rather than off `match.ended`.
