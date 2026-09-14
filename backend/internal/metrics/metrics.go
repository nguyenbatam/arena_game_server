package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CCU = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "arena_ccu",
		Help: "Concurrent connected players",
	})
	WSConnections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_ws_connections_total",
		Help: "Accepted WebSocket/TCP sessions",
	})
	Rooms = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "arena_rooms_active",
		Help: "Active game rooms on this instance",
	})
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "arena_matchmaking_queue_depth",
		Help: "Players waiting in matchmaking",
	})
	QueueWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "arena_matchmaking_wait_seconds",
		Help:    "Time spent in matchmaking queue",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 8, 15, 30},
	})
	TickDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "arena_tick_duration_seconds",
		Help:    "Game simulation time per tick",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.016, 0.033, 0.05},
	})
	TickOverruns = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_tick_overruns_total",
		Help: "Ticks that exceeded the fixed timestep budget",
	})
	Inputs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_inputs_total",
		Help: "Player inputs applied",
	})
	Snapshots = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_snapshots_total",
		Help: "Snapshots broadcast",
	})
	SnapshotsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_snapshots_dropped_total",
		Help: "Snapshots dropped because a client send buffer was full",
	})
	SnapshotsFull = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_snapshots_full_total",
		Help: "Full snapshots sent: new subscriber, reconnect, or an ack older than the history window",
	})
	SnapshotsDelta = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_snapshots_delta_total",
		Help: "Delta snapshots sent against a baseline the client acknowledged",
	})
	// SnapshotBytes is labelled by encoding, and the two children it can have
	// are resolved once below rather than looked up per send. See
	// SnapshotBytesFull.
	SnapshotBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "arena_snapshot_bytes_total",
		Help: "Snapshot payload bytes written, by encoding",
	}, []string{"kind"})
	TurnMatches = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_turn_matches_started_total",
		Help: "Turn-based matches created",
	})
	TurnMoves = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "arena_turn_moves_total",
		Help: "Turn-based moves applied, by origin",
	}, []string{"origin"})
	TurnRejects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "arena_turn_rejected_total",
		Help: "Turn-based submissions refused, by reason",
	}, []string{"reason"})
	TurnSyncs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "arena_turn_syncs_total",
		Help: "Cursor syncs served, by kind — a rising full_resync rate means the log window is too small",
	}, []string{"kind"})
	TurnPushDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_turn_push_dropped_total",
		Help: "Turn updates that did not fit a client send buffer; the connection is closed and the client resyncs from its cursor",
	})
	TurnConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_turn_cas_conflicts_total",
		Help: "Commits that lost their compare-and-swap and were retried",
	})
	EventsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_events_published_total",
		Help: "Async domain events published",
	})
	MatchesStarted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_matches_started_total",
		Help: "Matches created",
	})
	MatchesEnded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_matches_ended_total",
		Help: "Matches finished",
	})
	GSHeartbeat = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "arena_gameserver_rooms",
		Help: "Rooms reported in gameserver heartbeat",
	}, []string{"node"})
	RateLimited = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_rate_limited_total",
		Help: "Requests rejected by rate limiter",
	})
	AuthFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_auth_failures_total",
		Help: "Failed player authentication attempts",
	})
)

// SnapshotBytesFull and SnapshotBytesDelta are SnapshotBytes' two children,
// resolved at startup.
//
// The label set here is closed — an encoding is full or it is delta — so there
// is nothing for a per-call lookup to discover. It was doing one anyway: the
// room charged these inside the per-subscriber loop, which runs once per client
// per tick, and WithLabelValues hashes its arguments and takes the vector's
// lock every time. Measured at 19.8 ns against 1.9 ns for the resolved child;
// at eight players that is about 150 ns a tick, roughly a tenth of the whole
// broadcast. Counters are safe to hold onto — the child is the counter, and
// nothing in this process ever deletes one.
var (
	SnapshotBytesFull  = SnapshotBytes.WithLabelValues("full")
	SnapshotBytesDelta = SnapshotBytes.WithLabelValues("delta")
)

// Panics counts recovered panics by the unit of work that died. It is a paging
// metric, not a health one: the recovery keeps the process up, and this counter
// is the only remaining evidence that a room or a connection was lost.
var Panics = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arena_panics_total",
	Help: "Recovered panics, labelled by the unit of work that died",
}, []string{"where"})

// PlacementsRefused counts matches that could not be placed because every game
// server was full or draining. It is the signal to add capacity: the queue is
// still filling, the fleet has stopped accepting, and the players are being put
// back where they came from.
var PlacementsRefused = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arena_placements_refused_total",
	Help: "Matches that found no game server with room",
})

// MatchSkillSpread is the rating gap between the strongest and weakest player
// in a formed match — the one number that says whether matchmaking is doing
// anything. Queue wait alone cannot: a queue that matches everybody instantly
// looks perfect right up until you notice who it matched them with.
var MatchSkillSpread = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arena_match_skill_spread",
	Help:    "Rating gap between the best and worst player in a formed match",
	Buckets: []float64{0, 50, 100, 200, 400, 800, 1600},
})

// ReplaysWritten and ReplayErrors track match recording. A replay that failed
// to write is not a gameplay problem, which is exactly why it needs a counter:
// nothing else would ever notice.
var (
	ReplaysWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_replays_written_total",
		Help: "Match replays written to disk",
	})
	ReplayErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_replay_errors_total",
		Help: "Match replays that could not be written",
	})
)

// InputsDropped counts inputs thrown away because a client was producing them
// faster than the server consumes them. A little is jitter; a lot is a client
// whose clock runs fast, and it will feel like rubber-banding to that player.
var InputsDropped = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arena_input_buffer_dropped_total",
	Help: "Inputs discarded because a player's buffer was over its target depth",
})

// InputUnderruns counts ticks where an active player had no input to apply and
// held their position. Some is unavoidable — a packet that has not arrived is
// not there to simulate — but a rising rate against a flat drop rate says the
// buffer is too shallow for the jitter these players are living with.
var InputUnderruns = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arena_input_underruns_total",
	Help: "Ticks on which an active player had no input available",
})

// MatchWarmup is how long a room waited for its players before starting the
// match clock.
//
// It should sit near zero in one process and near the network round trip across
// nodes. A distribution pushed up against WARMUP_TIMEOUT means players are not
// arriving, which is a placement or handoff problem showing up where it is
// actually felt.
var MatchWarmup = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arena_match_warmup_seconds",
	Help:    "Time a room waited for its seats to connect before starting the match",
	Buckets: []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10},
})

// WarmupTimeouts counts matches that started short-handed.
var WarmupTimeouts = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arena_match_warmup_timeouts_total",
	Help: "Matches that began before every seat had connected",
})

// RosterEventsDropped counts departures and rejoins a room's queue could not
// take.
//
// It should never move. Unlike a dropped input — which costs one frame of
// movement — a dropped departure leaves a disconnected player's avatar standing
// in the match for the rest of it, paying score to whoever shoots it and
// writing that score into the ladder. If this is above zero, a room stopped
// draining its queue, and the interesting question is why.
var RosterEventsDropped = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arena_roster_events_dropped_total",
	Help: "Departures or rejoins a room could not queue",
})

// LagCompShots, LagCompTicks and LagCompCapped are how far into the past shots
// are being compensated.
//
// The compensation is derived from the ack_tick a client puts on its input, and
// that number comes off the wire: a client that under-reports it is claiming a
// worse connection than it has and collecting projectile catch-up for the
// difference. sim.MaxLagCompTicks caps what the claim can buy, which is the
// defence; these are how the claim becomes visible.
//
// Counters rather than a histogram, and accumulated per room before they are
// touched. A histogram Observe on the tick path costs 11 ns uncontended and
// 275 ns at -cpu 10 — measured — and a player holding the fire button produces
// one per tick, so at 10k CCU this was 200k contended observes a second on one
// shared set of buckets, charged to the goroutines with a tick budget to keep.
// Three counters flushed once per room per tick is 25k, and only on ticks where
// somebody shot. Same argument flushCounters already makes for the fanout.
//
// What the shape costs is the distribution; what it keeps is the question worth
// asking. LagCompTicks/LagCompShots is the mean compensation the fleet is
// granting, and LagCompCapped/LagCompShots is the fraction of shots reaching
// straight for the ceiling — a population on bad links spreads out below it,
// and a client turning the dial sits on it. Neither tells a liar from a bad
// link on its own: that needs a latency the server measures itself, which means
// a server-initiated ping the client echoes, and is a protocol change.
var (
	LagCompShots = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_lag_compensated_shots_total",
		Help: "Shots granted some lag compensation",
	})
	LagCompTicks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_lag_compensation_ticks_total",
		Help: "Ticks of lag compensation granted, summed over shots",
	})
	LagCompCapped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_lag_compensation_capped_total",
		Help: "Shots granted the maximum compensation sim.MaxLagCompTicks allows",
	})
)

// EventsDropped counts lifecycle events the broker never accepted.
//
// It exists because arena_events_published_total used to be incremented by the
// caller the moment it handed an event over — with the write queued
// asynchronously and the error discarded, so the counter reported success for
// events that were never delivered. A success metric that cannot fail is worse
// than no metric: it is a graph that stays flat through an outage.
var EventsDropped = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arena_events_dropped_total",
	Help: "Lifecycle events the event bus failed to deliver",
})

// The platform tier's database calls.
//
// Split out from everything else in this file because they are the only work in
// the process that is allowed to be slow. A tick is measured against a 50 ms
// budget; a purchase is a transaction across four tables and is measured
// against a player's patience. Mixing them into one latency histogram would
// bury the interesting tail of each in the other's bulk.
//
// PlatformOps carries the outcome as a label rather than counting only
// failures: "declined" is the ordinary answer to an overdrawn wallet or a
// cosmetic somebody already owns, and it must not be read as an error. Watch
// result="error" — result="declined" is the system working.
var (
	PlatformOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "arena_platform_ops_total",
		Help: "Platform tier operations by kind and outcome (ok, declined, error)",
	}, []string{"op", "result"})
	PlatformLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "arena_platform_op_duration_seconds",
		Help:    "Platform tier operation latency",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
	}, []string{"op"})
	// MatchesRecorded counts player lines actually written by a finished match.
	// It sits below arena_matches_ended_total by exactly the bots and the
	// unauthenticated sessions, and a gap wider than that means results are
	// being dropped somewhere between the room and the database.
	MatchesRecorded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "arena_platform_match_rows_total",
		Help: "Per-player match results written to the platform store",
	})
)

// CoordErrors counts failed calls to the coordination stores — presence, the
// matchmaking queue, the node/room directory, the placement queue and the
// notify bus.
//
// None of these is the simulation, and none of them is worth failing a player's
// connection over, so every call site treats a failure as soft: it logs and
// carries on. That is exactly why the counter has to exist. A soft failure with
// no counter is invisible — a Redis that has started refusing writes looks,
// from every graph in this file, like a server nobody is using: CCU flat,
// matches flat, no errors anywhere. Labelled by operation so the graph says
// which half of the coordination layer is failing rather than only that
// something is.
var CoordErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arena_coordination_errors_total",
	Help: "Failed coordination-store calls, by operation",
}, []string{"op"})

// TurnDeadlineArmFailures counts turn deadlines that could not be scheduled.
//
// Its own counter rather than a CoordErrors label because the consequence is
// unique in this process: nothing else counts down in turn mode, so a turn with
// no armed deadline is a match that can only be ended by the players
// themselves. One that is abandoned instead sits until turn.DefaultLiveTTL —
// 24 hours — with the opponent waiting on a move that will never come.
var TurnDeadlineArmFailures = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arena_turn_deadline_arm_failures_total",
	Help: "Turn deadlines that could not be armed; the match has nothing counting down",
})

// HTTPWriteErrors counts responses that could not be written to the client.
//
// Almost always a caller that hung up mid-response, which is not a fault and is
// why these are logged at all rather than raised: the status line is already on
// the wire by the time the body fails, so there is nothing left to tell the
// client. It is worth counting because the other reading — a handler
// serialising something that cannot be encoded — is a real bug that produces a
// 200 with a truncated body and no other trace.
var HTTPWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arena_http_write_errors_total",
	Help: "HTTP responses that could not be fully written, by handler",
}, []string{"handler"})
