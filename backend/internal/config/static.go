package config

import (
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
)

type Static struct {
	Role          pb.Role
	NodeID        string
	WebDir        string
	TurnLimit     time.Duration
	TurnRateLimit int
	// TurnLiveTTL / TurnEndedTTL are how long a match's Redis keys survive.
	// Finished matches are almost all of what is stored at any moment, so
	// TurnEndedTTL is the knob that decides the memory bill.
	TurnLiveTTL  time.Duration
	TurnEndedTTL time.Duration
	PublicAddr   string
	HTTPAddr     string
	TCPAddr      string
	UDPAddr      string

	RedisAddr     string
	RedisPassword string
	// RedisTimeout bounds one coordination call. Presence, the queue and the
	// room directory all sit on a player's connection path, and none of them
	// is the simulation: a lookup that has not answered inside this budget is
	// not going to save the login it is holding up, it is only going to hold
	// up the next one too. Without a ceiling a stalled Redis parks every
	// connection goroutine in the process, which is how one slow dependency
	// becomes a dead gateway.
	RedisTimeout time.Duration
	KafkaBrokers []string
	KafkaTopic   string

	AdminToken     string
	JWTSecret      string
	JWTTTL         time.Duration
	AllowedOrigins []string
	TrustProxy     bool
	QueueRateLimit int
	LoginRateLimit int
	HelloRateLimit int
	// ConnMsgRate / ConnByteRate are what one established connection may spend,
	// as opposed to the Window limiters above, which are keyed by IP and guard
	// the doors a stranger knocks on. Both are needed: the per-IP limiters stop
	// a flood of new connections and do nothing about an authenticated client
	// that sends a hundred thousand inputs a second, while a per-IP cap tight
	// enough to catch that one client would also throw out everyone sharing a
	// carrier NAT. Zero disables the dimension.
	ConnMsgRate   int
	ConnMsgBurst  int
	ConnByteRate  int
	ConnByteBurst int
	// InputBuffer is how many ticks of input a room holds in reserve for a
	// player, absorbing the jitter that otherwise costs them a frame of
	// movement every time two inputs land inside one tick window. It is a
	// ceiling rather than a delay: a steady stream is drained every tick and
	// nothing is held back. Fixed for a room's life, like the tick rate.
	InputBuffer int
	// SnapshotRate is how many snapshots a second a room sends, independent of
	// how many times a second it simulates. Zero — the default — sends one per
	// tick, which is what this server did before the knob existed.
	//
	// They are different numbers in every engine that has had to pay for
	// bandwidth: Source calls them tick and sv_updaterate, Overwatch simulates
	// at 60 Hz and sends at 20. Raising the tick rate buys hit resolution and
	// input latency; raising the send rate buys egress, and egress grows with
	// the square of the room size. Tied together, moving TICK_RATE from 20 to
	// 60 through /admin/config tripled every room's outbound traffic as a side
	// effect of asking for a better simulation.
	SnapshotRate int
	// WarmupTimeout is how long a room waits for its players to connect before
	// starting the match clock. Zero starts immediately.
	//
	// The clock used to start when the placement job was taken, which is before
	// anybody has been told the match exists. In one process that is
	// milliseconds; across nodes it is a handoff — a new socket, a HELLO and a
	// JOIN_ROOM — and every second of it comes out of the match.
	WarmupTimeout time.Duration
	// MaxRooms is how many matches this node will host. Placement balances but
	// never refuses, so without a declared ceiling a busy fleet keeps stacking
	// rooms onto nodes that are already missing their tick budget — the players
	// already in those matches pay for it. Zero means no ceiling.
	MaxRooms int
	// PlatformDSN selects the platform tier's store — accounts, profiles,
	// inventory, currency, match history and the leaderboard.
	//
	// Three values, following REDIS_ADDR's shape where one variable decides the
	// whole mode:
	//
	//   ""              off. /auth/login hands a token to whoever asks, exactly
	//                   as it did before there was a platform tier, and nothing
	//                   a player does outlives the process.
	//   "memory"        on, with no database. For the demo and for tests: every
	//                   rule is enforced, nothing is durable.
	//   a postgres URL  on, durable.
	PlatformDSN string
	// PlatformMaxConns caps the connection pool. Every replica opens its own,
	// so this multiplies by the fleet and has to stay under the database's
	// max_connections with room to spare.
	PlatformMaxConns int
	// PlatformTimeout bounds one database call, for the same reason
	// RedisTimeout bounds one Redis call — with one difference worth the extra
	// knob: a SQL transaction that reads and writes four tables is doing more
	// work than an HGET, so it gets a wider budget rather than sharing one
	// tuned for a key-value lookup.
	PlatformTimeout time.Duration
	// PlatformRateLimit is per client IP, per minute, across the /platform
	// endpoints. These are the only endpoints in the process that put an
	// anonymous request in front of a database.
	PlatformRateLimit int
	// ReplayDir turns on match recording. Empty disables it.
	ReplayDir       string
	MaxReplays      int
	PprofEnabled    bool
	Env             string
	ShutdownTimeout time.Duration
	DrainTimeout    time.Duration
}

// SnapshotEvery converts SnapshotRate into the tick interval a room wants.
//
// Clamped rather than validated: a send rate at or above the tick rate is
// "every tick", which is both the honest reading and the safe one. Rounding is
// downward so the room never sends *less* often than asked — at 60 Hz and a
// requested 25, 60/25 is 2, which is 30 Hz.
func (s Static) SnapshotEvery(tickRate int) int {
	if s.SnapshotRate <= 0 || tickRate <= 0 || s.SnapshotRate >= tickRate {
		return 1
	}
	return tickRate / s.SnapshotRate
}

func (s Static) MatchTicks(d View) uint32 {
	tr := d.TickRate
	if tr <= 0 {
		tr = 20
	}
	return uint32(d.MatchSeconds * tr)
}

func LoadStatic() Static {
	s := Static{
		Role:           ParseRole(envAny("all", "ROLE", "ARENA_ROLE")),
		NodeID:         envAny("gs-local", "NODE_ID", "ARENA_INSTANCE_ID"),
		WebDir:         resolveWebDir(),
		TurnLimit:      envDuration("TURN_LIMIT", 20*time.Second),
		TurnRateLimit:  envInt("TURN_RATE_LIMIT", 120),
		TurnLiveTTL:    envDuration("TURN_LIVE_TTL", 24*time.Hour),
		TurnEndedTTL:   envDuration("TURN_ENDED_TTL", 30*time.Minute),
		PublicAddr:     envAny("ws://localhost:8080/ws", "PUBLIC_ADDR", "ARENA_PUBLIC_HOST"),
		HTTPAddr:       envAny(":8080", "HTTP_ADDR", "ARENA_HTTP_ADDR"),
		TCPAddr:        envAny(":8081", "TCP_ADDR", "ARENA_TCP_ADDR"),
		UDPAddr:        envOpt(":8082", "UDP_ADDR", "ARENA_UDP_ADDR"),
		RedisAddr:      envAny("", "REDIS_ADDR", "ARENA_REDIS_ADDR"),
		RedisPassword:  envAny("", "REDIS_PASSWORD"),
		RedisTimeout:   envDuration("REDIS_TIMEOUT", time.Second),
		KafkaTopic:     envAny("arena.events", "KAFKA_TOPIC", "ARENA_KAFKA_TOPIC"),
		AdminToken:     envAny("", "ADMIN_TOKEN", "ARENA_ADMIN_TOKEN"),
		JWTSecret:      envAny("", "JWT_SECRET", "ARENA_JWT_SECRET"),
		JWTTTL:         envDuration("JWT_TTL", 24*time.Hour),
		Env:            envAny("development", "ENV", "ARENA_ENV"),
		TrustProxy:     envBool("TRUST_PROXY", false),
		QueueRateLimit: envInt("QUEUE_RATE_LIMIT", 0),
		LoginRateLimit: envInt("LOGIN_RATE_LIMIT", 30),
		HelloRateLimit: envInt("HELLO_RATE_LIMIT", 60),
		// 120 messages a second is six times what a 20 Hz client sends, so it
		// takes a deliberately misbehaving client to reach it.
		ConnMsgRate:   envInt("CONN_MSG_RATE", 120),
		ConnMsgBurst:  envInt("CONN_MSG_BURST", 240),
		ConnByteRate:  envInt("CONN_BYTE_RATE", 128<<10),
		ConnByteBurst: envInt("CONN_BYTE_BURST", 256<<10),
		// Kept in step with room.DefaultInputBuffer, which is where the
		// reasoning for the number lives; config does not import room, and a
		// package of knobs should not start depending on the packages it
		// configures.
		InputBuffer:       envInt("INPUT_BUFFER", 2),
		SnapshotRate:      envInt("SNAPSHOT_RATE", 0),
		WarmupTimeout:     envDuration("WARMUP_TIMEOUT", 5*time.Second),
		MaxRooms:          envInt("MAX_ROOMS", 0),
		PlatformDSN:       envAny("", "PLATFORM_DSN"),
		PlatformMaxConns:  envInt("PLATFORM_MAX_CONNS", 16),
		PlatformTimeout:   envDuration("PLATFORM_TIMEOUT", 2*time.Second),
		PlatformRateLimit: envInt("PLATFORM_RATE_LIMIT", 60),
		ReplayDir:         envAny("", "REPLAY_DIR"),
		MaxReplays:        envInt("MAX_REPLAYS", 64),
		ShutdownTimeout:   envDuration("SHUTDOWN_TIMEOUT", envDuration("ARENA_SHUTDOWN_WAIT", 15*time.Second)),
		DrainTimeout:      envDuration("DRAIN_TIMEOUT", envDuration("ARENA_DRAIN_TIMEOUT", 45*time.Second)),
	}
	if s.Production() {
		if !envSet("TRUST_PROXY") && !envSet("ARENA_TRUST_PROXY") {
			s.TrustProxy = true
		}
		// Only a node that actually hosts matches needs a ceiling; warning a
		// gateway about one would be noise, and noise is how a real warning
		// gets ignored.
		if s.MaxRooms == 0 && (s.Role == pb.Role_ROLE_ALL || s.Role == pb.Role_ROLE_GAME_SERVER) {
			log.Printf("WARNING: MAX_ROOMS=0 — this node advertises no room ceiling, " +
				"so placement will keep giving it matches after its tick budget is gone.")
		}
		if s.QueueRateLimit == 0 && !envSet("QUEUE_RATE_LIMIT") {
			s.QueueRateLimit = 12
		}
		s.warnDisabledLimits()
	}
	if envBool("PPROF", false) || envBool("ARENA_PPROF", false) {
		s.PprofEnabled = true
	}
	if origins := envAny("", "ALLOWED_ORIGINS", "ARENA_ALLOWED_ORIGINS"); origins != "" {
		s.AllowedOrigins = split(origins)
	}
	if brokers := envAny("", "KAFKA_BROKERS", "ARENA_KAFKA_BROKERS"); brokers != "" {
		s.KafkaBrokers = split(brokers)
	}
	return s
}

// warnDisabledLimits says so, loudly, when production is running with a
// limiter switched off.
//
// Zero is the documented way to disable one, and load testing needs exactly
// that: every bot dials from a single client IP, so the per-IP limiters would
// otherwise cap the whole fleet. That makes `HELLO_RATE_LIMIT=0` a line people
// copy out of a load-test recipe — and pasted into a production manifest it
// silently removes the only thing standing between the gateway and a connection
// flood. Refusing to start would be worse: there are real deployments that
// terminate abuse upstream and genuinely want these off. So it boots, and says
// what it did.
func (s Static) warnDisabledLimits() {
	limits := []struct {
		env string
		v   int
	}{
		{"HELLO_RATE_LIMIT", s.HelloRateLimit},
		{"QUEUE_RATE_LIMIT", s.QueueRateLimit},
		{"LOGIN_RATE_LIMIT", s.LoginRateLimit},
		{"TURN_RATE_LIMIT", s.TurnRateLimit},
		{"CONN_MSG_RATE", s.ConnMsgRate},
	}
	// Only worth a line when there is a database behind those endpoints to
	// protect. Warning about a limiter on routes that are not mounted is the
	// kind of noise that teaches people to skim the warnings.
	if s.PlatformEnabled() {
		limits = append(limits, struct {
			env string
			v   int
		}{"PLATFORM_RATE_LIMIT", s.PlatformRateLimit})
	}
	for _, l := range limits {
		if l.v <= 0 {
			log.Printf("WARNING: %s=0 disables that rate limiter, and ENV=%s. "+
				"If this came from a load-test recipe, it does not belong here.", l.env, s.Env)
		}
	}
}

// Validate refuses to start a production process that is missing something it
// cannot safely run without.
//
// The rate limiters warn rather than fail because there are real deployments
// that terminate abuse upstream. Authentication is not like that. With
// JWT_SECRET unset the server accepts any name from anyone and hands out a
// session on the spot: there is no deployment where that is the intent in
// production, and warning about it means it ships. Failing to boot is a
// deployment that rolls back; a warning is a line in a log nobody reads until
// after the incident.
func (s Static) Validate() error {
	if !s.Production() {
		return nil
	}
	if s.JWTSecret == "" {
		return errors.New("ENV=production with no JWT_SECRET: authentication would be disabled, " +
			"and any client could claim any identity. Set JWT_SECRET, or run with ENV=development")
	}
	if s.AdminToken == "" {
		// Not fatal: with no token the admin endpoints deny everything in
		// production, which is safe — just unusable.
		log.Printf("WARNING: ADMIN_TOKEN is unset, so /admin/config and pprof will refuse every request.")
	}
	if s.PlatformMemory() {
		// Not fatal: a deployment may genuinely want the platform endpoints up
		// with nothing behind them — a staging tier, a smoke test. It is worth
		// a loud line anyway, because the failure mode is silent and delayed.
		// Everything a player registers, earns or buys is gone at the next
		// restart, and nobody finds out until the restart.
		log.Printf("WARNING: PLATFORM_DSN=memory with ENV=%s — accounts, wallets and "+
			"match history are held in RAM and are lost on restart. Point it at Postgres.", s.Env)
	}
	if strings.Contains(s.PublicAddr, "localhost") || strings.Contains(s.PublicAddr, "127.0.0.1") {
		log.Printf("WARNING: PUBLIC_ADDR=%s in production — clients are handed this address to reach "+
			"the game server directly, and they are not on this host.", s.PublicAddr)
	}
	return nil
}

// PlatformEnabled reports whether the platform tier is switched on at all.
func (s Static) PlatformEnabled() bool { return s.PlatformDSN != "" }

// PlatformMemory reports the no-database mode. See PlatformDSN.
func (s Static) PlatformMemory() bool {
	return strings.EqualFold(s.PlatformDSN, "memory")
}

func (s Static) Production() bool {
	return strings.EqualFold(s.Env, "production") || strings.EqualFold(s.Env, "prod")
}

func ParseRole(s string) pb.Role {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "gateway":
		return pb.Role_ROLE_GATEWAY
	case "matchmaker":
		return pb.Role_ROLE_MATCHMAKER
	case "gameserver", "game_server", "gs":
		return pb.Role_ROLE_GAME_SERVER
	default:
		return pb.Role_ROLE_ALL
	}
}

func LoadViewFromEnv() View {
	tick := envInt("TICK_RATE", envInt("ARENA_TICK_RATE", 20))
	if tick != 20 && tick != 30 && tick != 60 {
		tick = 20
	}
	v := View{
		TickRate:        tick,
		RoomSize:        envInt("ROOM_SIZE", envInt("ARENA_ROOM_SIZE", 8)),
		MinPlayers:      envInt("MIN_PLAYERS", envInt("ARENA_MIN_ROOM_SIZE", 1)),
		QueueTimeout:    envDuration("QUEUE_TIMEOUT", envDuration("ARENA_QUEUE_WAIT", 2*time.Second)),
		MatchSeconds:    envInt("MATCH_SECONDS", envInt("ARENA_MATCH_SECONDS", 90)),
		DisconnectGrace: envDuration("DISCONNECT_GRACE", 15*time.Second),
		SendBuffer:      envInt("SEND_BUFFER", 32),
		MaxCCU:          envInt("MAX_CCU", 12000),
		// Two hundred points is close company; a hundred more per second of
		// waiting means nobody is held back for long, and the cap keeps the
		// widest match still recognisable as a match.
		SkillWindow:    envInt("SKILL_WINDOW", 200),
		SkillWiden:     envInt("SKILL_WIDEN", 100),
		SkillMaxWindow: envInt("SKILL_MAX_WINDOW", 1200),
	}
	v.clamp()
	return v
}

func envAny(def string, keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return def
}

// envOpt is envAny for a setting whose empty value means something.
//
// envAny cannot express "set, and deliberately blank": it treats an empty value
// as an absent one and hands back the default. That is right for a name or a
// secret, and wrong for UDP_ADDR, where blank is documented as "do not listen"
// — and where falling back to :8082 means a process told not to open a UDP
// socket opens one anyway, then fails to bind and exits if anything else
// already holds the port. Found by hitting exactly that.
func envOpt(def string, keys ...string) string {
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			return v
		}
	}
	return def
}

func envSet(keys ...string) bool {
	for _, k := range keys {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	return def
}

func envBool(k string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func split(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// resolveWebDir finds the frontend directory. The Go module lives in backend/,
// so the server may be started either from the repo root or from backend/;
// WEB_DIR wins when set (Docker does), otherwise try both layouts.
func resolveWebDir() string {
	if v := os.Getenv("WEB_DIR"); v != "" {
		return v
	}
	for _, cand := range []string{"web", "../web"} {
		if fi, err := os.Stat(cand); err == nil && fi.IsDir() {
			return cand
		}
	}
	return "web"
}
