package app

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/nguyenbatam/arena_game_server/gen/pb"
	"github.com/nguyenbatam/arena_game_server/internal/auth"
	"github.com/nguyenbatam/arena_game_server/internal/cluster"
	"github.com/nguyenbatam/arena_game_server/internal/config"
	"github.com/nguyenbatam/arena_game_server/internal/events"
	"github.com/nguyenbatam/arena_game_server/internal/matchmaking"
	"github.com/nguyenbatam/arena_game_server/internal/metrics"
	"github.com/nguyenbatam/arena_game_server/internal/net/httputil"
	tcpsrv "github.com/nguyenbatam/arena_game_server/internal/net/tcp"
	udpsrv "github.com/nguyenbatam/arena_game_server/internal/net/udp"
	wssrv "github.com/nguyenbatam/arena_game_server/internal/net/ws"
	"github.com/nguyenbatam/arena_game_server/internal/notify"
	"github.com/nguyenbatam/arena_game_server/internal/placement"
	"github.com/nguyenbatam/arena_game_server/internal/platform"
	"github.com/nguyenbatam/arena_game_server/internal/presence"
	"github.com/nguyenbatam/arena_game_server/internal/protocol"
	"github.com/nguyenbatam/arena_game_server/internal/ratelimit"
	"github.com/nguyenbatam/arena_game_server/internal/rating"
	"github.com/nguyenbatam/arena_game_server/internal/replay"
	"github.com/nguyenbatam/arena_game_server/internal/room"
	"github.com/nguyenbatam/arena_game_server/internal/safe"
	"github.com/nguyenbatam/arena_game_server/internal/session"
	"github.com/nguyenbatam/arena_game_server/internal/sim"
	"github.com/nguyenbatam/arena_game_server/internal/turn"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

type App struct {
	cfg config.Static
	dyn *config.Live

	hub      *session.Hub
	rooms    *room.Manager
	queue    matchmaking.Queue
	registry cluster.Registry
	jobs     placement.Queue
	notify   notify.Bus
	presence presence.Store
	bus      events.Bus
	rdb      *redis.Client
	jwt      *auth.JWT
	rating   rating.Store
	// platform is the durable half — accounts, wallets, inventory, history and
	// the ladder. Nil when PLATFORM_DSN is unset, and every use of it is
	// guarded: the realtime tier ran without one before this existed and still
	// has to, because a demo has to come up from a clone with no database.
	platform   *platform.Service
	replays    *replay.Store
	turn       *turn.Service
	turnPair   turn.Pairing
	turnHub    *turnHub
	queueRL    *ratelimit.Window
	loginRL    *ratelimit.Window
	platformRL *ratelimit.Window
	helloRL    *ratelimit.Window
	turnRL     *ratelimit.Window
	// mkBudget is built once and handed to every connection that needs one. A
	// method value would allocate a closure per message on the read path.
	mkBudget func() *ratelimit.Budget

	// coord throttles the coordination-error log. See App.soft.
	coord coordLog

	httpServer *http.Server
	tcpLn      net.Listener
	udpPC      net.PacketConn
	ready      atomic.Bool
	draining   atomic.Bool
}

func New(cfg config.Static, dyn *config.Live) (*App, error) {
	a := &App{
		cfg:     cfg,
		dyn:     dyn,
		hub:     session.NewHub(),
		rooms:   room.NewManager(),
		turnHub: newTurnHub(),
	}

	self := &pb.GameServer{Id: cfg.NodeID, PublicAddr: cfg.PublicAddr}

	if cfg.RedisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
		var pingErr error
		for i := 0; i < 30; i++ {
			pingCtx, cancel := bounded(context.Background(), cfg.RedisTimeout)
			pingErr = rdb.Ping(pingCtx).Err()
			cancel()
			if pingErr == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if pingErr != nil {
			return nil, fmt.Errorf("redis: %w", pingErr)
		}
		// The hot-reloadable config rides the same client as everything else.
		// It used to have its own, dialled in main purely to hand to NewLive —
		// a second pool against the same server, never closed.
		loadCtx, cancel := bounded(context.Background(), cfg.RedisTimeout)
		dyn.UseRedis(loadCtx, rdb)
		cancel()
		a.queue = matchmaking.NewRedis(rdb)
		a.registry = cluster.NewRedis(rdb)
		a.jobs = placement.NewRedis(rdb)
		a.notify = notify.NewRedis(rdb)
		a.presence = presence.NewRedis(rdb)
		a.rating = rating.NewRedis(rdb)
		a.rdb = rdb
		log.Printf("redis enabled @ %s", cfg.RedisAddr)
	} else {
		a.queue = matchmaking.NewMemory()
		a.registry = cluster.NewMemory(self)
		a.jobs = placement.NewMemory()
		a.notify = notify.NewMemory()
		a.presence = presence.NewMemory()
		a.rating = rating.NewMemory()
		log.Printf("in-memory mode (set REDIS_ADDR for distributed)")
	}

	// Turn-based mode shares the connection, auth and session layers with the
	// arena and swaps only the match model: event log instead of tick loop.
	ttl := turn.TTL{Live: cfg.TurnLiveTTL, Ended: cfg.TurnEndedTTL}
	var tstore turn.Store
	var tdeadlines turn.Deadlines
	if a.rdb != nil {
		tstore, tdeadlines = turn.NewRedisStore(a.rdb, ttl), turn.NewRedisDeadlines(a.rdb)
		a.turnPair = turn.NewRedisPairing(a.rdb)
	} else {
		tstore, tdeadlines = turn.NewMemoryStore(ttl), turn.NewMemoryDeadlines()
		a.turnPair = turn.NewMemoryPairing()
	}
	a.turn = turn.NewService(turn.Options{
		Store: tstore, Deadlines: tdeadlines,
		Notify:    a.turnNotify,
		OnEnd:     a.onTurnEnd,
		TurnLimit: cfg.TurnLimit,
		OpTimeout: cfg.RedisTimeout,
	})

	if len(cfg.KafkaBrokers) > 0 {
		a.bus = events.NewKafka(cfg.KafkaBrokers, cfg.KafkaTopic)
		log.Printf("kafka enabled brokers=%v topic=%s", cfg.KafkaBrokers, cfg.KafkaTopic)
	} else {
		a.bus = events.LogBus{}
	}
	if cfg.JWTSecret != "" {
		j, err := auth.NewJWT(cfg.JWTSecret, cfg.JWTTTL)
		if err != nil {
			return nil, err
		}
		a.jwt = j
		log.Printf("jwt auth enabled ttl=%s", cfg.JWTTTL)
	}
	if cfg.QueueRateLimit > 0 {
		a.queueRL = ratelimit.NewWindow(cfg.QueueRateLimit, time.Minute)
	}
	if cfg.LoginRateLimit > 0 {
		a.loginRL = ratelimit.NewWindow(cfg.LoginRateLimit, time.Minute)
	}
	if cfg.PlatformRateLimit > 0 {
		a.platformRL = ratelimit.NewWindow(cfg.PlatformRateLimit, time.Minute)
	}
	if cfg.HelloRateLimit > 0 {
		a.helloRL = ratelimit.NewWindow(cfg.HelloRateLimit, time.Minute)
	}
	if cfg.TurnRateLimit > 0 {
		a.turnRL = ratelimit.NewWindow(cfg.TurnRateLimit, time.Minute)
	}
	if cfg.PlatformEnabled() {
		if err := a.startPlatform(cfg); err != nil {
			return nil, err
		}
	}
	replays, err := replay.NewStore(cfg.ReplayDir, cfg.MaxReplays)
	if err != nil {
		return nil, err
	}
	a.replays = replays
	if replays != nil {
		log.Printf("replay recording enabled dir=%s sample=%d concurrent", cfg.ReplayDir, cfg.MaxReplays)
	}
	a.mkBudget = func() *ratelimit.Budget {
		return ratelimit.NewBudget(cfg.ConnMsgRate, cfg.ConnMsgBurst, cfg.ConnByteRate, cfg.ConnByteBurst)
	}
	return a, nil
}

// routes builds the HTTP surface. Split out of Run so a test can exercise the
// real mux — which endpoints exist, and on which methods — rather than a second
// copy of the routing table that drifts from this one.
func (a *App) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wssrv.Serve(a.hub, a.sendBuf, a.cfg.AllowedOrigins, a.cfg.TrustProxy, a.onMessage, a.onClose))
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/readyz", a.handleReadyz)
	mux.HandleFunc("/auth/login", a.handleLogin)
	a.mountPlatform(mux)
	mux.HandleFunc("/stats", a.handleStats)
	mux.HandleFunc("/admin/config", a.handleConfig)
	if a.cfg.PprofEnabled {
		mux.Handle("/debug/pprof/", a.adminHTTP(http.HandlerFunc(pprof.Index)))
		mux.Handle("/debug/pprof/cmdline", a.adminHTTP(http.HandlerFunc(pprof.Cmdline)))
		mux.Handle("/debug/pprof/profile", a.adminHTTP(http.HandlerFunc(pprof.Profile)))
		mux.Handle("/debug/pprof/symbol", a.adminHTTP(http.HandlerFunc(pprof.Symbol)))
		mux.Handle("/debug/pprof/trace", a.adminHTTP(http.HandlerFunc(pprof.Trace)))
		mux.Handle("/debug/pprof/heap", a.adminHTTP(pprof.Handler("heap")))
		mux.Handle("/debug/pprof/goroutine", a.adminHTTP(pprof.Handler("goroutine")))
		mux.Handle("/debug/pprof/mutex", a.adminHTTP(pprof.Handler("mutex")))
		mux.Handle("/debug/pprof/block", a.adminHTTP(pprof.Handler("block")))
	}
	mux.Handle("/", http.FileServer(http.Dir(a.cfg.WebDir)))
	return mux
}

func (a *App) Run(ctx context.Context) error {
	a.ready.Store(true)
	view := a.dyn.Get()
	mux := a.routes()
	a.httpServer = &http.Server{
		Addr:              a.cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	safe.Go("http.serve", func() {
		log.Printf("http %s role=%s node=%s tick=%d", a.cfg.HTTPAddr, protocol.RoleString(a.cfg.Role), a.cfg.NodeID, view.TickRate)
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http: %v", err)
		}
	})

	safe.GoLoop(ctx, "config.watch", a.dyn.Watch)

	role := a.cfg.Role
	if role == pb.Role_ROLE_ALL || role == pb.Role_ROLE_GATEWAY {
		// A gateway that cannot subscribe is a gateway no match will ever
		// reach: assignments are how a formed match finds the node holding the
		// player. Failing to start is the honest answer — the alternative is a
		// process that passes its readiness check and silently drops every
		// player who queues on it.
		if err := a.notify.Subscribe(ctx, a.onAssignment); err != nil {
			return fmt.Errorf("subscribe assignments: %w", err)
		}
		// Turn updates decided elsewhere land here — see App.turnNotify.
		if err := a.notify.SubscribeTurn(ctx, a.cfg.NodeID, a.onTurnPush); err != nil {
			return fmt.Errorf("subscribe turn pushes: %w", err)
		}
	}
	if role == pb.Role_ROLE_ALL || role == pb.Role_ROLE_GAME_SERVER {
		// Soft: heartbeatLoop below repeats this every two seconds, so a first
		// one that does not land delays placement onto this node rather than
		// losing it.
		a.soft("registry.heartbeat", a.registry.Heartbeat(ctx, &pb.GameServer{Id: a.cfg.NodeID, PublicAddr: a.cfg.PublicAddr}))
		safe.GoLoop(ctx, "gs.jobs", a.jobLoop)
		safe.GoLoop(ctx, "gs.job_reaper", a.jobReaper)
		safe.GoLoop(ctx, "gs.heartbeat", a.heartbeatLoop)
	}
	if role == pb.Role_ROLE_ALL || role == pb.Role_ROLE_MATCHMAKER {
		safe.GoLoop(ctx, "matchmaker", a.matchLoop)
		// Turn deadlines are swept here rather than on the game server: with no
		// tick loop there is nothing counting down, and the pop is atomic so
		// several replicas may sweep at once.
		safe.GoLoop(ctx, "turn.deadlines", func(ctx context.Context) { a.turn.RunDeadlines(ctx, time.Second) })
	}

	if role == pb.Role_ROLE_ALL || role == pb.Role_ROLE_GATEWAY || role == pb.Role_ROLE_GAME_SERVER {
		ln, err := net.Listen("tcp", a.cfg.TCPAddr)
		if err != nil {
			return err
		}
		a.tcpLn = ln
		safe.Go("tcp.serve", func() { tcpsrv.Serve(ln, a.hub, a.sendBuf, a.onMessage, a.onClose) })
		log.Printf("tcp %s (length-prefixed protobuf)", a.cfg.TCPAddr)

		if a.cfg.UDPAddr != "" {
			pc, err := net.ListenPacket("udp", a.cfg.UDPAddr)
			if err != nil {
				return err
			}
			a.udpPC = pc
			safe.Go("udp.serve", func() {
				udpsrv.Serve(pc, a.hub, a.sendBuf, a.onMessage, a.onClose, udpsrv.DefaultIdle)
			})
			log.Printf("udp %s (one datagram = one protobuf envelope)", a.cfg.UDPAddr)
		}
	}

	<-ctx.Done()
	return a.shutdown()
}

// op bounds one coordination call made on a connection's behalf.
//
// Every store this gateway talks to — presence, the queue, the room directory,
// the turn log — is a lookup off the tick path, and all of them used to be
// called with context.Background(): no deadline at all. A Redis that stops
// answering then parks the goroutine of every player logging in, joining or
// disconnecting, and the process runs out of them long before anything reports
// a problem. A ceiling turns that outage into slow logins and failed lookups,
// which the callers already handle.
//
// The budget is deliberately generous: it is a ceiling on the worst case, not
// a target, and REDIS_TIMEOUT moves it.
//
// It applies in in-memory mode too, where nothing can block and the deadline
// can therefore only ever fire spuriously. That is on purpose: one rule for
// every store is worth more than shaving a timer off calls that return in
// nanoseconds, and gating it on REDIS_ADDR would have said nothing useful about
// the Kafka publishes that go through here as well.
func (a *App) op() (context.Context, context.CancelFunc) {
	return a.opIn(context.Background())
}

// opIn is op for a caller that already holds a context worth inheriting — a
// background loop that must also stop at shutdown, or an HTTP request that must
// stop when the client goes away.
func (a *App) opIn(parent context.Context) (context.Context, context.CancelFunc) {
	return bounded(parent, a.cfg.RedisTimeout)
}

// bounded applies a deadline, treating a non-positive one as "no deadline" —
// which is what an in-memory store wants, and what a test that builds its own
// config.Static gets by leaving the field unset.
func bounded(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, d)
}

// sendBuf is read per accepted connection rather than once at startup, so a
// send_buffer pushed over /admin/config reaches everything that connects after
// it. Live connections keep the depth they were built with: resizing a channel
// under a writer means moving the messages already queued in it, and the buffer
// only has to be right for the connection's remaining life, not its whole life.
func (a *App) sendBuf() int { return a.dyn.Get().SendBuffer }

func (a *App) shutdown() error {
	log.Printf("graceful shutdown drain=%s", a.cfg.DrainTimeout)
	a.draining.Store(true)
	a.ready.Store(false)

	// Leave the placement pool before waiting for rooms to finish. The
	// heartbeat record would otherwise stay valid for its full TTL, and a
	// matchmaker would keep sending matches to a process that is on its way
	// out — for fifteen seconds, which is long enough to strand a lot of
	// players on a server that will never start their room.
	if a.registry != nil {
		ctx, cancel := a.op()
		a.soft("registry.unregister", a.registry.Unregister(ctx, a.cfg.NodeID))
		cancel()
	}

	if n := a.awaitRooms(time.Now().Add(a.cfg.DrainTimeout)); n > 0 {
		log.Printf("drain timeout: %d rooms still active", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()
	if a.httpServer != nil {
		closeErr("http server", a.httpServer.Shutdown(ctx))
	}
	if a.tcpLn != nil {
		closeErr("tcp listener", a.tcpLn.Close())
	}
	if a.udpPC != nil {
		closeErr("udp socket", a.udpPC.Close())
	}
	a.rooms.StopAll()
	// Stop only asks. Waiting for the rooms to actually unwind is what makes
	// the asking mean anything, because everything a match owes the rest of the
	// system happens on the way out: Run's deferred closeRecorder writes the
	// replay file, and OnEnd records the result and unregisters the room.
	//
	// Without the wait the process could exit in the middle of any of that.
	// Observed rather than reasoned about: a SIGTERM during a match left
	// zero-byte .arnr files behind — os.WriteFile had created the file and the
	// process died before the bytes landed — and cmd/replay reports one of
	// those as an error, which is worse than the recording simply not existing.
	//
	// Bounded by what is left of the shutdown budget, so a room that will not
	// stop cannot hold the process open indefinitely.
	if n := a.awaitRooms(time.Now().Add(a.cfg.ShutdownTimeout)); n > 0 {
		log.Printf("shutdown: %d rooms did not unwind; their recordings and results may be incomplete", n)
	}
	a.hub.CloseAll()
	closeErr("event bus", a.bus.Close())
	if a.platform != nil {
		// Last, and the ordering buys less than it looks like, so it is worth
		// being exact about what survives.
		//
		// sql.DB.Close refuses new queries and then waits for the ones already
		// executing on the server, so a RecordMatch transaction in flight right
		// now finishes. What it does not cover is a result that has not reached
		// the database yet.
		//
		// awaitRooms above now waits for every room goroutine to unwind, which
		// closes the window on anything a finishing room does on its way out —
		// a match that reached its last tick during the drain has run OnEnd,
		// and therefore recordResult, before this line.
		//
		// Two holes are left, and both are lifecycle questions rather than
		// shutdown ones. A room that is *stopped* rather than finished never
		// calls OnEnd at all, so a match cut off by the drain records no result
		// — which is defensible, since its result would be a mid-match one, but
		// it is a decision and not an accident. And onTurnEnd hands its write
		// to a goroutine on purpose, which nothing here waits for; that write
		// is idempotent on the match id, so the answer there is the outbox this
		// repo already argues for rather than another wait.
		closeErr("platform store", a.platform.Close())
	}
	return nil
}

// awaitRooms waits for every room on this node to finish, and reports how many
// are still going when the deadline passes.
//
// A room is removed from the manager by its own goroutine, after Run returns —
// so a count of zero means every tick loop has unwound and every deferred
// close, recording and result has already run. That is the property both
// callers want, and it is why this polls the manager rather than the rooms.
func (a *App) awaitRooms(deadline time.Time) int {
	for a.rooms.Count() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	return a.rooms.Count()
}

func (a *App) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !a.ready.Load() || a.draining.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	if a.rdb != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := a.rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (a *App) adminHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.adminAuthorized(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *App) adminAuthorized(r *http.Request) bool {
	if a.cfg.AdminToken == "" {
		return !a.cfg.Production()
	}
	// Constant time, for the same reason udp.cookies uses hmac.Equal: this is a
	// secret compared against attacker-supplied bytes, and == returns on the
	// first differing byte. That turns the token into something guessable a
	// character at a time by anyone who can measure the response, and what it
	// guards is /admin/config — the live tick rate, room size and CCU ceiling
	// for the whole fleet — plus every pprof endpoint.
	//
	// The header is compared whole rather than sliced past the scheme, so a
	// short or missing one takes the same path as a wrong one.
	want := "Bearer " + a.cfg.AdminToken
	got := r.Header.Get("Authorization")
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (a *App) onMessage(c *session.Conn, raw []byte) {
	// One bad message belongs to one client. The recover is written out here
	// rather than wrapped in safe.Do because this runs on every input frame of
	// every connection — twenty times a second each — and a closure per call is
	// an allocation the tick path does not need to pay for.
	defer func() {
		if v := recover(); v != nil {
			metrics.Panics.WithLabelValues("gateway.message").Inc()
			log.Printf("PANIC handling message: %v\n%s", v, debug.Stack())
		}
	}()

	if !a.allowConn(c, len(raw)) {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_RATE_LIMITED, "message rate exceeded"))
		c.Close()
		return
	}

	env, err := protocol.UnmarshalEnv(raw)
	if err != nil {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_BAD_PAYLOAD, "bad protobuf"))
		return
	}
	switch env.Type {
	case pb.MsgType_MSG_TYPE_HELLO:
		a.onHello(c, env.GetHello())
	case pb.MsgType_MSG_TYPE_JOIN_QUEUE:
		a.onJoinQueue(c)
	case pb.MsgType_MSG_TYPE_JOIN_ROOM:
		jr := env.GetJoinRoom()
		if jr == nil {
			return
		}
		c.SetLastTick(jr.LastAckTick)
		a.joinRoom(c, jr.RoomId, jr.YourId)
	case pb.MsgType_MSG_TYPE_INPUT:
		roomID, seat := c.Room()
		if roomID == "" || seat == 0 {
			return
		}
		r := a.rooms.Get(roomID)
		if r == nil {
			return
		}
		in := protocol.InputFromEnv(env, seat)
		r.SubmitInput(in)
	case pb.MsgType_MSG_TYPE_TURN_JOIN:
		a.onTurnJoin(c, env.GetTurnJoin())
	case pb.MsgType_MSG_TYPE_TURN_PLAY:
		a.onTurnPlay(c, env.GetTurnPlay())
	case pb.MsgType_MSG_TYPE_TURN_SYNC:
		a.onTurnSync(c, env.GetTurnSync())
	case pb.MsgType_MSG_TYPE_PING:
		nonce := uint64(0)
		if p := env.GetPing(); p != nil {
			nonce = p.Nonce
		}
		c.Send(protocol.Pong(nonce, uint32(time.Now().UnixMilli())))
	}
}

func (a *App) onHello(c *session.Conn, h *pb.Hello) {
	if !a.allowRate(a.helloRL, c.RemoteIP) {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_RATE_LIMITED, "too many connections"))
		c.Close()
		return
	}
	if !protocol.AcceptVersion(h.GetProtocolVersion()) {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
			fmt.Sprintf("client speaks v%d, this server speaks v%d", h.GetProtocolVersion(), protocol.Version)))
		c.Close()
		return
	}
	if err := a.authenticate(c, h); err != nil {
		metrics.AuthFailures.Inc()
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_UNAUTHORIZED, "unauthorized"))
		c.Close()
		return
	}
	view := a.dyn.Get()
	// Count already includes this connection: the transport files it in the hub
	// before HELLO, so the ceiling is reached at Count == MaxCCU and exceeded
	// only above it.
	if view.MaxCCU > 0 && a.hub.Count() > view.MaxCCU {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_UNSPECIFIED, "server full"))
		c.Close()
		return
	}
	connID := c.ID()
	c.Send(protocol.Welcome(connID, c.PlayerID(), view.TickRate))

	ctx, cancel := a.op()
	defer cancel()

	// After authenticate the conn is filed under its durable id, so that is the
	// presence key. A client may still name an older session explicitly; only
	// consult it when the durable id has no presence of its own.
	pid := connID
	if h != nil && h.SessionId != "" {
		pid = h.SessionId
	}
	prev, err := a.presence.Get(ctx, connID)
	a.soft("presence.get", err)
	if prev == nil && pid != connID {
		var err error
		prev, err = a.presence.Get(ctx, pid)
		a.soft("presence.get", err)
	}
	if prev != nil && prev.Status == pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED && prev.RoomId != "" {
		if prev.Seen > 0 && time.Since(time.Unix(prev.Seen, 0)) > view.DisconnectGrace {
			a.soft("presence.delete", a.presence.Delete(ctx, prev.PlayerId))
		} else if a.resumeMatch(c, prev, view) {
			return
		}
	}
	a.soft("presence.set", a.presence.Set(ctx, &pb.Presence{
		PlayerId: connID, Name: c.Name(), NodeId: a.cfg.NodeID, Status: pb.PresenceStatus_PRESENCE_STATUS_ONLINE,
	}))
}

func (a *App) authenticate(c *session.Conn, h *pb.Hello) error {
	if a.jwt == nil {
		if h != nil && h.Name != "" {
			c.SetName(h.Name)
		}
		if c.Name() == "" {
			c.SetName("player")
		}
		if h != nil && h.SessionId != "" && h.SessionId != c.ID() {
			if existing := a.hub.Get(h.SessionId); existing != nil && existing != c {
				existing.Close()
			}
			a.hub.Rekey(c, h.SessionId)
		}
		return nil
	}
	token := c.Token
	if h != nil && h.GetAccessToken() != "" {
		token = h.GetAccessToken()
	}
	claims, err := a.jwt.Verify(token)
	if err != nil {
		return err
	}
	if h != nil && h.SessionId != "" && h.SessionId != claims.PlayerID {
		return auth.ErrInvalid
	}
	a.hub.Rekey(c, claims.PlayerID)
	if h != nil && h.Name != "" {
		c.SetName(h.Name)
	} else {
		c.SetName(claims.Name)
	}
	return nil
}

// allowConn charges one message against this connection's own allowance.
//
// Checked before the protobuf is unmarshalled, because the unmarshal is the
// expensive half: an authenticated client sending inputs as fast as the link
// allows costs the server a parse per message, and the room's input channel
// only drops them afterwards. The per-IP limiters above cannot help here —
// they guard the doors, and this client is already inside.
func (a *App) allowConn(c *session.Conn, size int) bool {
	if a.cfg.ConnMsgRate <= 0 && a.cfg.ConnByteRate <= 0 {
		return true
	}
	if c.Budget(a.mkBudget).Allow(size) {
		return true
	}
	metrics.RateLimited.Inc()
	return false
}

func (a *App) allowRate(lim *ratelimit.Window, key string) bool {
	if lim == nil || key == "" {
		return true
	}
	if lim.Allow(key) {
		return true
	}
	metrics.RateLimited.Inc()
	return false
}

func (a *App) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if a.jwt == nil {
		http.Error(w, "auth disabled", http.StatusNotFound)
		return
	}
	ip := httputil.RealIP(r, a.cfg.TrustProxy)
	if !a.allowRate(a.loginRL, ip) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	var req struct {
		Name     string `json:"name"`
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !readJSON(w, r, &req) {
		return
	}

	// With the platform tier on, a login is an authentication rather than a
	// formality, and the id in the token belongs to an account that already
	// existed. Without it this endpoint does what it always did: hand a token
	// to whoever asks, under a fresh random id that means nothing tomorrow.
	//
	// The two are not offered side by side on purpose. An anonymous fallback
	// next to a credentialled login is a way to get a session without one, and
	// every read the platform serves is authorised by exactly this token.
	if a.platform != nil {
		started := time.Now()
		ctx, cancel := a.platformOp(r)
		defer cancel()
		acc, err := a.platform.Login(ctx, req.Username, req.Password)
		if err != nil {
			a.platformErr(w, "login", started, err)
			return
		}
		a.issueSession(w, "login", started, acc)
		return
	}

	tok, pid, exp, err := a.jwt.Issue(strings.TrimSpace(req.Name))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	writeJSONErr("login", json.NewEncoder(w).Encode(map[string]any{
		"token": tok, "player_id": pid, "expires_at": exp.Unix(),
	}))
}

// resumeMatch puts a reconnecting player back where they were, and reports
// whether there was anywhere to put them.
//
// A presence record outlives the match it names. It is written when the socket
// drops and kept for the disconnect grace window, and the match can perfectly
// well finish inside that window — which is the ordinary case for somebody
// whose connection failed in the closing seconds of a game. Announcing
// MATCH_FOUND for it and then failing to join answered that reconnect with
// ROOM_NOT_ON_NODE, and returned before presence was set back to online, so the
// next attempt found the same stale record and did the same thing, for the
// whole of the grace window.
//
// So the room is checked before anything is promised: the directory for a match
// that may be on another node, and the local manager for one that was never
// registered there. Neither knowing it means the match is over.
func (a *App) resumeMatch(c *session.Conn, prev *pb.Presence, view config.View) bool {
	ctx, cancel := a.op()
	defer cancel()

	info, err := a.registry.GetRoom(ctx, prev.RoomId)
	if err != nil {
		// "The directory says there is no such room" and "the directory could
		// not answer" both arrive here as a nil info, and they must not be
		// treated alike. Deleting the record on a transient lookup failure
		// throws away the only thing that knows where this player was, and the
		// match they were in goes on without them for the rest of it. So a
		// failed read declines to resume — the player is greeted as a new
		// arrival — but the record is left standing for their next attempt.
		a.soft("registry.get_room", err)
		return false
	}
	if info == nil && a.liveRoom(prev.RoomId) == nil {
		// Nothing to go back to. Drop the record so the next HELLO does not
		// walk the same path, and let the caller greet them as a new arrival.
		a.soft("presence.delete", a.presence.Delete(ctx, prev.PlayerId))
		return false
	}

	host := ""
	seed := int64(0)
	if info != nil {
		seed = info.Seed
		if info.PublicAddr != "" && info.PublicAddr != a.cfg.PublicAddr {
			host = info.PublicAddr
		}
	}
	c.Send(protocol.MatchFound(prev.RoomId, host, prev.SeatId, seed, view.TickRate))
	if host != "" {
		c.SetHandoff(true)
		return true
	}
	a.joinRoom(c, prev.RoomId, prev.SeatId)
	a.publishEvent(ctx, &pb.Event{
		Type: pb.EventType_EVENT_TYPE_PLAYER_RECONNECT, RoomId: prev.RoomId, PlayerId: c.ID(),
	})
	return true
}

func (a *App) onJoinQueue(c *session.Conn) {
	if a.cfg.Role == pb.Role_ROLE_GAME_SERVER {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_WRONG_ROLE, "join_queue on gateway"))
		return
	}
	if c.RoomID() != "" {
		return
	}
	if !a.allowRate(a.queueRL, c.RemoteIP) {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_RATE_LIMITED, "queue rate limited"))
		return
	}
	ctx, cancel := a.op()
	defer cancel()

	connID, name := c.ID(), c.Name()
	// The one coordination failure on this path that the player has to be told
	// about, rather than logged and carried past. QUEUED is a promise that
	// something is now looking for a match for them; sending it after the
	// enqueue failed leaves them watching a queue screen that can never
	// resolve, with no error and nothing to retry against. An error they can
	// act on — the client offers the button again.
	if err := a.queue.Enqueue(ctx, &pb.QueuePlayer{
		ConnId: connID, PlayerId: connID, Name: name, Skill: int32(a.ratingOf(ctx, connID)),
	}); err != nil {
		a.soft("queue.enqueue", err)
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_UNSPECIFIED, "could not join the queue, try again"))
		return
	}
	// Soft, unlike the enqueue: the player really is in the queue now, and a
	// presence record that did not write costs them a slower reconnect rather
	// than the match itself.
	a.soft("presence.set", a.presence.Set(ctx, &pb.Presence{
		PlayerId: connID, Name: name, NodeId: a.cfg.NodeID, Status: pb.PresenceStatus_PRESENCE_STATUS_QUEUED,
	}))
	c.Send(protocol.Queued())
}

// liveRoom is the room manager's answer for a room that can still take a
// player.
//
// A finished room stays in the manager until its goroutine unwinds, and seating
// somebody into one strands them: the tick loop has returned, so no snapshot is
// ever broadcast, and Room.Subscribe deliberately no longer sends one of its
// own. The old behaviour was to seat them and let the signon copy of the final
// snapshot stand in for a match — which also left the connection bound to a
// room that no longer exists. See releaseSeats.
func (a *App) liveRoom(id string) *room.Room {
	r := a.rooms.Get(id)
	if r == nil || r.Closed() {
		return nil
	}
	return r
}

// releaseSeats unbinds the connections a finished match was holding.
//
// Nothing used to clear the binding at all: Conn.roomID was written when the
// player joined and stayed written for the life of the connection. The player
// was then refused by onJoinQueue for the rest of their session — silently,
// because that guard simply returns — so "play again" on a socket that had
// already played once did nothing at all. Their disconnect also wrote a
// presence record naming a room that was over, which is the record resumeMatch
// above has to defend against.
//
// Lock-only and local on purpose. This runs on the room's own goroutine, before
// Run's cleanup has stopped counting the room, so a store round trip per seat
// here is time the node spends advertising capacity it has already given back —
// the same argument ratingsOf makes. The presence record left saying IN_MATCH
// is not worth a write to correct: no reader acts on it (onHello resumes only a
// DISCONNECTED one), and whatever the player does next overwrites it — a
// requeue writes QUEUED, a disconnect deletes it, and doing nothing lets it
// expire on its own TTL.
func (a *App) releaseSeats(req *pb.RoomRequest) {
	for _, s := range req.Seats {
		if s.ConnId == "" {
			continue
		}
		c := a.hub.Get(s.ConnId)
		if c == nil {
			continue
		}
		// Identity-checked the way onClose checks it: by the time a match ends
		// this connection may already have been replaced and seated somewhere
		// else, and clearing that binding would cut off a live match.
		if id, _ := c.Room(); id == req.RoomId {
			c.BindRoom("", 0)
		}
	}
}

func (a *App) joinRoom(c *session.Conn, roomID string, yourID uint32) {
	ctx, cancel := a.op()
	defer cancel()

	r := a.liveRoom(roomID)
	if r == nil {
		info, err := a.registry.GetRoom(ctx, roomID)
		// A failed lookup lands on the same answer as a missing entry — this
		// node does not have the room and cannot say who does — but only one of
		// the two is a fault worth seeing in a graph.
		a.soft("registry.get_room", err)
		if info != nil && info.PublicAddr != "" && info.PublicAddr != a.cfg.PublicAddr {
			c.SetHandoff(true)
			c.Send(protocol.Redirect(roomID, info.PublicAddr, yourID))
			return
		}
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_ROOM_NOT_ON_NODE, "room not on this node"))
		return
	}
	connID := c.ID()
	seat, ok := r.BindSeat(connID, yourID)
	if !ok {
		c.Send(protocol.Err(pb.ErrorCode_ERROR_CODE_ROOM_NOT_FOUND, "invalid seat"))
		return
	}
	c.BindRoom(roomID, seat)
	c.SetHandoff(false)
	// Undoes the Leave that their disconnect queued, if there was one. A join
	// that is not a reconnect finds nothing to undo, which is why this is
	// unconditional rather than guarded on the presence record — the seat is
	// the only thing either side agrees on.
	r.Rejoin(seat)
	r.Subscribe(seat, func(msg []byte) {
		c.SetLastTick(r.LastTick())
		c.Send(msg)
	})
	a.soft("presence.set", a.presence.Set(ctx, &pb.Presence{
		PlayerId: connID, Name: c.Name(), NodeId: a.cfg.NodeID,
		Status: pb.PresenceStatus_PRESENCE_STATUS_IN_MATCH, RoomId: roomID, SeatId: seat, LastTick: r.LastTick(),
	}))
}

func (a *App) onClose(c *session.Conn) {
	connID := c.ID()
	// A reconnect files the new connection under this same durable id and
	// closes this one, but a dead socket can take until its read deadline to
	// notice. By then every binding below — the queue entry, the seat
	// subscription, the presence record — belongs to the connection that
	// replaced this one, and tearing them down would cut off the player who
	// just came back. turnHub.drop makes the same check for the same reason.
	superseded := false
	if cur := a.hub.Get(connID); cur != nil && cur != c {
		superseded = true
	}

	// drop is identity-checked, so it clears the turn delivery entry only while
	// that entry still names this exact connection. That makes it safe to run
	// even for a superseded conn — and worth running: an entry left pointing at
	// a closed socket swallows every push for that player.
	id, dropped := a.turnHub.drop(c)

	if superseded {
		return
	}

	ctx, cancel := a.op()
	defer cancel()

	a.soft("queue.remove", a.queue.Remove(ctx, connID))
	if c.IsHandoff() {
		return
	}
	if dropped {
		// Unpark them too, or the next arrival is matched against a player who
		// has already left and the match opens with one seat dead.
		a.soft("turnpair.cancel", a.turnPair.Cancel(ctx, id))
	}

	roomID, seat := c.Room()
	if roomID == "" {
		a.soft("presence.delete", a.presence.Delete(ctx, connID))
		return
	}
	if r := a.rooms.Get(roomID); r != nil {
		r.Unsubscribe(seat)
		// Unsubscribe stops sending to them; Leave stops the match paying them
		// out. Without it the avatar stands there for the rest of the match as
		// a motionless target worth a kill every RespawnTicks — and those kills
		// are written into the ladder by recordResult, which cannot tell a
		// farmed one from a real one. See room.Room.Leave.
		r.Leave(seat)
	}
	tick := c.LastTick()
	a.soft("presence.set", a.presence.Set(ctx, &pb.Presence{
		PlayerId: connID, Name: c.Name(), NodeId: a.cfg.NodeID,
		Status: pb.PresenceStatus_PRESENCE_STATUS_DISCONNECTED,
		RoomId: roomID, SeatId: seat, LastTick: tick,
	}))
	a.publishEvent(ctx, &pb.Event{
		Type: pb.EventType_EVENT_TYPE_PLAYER_DISCONNECT, RoomId: roomID, PlayerId: connID, Tick: tick,
	})
}

func (a *App) onAssignment(as *pb.Assignment) {
	c := a.hub.Get(as.ConnId)
	if c == nil {
		return
	}
	c.SetPlayerID(as.PlayerId)
	c.Send(protocol.MatchFound(as.RoomId, as.Host, as.PlayerId, as.Seed, int(as.TickRate)))
	if as.Host == "" {
		a.joinRoom(c, as.RoomId, as.PlayerId)
		return
	}
	c.SetHandoff(true)
}

func (a *App) matchLoop(ctx context.Context) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if a.draining.Load() {
				continue
			}
			a.matchPass(ctx)
		}
	}
}

// maxFormsPerPass is how many matches one turn of the matchmaker will form.
//
// A pass used to form exactly one, which quietly made the matchmaker a
// throughput ceiling rather than a latency one: at the 50 ms tick below that is
// twenty matches a second per replica, and the fleet this repo is sized for —
// 10k CCU, eight to a room — is 1250 rooms. Seating a cold queue took a minute,
// and once ninety-second matches started ending in step the steady-state churn
// (1250 / 90 ≈ 14 a second) sat inside a factor of 1.5 of the ceiling, with
// nothing left for the burst that follows a wave of matches finishing together.
//
// A cap rather than "drain the queue" because the work is not free: each form
// is a Lua script that blocks Redis for as long as it runs, and each placement
// is several more round trips. This bounds one pass at 640 matches a second and
// leaves the ticker to decide the rest.
const maxFormsPerPass = 32

// matchPass is one turn of the matchmaker: read the queue, form what can be
// formed, place it. Bounded, because it talks to Redis and the loop that calls
// it fires twenty times a second — a pass that hangs must not stop the next.
func (a *App) matchPass(parent context.Context) {
	ctx, cancel := a.opIn(parent)
	defer cancel()

	d := a.dyn.Get()
	n, err := a.queue.Depth(ctx)
	if err != nil {
		// Reported rather than discarded, and the gauge is left alone. Setting
		// it to the zero a failed read hands back would draw an empty queue
		// through an outage, which is the one moment the graph is being watched
		// — and "no queue" and "cannot see the queue" want opposite responses.
		a.soft("queue.depth", err)
	} else {
		metrics.QueueDepth.Set(float64(n))
	}

	// Built from the view already read above rather than reading it again:
	// matchRules used to call dyn.Get() a second time, so one pass could form
	// matches under two different configurations if a push landed between them.
	rules := rulesFrom(d)
	for i := 0; i < maxFormsPerPass; i++ {
		// The pass shares one deadline, so a queue deep enough to hit the cap
		// must not run past it and leave the last forms to fail one by one.
		if ctx.Err() != nil {
			return
		}
		m, err := a.queue.TryForm(ctx, rules)
		if err != nil {
			log.Printf("matchmaker: %v", err)
			return
		}
		if m == nil {
			return
		}
		if err := a.createMatch(ctx, m, d); err != nil {
			// Stop rather than carry on. createMatch puts the players back in
			// the queue when it cannot place them, so the next TryForm would
			// hand back the same people and the pass would spin against a fleet
			// that has already said it is full.
			log.Printf("create match: %v", err)
			return
		}
	}
}

// publishEvent hands a lifecycle event to the bus and says so when that fails.
//
// The four call sites used to discard the error, and two of them then counted
// the event as published — so a broker that was refusing writes produced a
// rising success graph and not one line of log. Delivery is now accounted for
// by the bus itself, where the asynchronous result actually arrives; this only
// reports the failures that happen before the message is queued at all.
func (a *App) publishEvent(ctx context.Context, e *pb.Event) {
	if err := a.bus.Publish(ctx, e); err != nil {
		log.Printf("events: %s for room %s: %v", e.GetType(), e.GetRoomId(), err)
	}
}

// matchRules turns the live config into the matchmaker's terms.
func (a *App) matchRules() matchmaking.Rules { return rulesFrom(a.dyn.Get()) }

// rulesFrom is matchRules for a caller that has already read the view, so one
// matchmaking pass is decided by one configuration throughout.
func rulesFrom(d config.View) matchmaking.Rules {
	return matchmaking.Rules{
		RoomSize:    d.RoomSize,
		MinPlayers:  d.MinPlayers,
		Timeout:     d.QueueTimeout,
		SkillWindow: d.SkillWindow,
		Widen:       d.SkillWiden,
		MaxWindow:   d.SkillMaxWindow,
	}
}

// ratingOf reads a player's rating, falling back to the default when the store
// cannot answer. A rating lookup must never be the reason somebody cannot
// queue: the cost of guessing is one match at the wrong skill, and the cost of
// failing is a player who does not get to play.
func (a *App) ratingOf(ctx context.Context, playerID string) int {
	if a.rating == nil {
		return rating.Default
	}
	v, err := a.rating.Get(ctx, playerID)
	if err != nil {
		log.Printf("rating: %s: %v", playerID, err)
		return rating.Default
	}
	return v
}

// ratingsOf is ratingOf for every seat in a match, in one round trip.
//
// The loop it replaces was a lookup per player, run in sequence — and run on
// the room's own goroutine, because OnEnd is called from inside the tick loop.
// Until it returned, Run's deferred cleanup had not happened, so the room was
// still counted by rooms.Count(): the node went on advertising a match that was
// over, and atCapacity() went on refusing placements for it. With the platform
// tier on, each of those lookups is a database query.
//
// Same fallback as the single-player version, for the same reason: a rating
// that cannot be read costs one match recorded at the wrong skill, and failing
// costs the record of a match that was actually played.
func (a *App) ratingsOf(ctx context.Context, playerIDs []string) []int {
	out := make([]int, len(playerIDs))
	for i := range out {
		out[i] = rating.Default
	}
	if a.rating == nil || len(playerIDs) == 0 {
		return out
	}
	vals, err := a.rating.GetMany(ctx, playerIDs)
	if err != nil {
		log.Printf("rating: read %d players: %v", len(playerIDs), err)
	}
	// A store that failed still hands back a full slice of defaults; one that
	// somehow did not is not worth trusting positionally.
	if len(vals) != len(playerIDs) {
		return out
	}
	return vals
}

// recordResult writes the match back into the ladder.
//
// Ratings are read on the way into the queue and written on the way out of a
// match, and this is the second half — without it the first half reads the same
// number forever and skill-based matchmaking is a search around a constant.
//
// Bots are skipped: they have no identity to carry a rating, and counting a win
// over one would let a player farm the timeout filler.
func (a *App) recordResult(ctx context.Context, req *pb.RoomRequest, snap sim.Snapshot) {
	if a.rating == nil || len(req.Seats) == 0 {
		return
	}
	scoreOf := make(map[uint32]int, len(snap.Players))
	for _, p := range snap.Players {
		if p.Bot {
			continue
		}
		scoreOf[uint32(p.ID)] = int(p.Score)
	}

	ids := make([]string, 0, len(req.Seats))
	scores := make([]int, 0, len(req.Seats))
	for _, s := range req.Seats {
		score, ok := scoreOf[s.PlayerId]
		if !ok || s.ConnId == "" {
			continue
		}
		ids = append(ids, s.ConnId)
		scores = append(scores, score)
	}
	if len(ids) < 2 {
		// A match against bots moves nobody: there is no opponent whose rating
		// the result could be measured against — and, now that a result also
		// pays a reward, no reason to pay one. A room the matchmaker filled
		// with bots after a queue timeout would otherwise be the cheapest
		// currency in the game.
		//
		// Checked before the ratings are read, not after. The read used to come
		// first and its result was then thrown away on this path — one Redis
		// round trip, or one Postgres query with the platform tier on, for
		// every bot-filled match, run on the room's own goroutine before Run's
		// cleanup could mark the room finished.
		return
	}
	// Read after the seats are collected rather than inside the loop: one round
	// trip for the match instead of one per player. See ratingsOf.
	ratings := a.ratingsOf(ctx, ids)

	updated := rating.Update(ratings, scores)
	if a.platform != nil {
		// One write for the whole result: the ratings, the record, the reward
		// and every player's history line, in one transaction keyed on the room
		// id. a.rating is the platform's own store in this mode, so calling Put
		// as well would be a second writer for a number that already moved.
		a.recordToPlatform(req.RoomId, "arena", ids, ratings, updated, scores)
		return
	}
	out := make(map[string]int, len(ids))
	for i, id := range ids {
		out[id] = updated[i]
	}
	if err := a.rating.Put(ctx, out); err != nil {
		log.Printf("rating: write %s: %v", req.RoomId, err)
	}
}

// newRoomID names a match.
//
// Random, not a timestamp. It used to be `r-<node>-<UnixNano>`, and the clock
// is not the unique thing it looks like: UnixNano reports nanoseconds but the
// underlying clock is coarser — measured here, 200k calls in a loop produced
// 16k distinct values, with as many as 18 calls sharing one. matchPass forms
// matches in a loop, so two matches landing in one clock tick is not a thought
// experiment.
//
// A duplicate room id was always wrong: two matches share a directory entry, so
// a player is routed to the wrong one. It became silent as well once results
// were recorded, because match_results is keyed on the match id — the second
// match's result is swallowed as a replay of the first, with no error anywhere.
// That is the failure this package is arranged to make impossible, arriving
// through the id rather than through the write.
//
// The node id stays in front so a room can still be traced to the process that
// created it, which is the only thing the timestamp was really buying.
//
// internal/turn learned the same lesson: a match id derived from the pair of
// players alone was reused the next time those two met, so it carries a nonce.
func (a *App) newRoomID() string {
	return "r-" + a.cfg.NodeID + "-" + session.NewID()
}

func (a *App) createMatch(ctx context.Context, m *matchmaking.Match, d config.View) error {
	gs, err := a.registry.PickLeastLoaded(ctx)
	if err != nil {
		// The players are already out of the queue — TryForm removed them
		// before this was called — so returning here without putting them back
		// is not a retry, it is the whole match dropped. They are still
		// connected and still waiting on a MATCH_FOUND that nobody is going to
		// send, for the rest of their session.
		//
		// This used to be exactly that path, and it was reachable: the
		// reservation write inside PickLeastLoaded discarded its own error, so
		// the only way out of it was a failure of the reads before it. Now that
		// a failed reservation is reported, it is the ordinary Redis blip.
		a.requeue(ctx, m.Players)
		metrics.PlacementsRefused.Inc()
		return fmt.Errorf("pick gameserver: %w", err)
	}
	if gs == nil {
		// Every node is full or draining. The players go back in the queue
		// rather than to a server that has already said no — they keep their
		// original queued_at, so they stay at the front and this shows up as
		// queue wait rather than as a silent loss.
		a.requeue(ctx, m.Players)
		metrics.PlacementsRefused.Inc()
		return fmt.Errorf("no gameserver available")
	}
	recordSpread(m)
	roomID := a.newRoomID()
	seed := time.Now().UnixNano()
	seats := make([]*pb.Seat, 0, len(m.Players))
	for i, p := range m.Players {
		wait := time.Duration(0)
		if p.QueuedAt > 0 {
			wait = time.Since(time.UnixMilli(p.QueuedAt))
			if wait > 0 {
				metrics.QueueWait.Observe(wait.Seconds())
			}
		}
		seats = append(seats, &pb.Seat{
			ConnId: p.ConnId, PlayerId: uint32(i + 1), Name: p.Name, QueuedAt: p.QueuedAt,
		})
	}
	req := &pb.RoomRequest{
		RoomId: roomID, Seed: seed, TickRate: int32(d.TickRate),
		MatchTicks: a.cfg.MatchTicks(d), Seats: seats, Bots: int32(m.Bots),
	}
	if err := a.jobs.Enqueue(ctx, gs.Id, req); err != nil {
		a.soft("registry.release_inflight", a.registry.ReleaseInflight(ctx, gs.Id))
		a.requeue(ctx, m.Players)
		return err
	}
	// Soft, though it is the most consequential of these: without the directory
	// entry a player who reconnects cannot be told which node holds their
	// match. The job is already queued and the room will run, so refusing here
	// would strand a match that is about to start — and joinRoom already
	// handles a missing entry by answering ROOM_NOT_ON_NODE rather than
	// misrouting. The counter is how this stops being invisible.
	a.soft("registry.register_room", a.registry.RegisterRoom(ctx, &pb.RoomInfo{
		RoomId: roomID, ServerId: gs.Id, PublicAddr: gs.PublicAddr, Seed: seed,
	}))
	a.publishEvent(ctx, &pb.Event{
		Type: pb.EventType_EVENT_TYPE_MATCH_STARTED, RoomId: roomID,
		Players: uint32(len(seats)), Bots: int32(m.Bots), Gs: gs.Id,
	})
	metrics.MatchesStarted.Inc()
	log.Printf("match %s players=%d bots=%d gs=%s", roomID, len(seats), m.Bots, gs.Id)
	return nil
}

// requeue puts a formed match's players back where they came from.
//
// Every caller reaches it after the queue has already handed these players
// over, so the choice is between this and losing them: they are connected,
// waiting, and no longer in any queue that the matchmaker reads.
//
// QueuedAt is carried through unchanged, which is what keeps a refusal from
// costing them their place — Enqueue stamps the current time only when the
// field is zero, so re-queueing a player whose placement failed would otherwise
// send them to the back for something the server did.
func (a *App) requeue(ctx context.Context, players []*pb.QueuePlayer) {
	for _, p := range players {
		a.soft("queue.requeue", a.queue.Enqueue(ctx, p))
	}
}

// recordSpread reports the rating gap inside a formed match.
func recordSpread(m *matchmaking.Match) {
	if len(m.Players) < 2 {
		return
	}
	lo, hi := m.Players[0].Skill, m.Players[0].Skill
	for _, p := range m.Players[1:] {
		if p.Skill < lo {
			lo = p.Skill
		}
		if p.Skill > hi {
			hi = p.Skill
		}
	}
	metrics.MatchSkillSpread.Observe(float64(hi - lo))
}

func (a *App) jobReaper(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			reapCtx, cancel := a.opIn(ctx)
			n, err := a.jobs.ReapStale(reapCtx, a.cfg.NodeID, 90*time.Second)
			cancel()
			if err != nil {
				log.Printf("job reaper: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("requeued %d stale room jobs", n)
			}
		}
	}
}

func (a *App) jobLoop(ctx context.Context) {
	for {
		// A draining node keeps the matches it has and takes no new ones.
		// Without this check SIGTERM starts a drain and the same process goes
		// on opening rooms throughout it, so the drain never reaches zero and
		// the players in those brand-new matches are cut off seconds later.
		if a.draining.Load() {
			return
		}
		job, err := a.jobs.Take(ctx, a.cfg.NodeID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if job == nil || job.Req == nil {
			continue
		}
		a.startRoom(job)
	}
}

// atCapacity reports whether this node should refuse another room.
//
// Placement already avoids full nodes, but its view is a heartbeat up to two
// seconds old and a burst of placements can land inside that window. This is
// the backstop that keeps the ceiling true, and it is the node itself asking —
// which is the only party that knows how its tick budget is actually doing.
func (a *App) atCapacity() bool {
	return a.cfg.MaxRooms > 0 && a.rooms.Count() >= a.cfg.MaxRooms
}

func (a *App) startRoom(job *placement.Job) {
	req := job.Req
	if a.draining.Load() || a.atCapacity() {
		// Hand the players back to the queue instead of opening a room that
		// cannot be served. They are not told anything: from their side they
		// are still queued, which is true.
		ctx, cancel := a.op()
		defer cancel()
		// One read for the whole refused match. This path runs when the node is
		// already at its ceiling, which is exactly when it should not be
		// spending a round trip per seat.
		seated := make([]string, 0, len(req.Seats))
		for _, s := range req.Seats {
			if s.ConnId != "" {
				seated = append(seated, s.ConnId)
			}
		}
		skills := a.ratingsOf(ctx, seated)
		i := 0
		for _, s := range req.Seats {
			if s.ConnId == "" {
				continue
			}
			// QueuedAt is carried through the placement precisely so it can be
			// restored here. Enqueue stamps the current time when it is zero,
			// which would send a player to the back of the queue for a refusal
			// that was entirely the server's doing.
			a.soft("queue.requeue", a.queue.Enqueue(ctx, &pb.QueuePlayer{
				ConnId: s.ConnId, PlayerId: s.ConnId, Name: s.Name,
				Skill: int32(skills[i]), QueuedAt: s.QueuedAt,
			}))
			i++
		}
		// The matchmaker registered this room in the directory before handing
		// the job over. Nothing is going to run it, so the entry has to go too
		// — left behind it points players at a node that never opened the room,
		// and it would sit there for the key's full 30 minutes.
		a.soft("registry.unregister_room", a.registry.UnregisterRoom(ctx, req.RoomId))
		a.soft("registry.release_inflight", a.registry.ReleaseInflight(ctx, a.cfg.NodeID))
		a.soft("jobs.ack", a.jobs.Ack(ctx, a.cfg.NodeID, job))
		// Correct the fleet's view of this node at once rather than waiting for
		// the next scheduled heartbeat. Placement chose this node on a record
		// up to a heartbeat old; leaving that record standing means it keeps
		// choosing it for the rest of the window, and every one of those costs
		// a directory write, a job round trip and a requeue. Measured over a
		// 12-second run at a ceiling of two rooms: 30 refusals become 2.
		a.soft("registry.heartbeat", a.registry.Heartbeat(ctx, a.gsRecord()))
		metrics.PlacementsRefused.Inc()
		log.Printf("refused room %s: rooms=%d max=%d draining=%v",
			req.RoomId, a.rooms.Count(), a.cfg.MaxRooms, a.draining.Load())
		return
	}
	defer func() {
		ctx, cancel := a.op()
		defer cancel()
		a.soft("registry.release_inflight", a.registry.ReleaseInflight(ctx, a.cfg.NodeID))
		// An ack that does not land is the one failure here with a visible
		// consequence: the reaper requeues the job after ninety seconds and the
		// match is delivered a second time. startRoom is written to survive
		// that — it finds the room already running and only republishes the
		// assignments — but it is worth counting rather than discovering from
		// the reaper's log.
		a.soft("jobs.ack", a.jobs.Ack(ctx, a.cfg.NodeID, job))
	}()
	ids := make([]uint32, 0, len(req.Seats))
	seats := make(map[string]uint32, len(req.Seats))
	for _, s := range req.Seats {
		ids = append(ids, s.PlayerId)
		if s.ConnId != "" {
			seats[s.ConnId] = s.PlayerId
		}
	}
	if a.rooms.Get(req.RoomId) != nil {
		// The same job was delivered twice — the reaper requeues anything taken
		// but not acked in ninety seconds, and a slow ack is enough. The room
		// is already running, so it is not started again, and no second
		// recording is opened: rooms.Start would hand back the existing room
		// and quietly drop the recorder, which then holds a sample slot for the
		// life of the process and stops recording after MAX_REPLAYS of them.
		//
		// The assignments do go out again. The first delivery may have died
		// before publishing them, and a player told twice where their match is
		// simply joins the room they are already in.
		a.publishAssignments(req)
		return
	}

	roster := room.RosterFromIDs(ids, int(req.Bots))
	// Begin returns nil once the sample is full, and the room takes a nil
	// recorder without noticing — matches are recorded by sample rather than
	// wholesale, because keeping every one costs more memory than the
	// simulations do.
	var rec room.Recorder
	if r := a.replays.Begin(replay.Header{
		RoomID: req.RoomId, Seed: req.Seed, TickRate: int(req.TickRate),
		MatchTicks: req.MatchTicks, Roster: roster,
	}); r != nil {
		rec = r
	}
	a.rooms.Start(context.Background(), room.Params{
		ID: req.RoomId, Seed: req.Seed, TickRate: int(req.TickRate), MatchTicks: req.MatchTicks, Roster: roster, Seats: seats,
		Recorder: rec,
		// Node-local netcode settings rather than something the matchmaker
		// decides: both are about the link between this server and its players.
		InputBuffer:   a.cfg.InputBuffer,
		SnapshotEvery: a.cfg.SnapshotEvery(int(req.TickRate)),
		Warmup:        a.cfg.WarmupTimeout,
		ExpectSeats:   seatedCount(req),
		OnEnd: func(snap sim.Snapshot) {
			// A fresh context, not one captured when the room started: this
			// runs minutes later and the room's own deadline is long gone.
			ctx, cancel := a.op()
			defer cancel()
			metrics.MatchesEnded.Inc()
			// Before anything that talks to a store: it costs nothing, and
			// until it runs every player in this match is still, as far as the
			// gateway is concerned, in it.
			a.releaseSeats(req)
			a.soft("registry.unregister_room", a.registry.UnregisterRoom(ctx, req.RoomId))
			a.recordResult(ctx, req, snap)
			a.publishEvent(ctx, &pb.Event{
				Type: pb.EventType_EVENT_TYPE_MATCH_ENDED, RoomId: req.RoomId,
				Winner: uint32(snap.Winner), Tick: snap.Tick,
			})
		},
	})
	a.publishAssignments(req)
}

// seatedCount is how many of a match's seats belong to a real connection.
//
// Bots are seated too, and they are never going to join: counting them would
// mean every bot-filled match waits out the whole warmup for players who do not
// exist.
func seatedCount(req *pb.RoomRequest) int {
	n := 0
	for _, s := range req.Seats {
		if s.ConnId != "" {
			n++
		}
	}
	return n
}

// publishAssignments tells each seated player where their match is.
//
// Host is empty in single-process mode: the gateway holding the connection is
// also the node running the room, and handing the client an address to
// reconnect to would send it round a loop back to here.
func (a *App) publishAssignments(req *pb.RoomRequest) {
	host := a.cfg.PublicAddr
	if a.cfg.Role == pb.Role_ROLE_ALL {
		host = ""
	}
	ctx, cancel := a.op()
	defer cancel()
	for _, s := range req.Seats {
		// Soft, and the player pays for it: an assignment that is not published
		// is a match this player is seated in and never told about. There is no
		// better answer from here — the room is already running and the seat is
		// already theirs — but a silent drop is how "some players never leave
		// the queue screen" becomes unexplainable.
		a.soft("notify.publish_assignment", a.notify.Publish(ctx, &pb.Assignment{
			ConnId: s.ConnId, PlayerId: s.PlayerId, Name: s.Name,
			RoomId: req.RoomId, Host: host, Seed: req.Seed, TickRate: req.TickRate,
		}))
	}
}

func (a *App) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			gs := a.gsRecord()
			hbCtx, cancel := a.opIn(ctx)
			a.soft("registry.heartbeat", a.registry.Heartbeat(hbCtx, gs))
			cancel()
			metrics.GSHeartbeat.WithLabelValues(a.cfg.NodeID).Set(float64(gs.Rooms))
		}
	}
}

// gsRecord is what this node tells the fleet about itself.
func (a *App) gsRecord() *pb.GameServer {
	return &pb.GameServer{
		Id: a.cfg.NodeID, PublicAddr: a.cfg.PublicAddr,
		Rooms: int32(a.rooms.Count()), Ccu: int32(a.hub.Count()),
		Capacity: int32(a.cfg.MaxRooms), Draining: a.draining.Load(),
	}
}

func (a *App) handleStats(w http.ResponseWriter, r *http.Request) {
	// Fleet shape — node id, live CCU, room count, whether this node is
	// draining — is operational detail. It is useful enough to keep open in
	// development and not something to serve to the internet in production.
	if a.cfg.Production() && !a.adminAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	d := a.dyn.Get()
	w.Header().Set("Content-Type", "application/json")
	writeJSONErr("stats", json.NewEncoder(w).Encode(map[string]any{
		"node": a.cfg.NodeID, "role": protocol.RoleString(a.cfg.Role),
		"tick_rate": d.TickRate, "room_size": d.RoomSize, "min_players": d.MinPlayers,
		"ccu": a.hub.Count(), "rooms": a.rooms.Count(), "ready": a.ready.Load(),
		"max_rooms": a.cfg.MaxRooms, "draining": a.draining.Load(),
		"protocol_version": protocol.Version,
		"skill_window":     d.SkillWindow, "skill_widen": d.SkillWiden,
	}))
}

type dynDTO struct {
	TickRate          int   `json:"tick_rate"`
	RoomSize          int   `json:"room_size"`
	MinPlayers        int   `json:"min_players"`
	QueueTimeoutMs    int64 `json:"queue_timeout_ms"`
	MatchSeconds      int   `json:"match_seconds"`
	DisconnectGraceMs int64 `json:"disconnect_grace_ms"`
	SendBuffer        int   `json:"send_buffer"`
	MaxCCU            int   `json:"max_ccu"`
	SkillWindow       int   `json:"skill_window"`
	SkillWiden        int   `json:"skill_widen"`
	SkillMaxWindow    int   `json:"skill_max_window"`
}

func viewDTO(v config.View) dynDTO {
	return dynDTO{
		TickRate: v.TickRate, RoomSize: v.RoomSize, MinPlayers: v.MinPlayers,
		QueueTimeoutMs: v.QueueTimeout.Milliseconds(), MatchSeconds: v.MatchSeconds,
		DisconnectGraceMs: v.DisconnectGrace.Milliseconds(), SendBuffer: v.SendBuffer, MaxCCU: v.MaxCCU,
		SkillWindow: v.SkillWindow, SkillWiden: v.SkillWiden, SkillMaxWindow: v.SkillMaxWindow,
	}
}

func (a *App) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if a.cfg.Production() && !a.adminAuthorized(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		writeJSONErr("config.get", json.NewEncoder(w).Encode(viewDTO(a.dyn.Get())))
	case http.MethodPut:
		if !a.adminAuthorized(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cur := viewDTO(a.dyn.Get())
		if err := json.Unmarshal(body, &cur); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		v := config.View{
			TickRate: cur.TickRate, RoomSize: cur.RoomSize, MinPlayers: cur.MinPlayers,
			QueueTimeout:    time.Duration(cur.QueueTimeoutMs) * time.Millisecond,
			MatchSeconds:    cur.MatchSeconds,
			DisconnectGrace: time.Duration(cur.DisconnectGraceMs) * time.Millisecond,
			SendBuffer:      cur.SendBuffer, MaxCCU: cur.MaxCCU,
			SkillWindow: cur.SkillWindow, SkillWiden: cur.SkillWiden, SkillMaxWindow: cur.SkillMaxWindow,
		}
		if err := a.dyn.Update(r.Context(), v); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		writeJSONErr("config.put", json.NewEncoder(w).Encode(viewDTO(a.dyn.Get())))
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}
