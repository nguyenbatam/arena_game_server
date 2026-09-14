package platform

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nguyenbatam/arena_game_server/internal/config"
)

// web/platform.js holds the client half of rules this package enforces on the
// server. Pinned from Go for the same reason web/cursor.js is: each of them
// fails quietly in the browser, and none of the Go tests would notice.
//
// Same harness as internal/turn/cursor_port_test.go and
// internal/sim/predict_port_test.go — run the module under node, compare against
// cases written here.
func runJS(t *testing.T, body string, arg any) []byte {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed; skipping JS parity check")
	}
	script, err := filepath.Abs("../../../web/platform.js")
	if err != nil {
		t.Fatal(err)
	}
	blob, err := json.Marshal(arg)
	if err != nil {
		t.Fatal(err)
	}
	js := fmt.Sprintf(`
const { KeyRing, sessionFrom, ratingDelta, pollRequestsPerMinute } = require(%q);
const input = JSON.parse(process.argv[1]);
%s
`, script, body)
	out, err := exec.Command(node, "-e", js, string(blob)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	return []byte(strings.TrimSpace(string(out)))
}

// The client half of the ledger's idempotency key.
//
// The server cannot tell a retry from a second purchase without being told, and
// what tells it is whether the key is the same. So every rule below is one the
// server is relying on and cannot check:
//
//   - the same key while an attempt is unanswered — otherwise a double-click on
//     a slow link is two purchases, which is exactly what
//     TestPurchaseIsChargedOnceHoweverOftenItIsRetried assumes cannot happen;
//   - a new key after an answer — otherwise a player who buys a consumable
//     twice on purpose is told "replay" and charged once;
//   - the same key after a failure — because the server rolled the key back
//     with the rest of the transaction, so the retry has to be able to succeed,
//     and minting a fresh one would double-charge on the case that matters: a
//     response lost in flight, where the purchase actually landed.
func TestWebKeyRingHoldsAKeyAcrossRetries(t *testing.T) {
	// A deterministic mint, so the assertions are about identity rather than
	// about randomness.
	body := `
const ring = new KeyRing((() => { let n = 0; return () => 'k' + (++n); })());
const out = [];
for (const step of input) {
  switch (step.op) {
    case 'begin':  out.push(ring.begin(step.item)); break;
    case 'settle': ring.settle(step.item); out.push(''); break;
    case 'keep':   ring.keep(step.item); out.push(''); break;
    case 'pending': out.push(String(ring.pending(step.item))); break;
  }
}
console.log(JSON.stringify(out));
`
	type step struct {
		Op   string `json:"op"`
		Item string `json:"item"`
	}
	steps := []step{
		// Two clicks on one slow request: one key, so one purchase.
		{"begin", "emote.gg"},
		{"begin", "emote.gg"},
		// A different item is a different attempt, never the same key.
		{"begin", "boost.xp"},
		// The first purchase is answered; the next one is a new purchase.
		{"settle", "emote.gg"},
		{"begin", "emote.gg"},
		// It fails. The key is kept, so the retry can succeed on the server's
		// terms — and cannot be charged twice if the failure was only the
		// response going missing.
		{"keep", "emote.gg"},
		{"begin", "emote.gg"},
		{"pending", "emote.gg"},
		// The other item's attempt was never settled and still holds its key.
		{"begin", "boost.xp"},
		// Settling something that is not outstanding is a no-op, not a throw.
		{"settle", "never.bought"},
		{"pending", "never.bought"},
	}
	want := []string{
		"k1", "k1",
		"k2",
		"",
		"k3",
		"",
		"k3",
		"true",
		"k2",
		"",
		"false",
	}

	var got []string
	if err := json.Unmarshal(runJS(t, body, steps), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d results for %d steps", len(got), len(steps))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step %d (%s %s) = %q, want %q\nfull: %v",
				i, steps[i].Op, steps[i].Item, got[i], want[i], got)
		}
	}
}

// expires_at crosses the wire as seconds — it is time.Time.Unix() — and
// Date.now() is milliseconds. Compare them directly and the answer is wrong by
// a factor of a thousand in whichever direction hurts: every session looks
// expired, or none ever does and the page keeps presenting a token the server
// will refuse.
//
// The Go side of that unit is asserted here too, so the two cannot drift: the
// cases are built from a time.Time the way the handler builds the field.
func TestWebSessionExpiryUsesTheUnitTheServerSends(t *testing.T) {
	body := `
const out = input.map(c => sessionFrom(c.raw, c.now) === null ? 'null' : 'ok');
console.log(JSON.stringify(out));
`
	now := time.Unix(1_700_000_000, 0)
	nowMS := now.UnixMilli()
	// Exactly what issueSession writes, in the unit it writes it in.
	live := fmt.Sprintf(`{"token":"t","player_id":"a-1","expires_at":%d}`, now.Add(time.Hour).Unix())
	stale := fmt.Sprintf(`{"token":"t","player_id":"a-1","expires_at":%d}`, now.Add(-time.Second).Unix())
	exact := fmt.Sprintf(`{"token":"t","player_id":"a-1","expires_at":%d}`, now.Unix())

	type kase struct {
		name string
		Raw  string `json:"raw"`
		Now  int64  `json:"now"`
		want string
	}
	cases := []kase{
		{"live token", live, nowMS, "ok"},
		{"expired a second ago", stale, nowMS, "null"},
		{"expiring exactly now", exact, nowMS, "null"},
		// The bug this test exists for: a seconds value compared against
		// milliseconds is ~1.7e9 vs ~1.7e12, so a live token reads as long
		// expired. If sessionFrom ever drops the *1000, this case flips.
		{"seconds not mistaken for millis", live, now.Add(-time.Hour).UnixMilli(), "ok"},
		// No expiry claim is not an expired one. The server is the authority
		// and answers 401; guessing here would sign people out early.
		{"no expiry claim", `{"token":"t","player_id":"a-1"}`, nowMS, "ok"},
		{"no token", `{"player_id":"a-1","expires_at":9999999999}`, nowMS, "null"},
		{"empty string", "", nowMS, "null"},
		{"literal null", "null", nowMS, "null"},
		{"not json", "{oh no", nowMS, "null"},
		{"not an object", `"a string"`, nowMS, "null"},
	}

	var got []string
	if err := json.Unmarshal(runJS(t, body, cases), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for i, c := range cases {
		if got[i] != c.want {
			t.Errorf("%s: sessionFrom = %s, want %s", c.name, got[i], c.want)
		}
	}
}

// A turn-based match records a rating that did not move, because that mode has
// no ladder. "+0" would claim the player drew against the ladder; a dash says
// there was no ladder in it. The Go side produces exactly these rows — see
// recordTurnResult, which passes the same number as before and after.
func TestWebRatingDeltaRendersAModeWithNoLadderAsADash(t *testing.T) {
	body := `console.log(JSON.stringify(input.map(ratingDelta)));`

	rows := []MatchRow{
		{RatingBefore: 1000, RatingAfter: 1016},
		{RatingBefore: 1000, RatingAfter: 984},
		// What recordTurnResult writes.
		{RatingBefore: 1000, RatingAfter: 1000},
		// A row from before the columns existed, or a bot seat.
		{},
	}
	want := []string{"+16", "-16", "—", "—"}

	var got []string
	if err := json.Unmarshal(runJS(t, body, rows), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %+v rendered %q, want %q", rows[i], got[i], want[i])
		}
	}
}

// The page's idle cost, checked against the server's own default.
//
// This is the only invariant here that spans both tiers, and it is the one that
// already broke: the first version redrew everything every ten seconds — five
// requests, thirty a minute — against a PLATFORM_RATE_LIMIT of 60 per IP, so a
// second tab put the page over its own limit and it began 429ing itself. From
// the outside that reads as the server misbehaving, and the server is doing
// exactly what it was told.
//
// The limit is read from config rather than written here, so raising the
// default does not leave a stale number in a test, and lowering it fails here
// instead of in somebody's browser.
func TestTheWebPagePollsInsideTheServersRateLimit(t *testing.T) {
	// How many tabs one household plausibly leaves open. The limiter is keyed
	// by IP, so tabs share the budget — as do two people behind one NAT.
	const tabs = 4

	limit := config.LoadStatic().PlatformRateLimit
	if limit <= 0 {
		t.Skip("PLATFORM_RATE_LIMIT is disabled in this environment")
	}

	body := `console.log(JSON.stringify(input.map(tabs => pollRequestsPerMinute(tabs))));`
	var got []float64
	if err := json.Unmarshal(runJS(t, body, []int{1, tabs}), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	one, many := got[0], got[1]
	t.Logf("one tab costs %.0f requests/min, %d tabs cost %.0f, limit is %d", one, tabs, many, limit)

	if many > float64(limit) {
		t.Fatalf("%d idle tabs spend %.0f requests a minute against a limit of %d — "+
			"the page rate-limits itself. Lengthen POLL_INTERVAL_MS or drop a request from poll().",
			tabs, many, limit)
	}
	// And leave room for the player actually doing something: signing in,
	// buying, and the full render each of those triggers.
	if headroom := float64(limit) - many; headroom < float64(one) {
		t.Fatalf("%d tabs leave %.0f requests a minute of headroom, less than one tab's poll — "+
			"a purchase or a sign-in would be refused", tabs, headroom)
	}
}
