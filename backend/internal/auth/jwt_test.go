package auth

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestIssueVerify(t *testing.T) {
	j, err := NewJWT("test-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tok, pid, _, err := j.Issue("alice")
	if err != nil || pid == "" || tok == "" {
		t.Fatalf("issue %q pid=%q err=%v", tok, pid, err)
	}
	claims, err := j.Verify(tok)
	if err != nil || claims.PlayerID != pid || claims.Name != "alice" {
		t.Fatalf("verify %+v err=%v", claims, err)
	}
}

func TestVerifyBad(t *testing.T) {
	j, err := NewJWT("test-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Verify("bad"); err == nil {
		t.Fatal("expected error")
	}
}

// IssueFor is the platform tier's half: the id comes from an account that
// already exists rather than being invented here.
//
// The split is the point. Issue mints the id itself, and an identity a process
// invents is what this repo spent effort getting rid of — two gateways counting
// independently hand the same id to two different people. When there is an
// account behind the login, the id has to come from the account, and this is
// the only thing that puts it in the token.
func TestIssueForCarriesTheCallersIdentity(t *testing.T) {
	j, err := NewJWT("test-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	tok, id, exp, err := j.IssueFor("a-durable-account", "Pilot One")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if id != "a-durable-account" {
		t.Fatalf("id = %q — IssueFor must not mint one", id)
	}
	if exp.Before(time.Now()) {
		t.Fatalf("expiry %v is in the past", exp)
	}
	claims, err := j.Verify(tok)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.PlayerID != "a-durable-account" || claims.Name != "Pilot One" {
		t.Fatalf("claims = %+v", claims)
	}
	// Subject too: it is the registered claim anything generic reads.
	if claims.Subject != "a-durable-account" {
		t.Fatalf("subject = %q", claims.Subject)
	}

	// Issuing the same id twice is the ordinary case — a player logs in again —
	// and both tokens have to authenticate them.
	tok2, _, _, err := j.IssueFor("a-durable-account", "Pilot One")
	if err != nil {
		t.Fatal(err)
	}
	if c2, err := j.Verify(tok2); err != nil || c2.PlayerID != claims.PlayerID {
		t.Fatalf("second token: %+v (%v)", c2, err)
	}
}

// A token with no subject authenticates nobody, and handing one out is worse
// than refusing: every read the platform serves is authorised by this field, so
// an empty one is a session that belongs to whatever row happens to match "".
func TestIssueForRefusesAnEmptyIdentity(t *testing.T) {
	j, err := NewJWT("test-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := j.IssueFor("", "Nobody"); err == nil {
		t.Fatal("issued a token for no account")
	}
	// A missing display name is not the same thing — it is cosmetic, and a
	// default is better than a refusal.
	tok, _, _, err := j.IssueFor("a-1", "")
	if err != nil {
		t.Fatalf("empty name: %v", err)
	}
	claims, err := j.Verify(tok)
	if err != nil || claims.Name == "" {
		t.Fatalf("claims = %+v (%v)", claims, err)
	}
}

// Verify must reject a token that merely looks right. The algorithm check is
// the one that matters: "alg": "none" is the oldest JWT attack there is, and a
// library that honours it accepts a token anybody can write.
func TestVerifyRejectsForgeries(t *testing.T) {
	j, err := NewJWT("test-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	good, _, _, err := j.IssueFor("a-1", "Pilot")
	if err != nil {
		t.Fatal(err)
	}

	other, err := NewJWT("a-different-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Verify(good); err == nil {
		t.Fatal("a token signed with one secret verified under another")
	}

	// alg=none, with the payload of a real token and no signature at all.
	parts := strings.Split(good, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts", len(parts))
	}
	none := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	if _, err := j.Verify(none + "." + parts[1] + "."); err == nil {
		t.Fatal("an unsigned alg=none token verified")
	}

	// A tampered payload under the original signature.
	if _, err := j.Verify(parts[0] + "." + parts[1] + "x." + parts[2]); err == nil {
		t.Fatal("a tampered payload verified")
	}

	// And an expired one.
	brief, err := NewJWT("test-secret", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	stale, _, _, err := brief.IssueFor("a-1", "Pilot")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, err := brief.Verify(stale); err == nil {
		t.Fatal("an expired token verified")
	}
}
