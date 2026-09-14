package session

import (
	"crypto/rand"
	"encoding/hex"
)

// NewID mints a session id.
//
// The id is a capability, not a label: Hub.Add files a connection under it and
// closes whatever was there before, so anyone able to predict or collide with
// one can evict the player holding it. That is why the WebSocket transport
// refuses to take an id from the request and mints it here instead.
//
// So the error is checked even though it cannot fire. As of Go 1.24
// crypto/rand.Read never returns a non-nil error — it terminates the process
// rather than hand back short or unseeded bytes — but the signature still has
// one, and a caller who swapped rand.Reader for a failing source would
// otherwise get a silently all-zero id: every session sharing one key, each new
// connection evicting the last. Failing loudly is the same answer
// udp.newCookies gives for the same question.
func NewID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("session: cannot seed id: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
