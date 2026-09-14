// Client-side rules of the platform tier.
//
// Three small things the browser has to get right, and each of them fails
// quietly when it does not — which is why they live in their own file and are
// pinned from Go in internal/platform/webclient_port_test.go rather than left
// to whoever edits the page next. Same arrangement as web/cursor.js.

// KeyRing holds one idempotency key per item across retries.
//
// This is the client half of a rule the server cannot enforce on its own: the
// server cannot tell a retry from a second purchase without being told, and
// what tells it is the key. So the key has to be minted **once per attempt**
// and kept until that attempt is answered — a key minted per request turns a
// double-click on a slow link into two purchases, which is the exact bug the
// ledger is keyed to prevent.
//
// The subtle half is what happens on failure: the key is **kept**, not
// discarded. A refused purchase — an empty wallet, a timeout — rolls back on
// the server with the key included, so the same request has to be free to
// succeed later. Minting a fresh key there would be correct only by accident,
// and would double-charge on the one case that matters: a response lost in
// flight, where the purchase actually landed.
class KeyRing {
  // mint is injected so a test can be deterministic; the page passes
  // crypto.randomUUID.
  constructor(mint) {
    this.mint = mint || (() => crypto.randomUUID());
    this.keys = new Map();
  }

  // begin returns the key for this attempt, minting one only if no attempt is
  // outstanding. Calling it twice without settling returns the same key.
  begin(itemID) {
    if (!this.keys.has(itemID)) this.keys.set(itemID, this.mint());
    return this.keys.get(itemID);
  }

  // settle forgets the key: the attempt was answered, and the next purchase of
  // this item is a new purchase rather than a retry of this one.
  //
  // A server replay counts as answered. It means the original attempt landed,
  // so there is nothing left to retry.
  settle(itemID) { this.keys.delete(itemID); }

  // keep is the failure path, written out rather than left implicit. It does
  // nothing on purpose, and the name is the documentation: the next begin()
  // must hand back the same key.
  keep(_itemID) {}

  // pending reports whether an attempt is outstanding, for a test or a spinner.
  pending(itemID) { return this.keys.has(itemID); }
}

// sessionFrom parses a stored session, returning null for anything unusable.
//
// The unit is the trap. `expires_at` arrives from the server in **seconds**
// since the epoch — it is `time.Time.Unix()` — and Date.now() is milliseconds.
// Compare them directly and the answer is wrong by a factor of a thousand in
// whichever direction hurts: every session looks expired, or none ever does and
// the page keeps presenting a token the server will refuse.
//
// Returning null rather than an expired session matters because of what the
// caller does with null: the game pages fall back to an anonymous login. A
// token the server will reject is worse than no token at all.
function sessionFrom(raw, nowMs) {
  let s;
  try {
    s = JSON.parse(raw || 'null');
  } catch {
    return null;
  }
  if (!s || typeof s !== 'object' || !s.token) return null;
  // No expiry claim is not an expired one: the server is the authority, and it
  // answers 401 if this is stale. Guessing here would sign people out early.
  if (s.expires_at && s.expires_at * 1000 <= nowMs) return null;
  return s;
}

// ratingDelta renders a history row's rating movement.
//
// Zero is not "no change this match", it is "this mode has no ladder": the
// turn-based game records its matches and pays for them and moves no rating,
// because one number cannot rank two different games. Showing "+0" would claim
// a player drew against the ladder; a dash says there was no ladder in it.
function ratingDelta(row) {
  const d = (row.rating_after || 0) - (row.rating_before || 0);
  if (d === 0) return '—';
  return d > 0 ? '+' + d : String(d);
}

// What one open tab costs the server while it sits there.
//
// The page polls so that a match finished in another tab shows up here, and the
// budget is not generous: PLATFORM_RATE_LIMIT defaults to 60 requests a minute
// per IP, and the IP is the household, not the tab. The first version of this
// page redrew everything every ten seconds — five requests — which is thirty a
// minute, so a second tab put the page over its own limit and it began 429ing
// itself. That is a bad failure to debug from the outside: the server is doing
// exactly what it was told, and the client is the one misbehaving.
//
// So the cadence is a declared number rather than a literal buried in a
// setInterval, and internal/platform/webclient_port_test.go checks it against
// the server's default from the Go side. If you add a request to the page's
// poll(), raise POLL_REQUESTS to match — the arithmetic below is only as
// truthful as that number.
const POLL_INTERVAL_MS = 15000;
const POLL_REQUESTS = 3;

// pollRequestsPerMinute is what `tabs` open copies of the page spend between
// them, doing nothing.
function pollRequestsPerMinute(tabs) {
  return tabs * POLL_REQUESTS * (60000 / POLL_INTERVAL_MS);
}

const exported = { KeyRing, sessionFrom, ratingDelta, pollRequestsPerMinute, POLL_INTERVAL_MS, POLL_REQUESTS };
if (typeof window !== 'undefined') Object.assign(window, exported);
if (typeof module !== 'undefined') module.exports = exported;
