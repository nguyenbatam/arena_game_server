// Cursor bookkeeping for the turn-based event log.
//
// The rule the whole sync family rests on: events are numbered consecutively,
// so the run a client is owed is exactly seq+1 .. current_seq. Anything else is
// a hole, and a hole must be repaired before the cursor moves past it.
//
// This matters more than it looks. A push travels over pub/sub between nodes
// and is fire-and-forget — the node deciding a move is often not the node
// holding this player, and nothing retries. Rendering an incomplete run and
// then advancing the cursor would lose those events permanently, which is the
// one thing this family does not allow (the arena is the opposite: there, the
// next tick patches whatever was missed).
//
// Kept in its own file so it can be exercised directly — see
// internal/turn/cursor_port_test.go.

// hasGap reports whether an update leaves a hole between the cursor and what it
// carries. Pass the decoded TURN_UPDATE and the cursor held right now.
function hasGap(update, seq) {
  const current = update.current_seq || 0;
  const events = update.events || [];
  if (current <= seq) return false;             // nothing new is owed
  if (!events.length) return true;              // owed events, given none
  if (events[0].seq !== seq + 1) return true;   // the run starts too late
  for (let i = 1; i < events.length; i++) {
    if (events[i].seq !== events[i - 1].seq + 1) return true;
  }
  // The run has to reach the cursor the server says it just moved us to.
  return events[events.length - 1].seq !== current;
}

if (typeof window !== 'undefined') window.hasGap = hasGap;
if (typeof module !== 'undefined') module.exports = { hasGap };
