-- The platform schema.
--
-- Applied on startup with IF NOT EXISTS rather than by a migration tool. That
-- is honest for a demo and wrong for a product: the first column that has to
-- change type, or the first index that has to be built CONCURRENTLY against a
-- live table, is the day this needs golang-migrate or Atlas and a versioned
-- history. The shape below is written so that day is a small one — every table
-- is keyed on something stable, and nothing derived is stored twice.

CREATE TABLE IF NOT EXISTS accounts (
    id            TEXT PRIMARY KEY,
    -- username is kept as typed for display; username_key is what UNIQUE is on,
    -- so "Tam" and "tam" cannot both be registered. Two columns rather than a
    -- lower(username) expression index because the login lookup is then a plain
    -- equality on an indexed column, and because CITEXT is an extension a
    -- managed Postgres may not have enabled.
    username      TEXT NOT NULL,
    username_key  TEXT NOT NULL UNIQUE,
    display_name  TEXT NOT NULL,
    pw_hash       BYTEA NOT NULL,
    pw_salt       BYTEA NOT NULL,
    pw_iters      INTEGER NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    last_login_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS profiles (
    account_id TEXT PRIMARY KEY REFERENCES accounts (id) ON DELETE CASCADE,
    rating     INTEGER NOT NULL,
    matches    INTEGER NOT NULL DEFAULT 0,
    wins       INTEGER NOT NULL DEFAULT 0,
    kills      INTEGER NOT NULL DEFAULT 0,
    -- The balance cannot go negative. Every spend is a guarded UPDATE that
    -- declines rather than trips this, so reaching it means a code path got in
    -- that was not supposed to — which is exactly what a constraint is for.
    currency   BIGINT  NOT NULL DEFAULT 0 CHECK (currency >= 0)
);

-- The leaderboard's whole query. Without it, ranking means a sequential scan of
-- the player base plus a sort, on an endpoint players refresh.
CREATE INDEX IF NOT EXISTS profiles_rank_idx
    ON profiles (rating DESC, account_id ASC) WHERE matches > 0;

CREATE TABLE IF NOT EXISTS inventory (
    account_id  TEXT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    item_id     TEXT NOT NULL,
    qty         INTEGER NOT NULL CHECK (qty >= 0),
    acquired_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, item_id)
);

-- Every currency or item movement, once.
--
-- The primary key is (account_id, idem_key) and not idem_key alone. Clients
-- choose their own keys and choose them independently, so a bare key means one
-- player's "buy-1" collides with another's and the second player's purchase is
-- swallowed as a duplicate — no error, no item, no charge, nothing to look at.
CREATE TABLE IF NOT EXISTS ledger (
    account_id TEXT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    idem_key   TEXT NOT NULL,
    kind       TEXT NOT NULL,
    item_id    TEXT NOT NULL DEFAULT '',
    qty        INTEGER NOT NULL DEFAULT 0,
    delta      BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, idem_key)
);

-- One row per player per match: the history a player reads, and the
-- idempotency key the recording writer relies on, in one table. Splitting them
-- would mean two writes that have to agree.
CREATE TABLE IF NOT EXISTS match_results (
    match_id      TEXT NOT NULL,
    account_id    TEXT NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    mode          TEXT NOT NULL,
    score         INTEGER NOT NULL,
    placement     INTEGER NOT NULL,
    rating_before INTEGER NOT NULL,
    rating_after  INTEGER NOT NULL,
    reward        BIGINT NOT NULL,
    ended_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (match_id, account_id)
);

CREATE INDEX IF NOT EXISTS match_results_account_idx
    ON match_results (account_id, ended_at DESC, match_id DESC);
