package platform

import "github.com/nguyenbatam/arena_game_server/internal/rating"

// StartingRating is where a new account's ladder position begins. It is
// rating.Default rather than a second copy of 1000: the matchmaker widens its
// search around this number, and two packages disagreeing about it would put
// every new player slightly outside the bracket they were meant to land in.
const StartingRating = rating.Default

// StartingCurrency is the sign-up grant. A store with nothing affordable in it
// is a store nobody ever sees, and the interesting paths here — spend, refuse
// to overdraw, replay a purchase — need a balance to exercise.
const StartingCurrency int64 = 250

// Item is one thing the store sells.
//
// Cosmetic only, and that is a design statement rather than a shortage of
// imagination: anything that changes what a player can do in a match has to be
// read by the simulation, and the simulation is the one place in this repo that
// is not allowed to ask a database a question. An item that alters damage would
// put a Postgres round trip inside a 50 ms tick budget. Loadout-style games
// solve that by resolving the loadout once at match creation and handing the
// room a plain struct; nothing here needs it yet, so nothing here has it.
type Item struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Price int64  `json:"price"`
	// Max is how many one account may hold. 1 for a cosmetic somebody owns or
	// does not; 0 for a consumable with no ceiling.
	Max int `json:"max"`
}

// Catalog is the store's price list, keyed by item id.
//
// In code rather than in a table, deliberately. A catalog is configuration that
// ships with a build: it is read on every purchase and changed by a deploy, and
// putting it in the database buys a join on the hot path and a migration every
// time a price moves. What must be in the database is what a player owns —
// inventory rows reference an item id and survive the item leaving the catalog,
// which is why Inventory never joins against this.
type Catalog map[string]Item

func DefaultCatalog() Catalog {
	items := []Item{
		{ID: "skin.crimson", Name: "Crimson Hull", Kind: "skin", Price: 150, Max: 1},
		{ID: "skin.void", Name: "Void Hull", Kind: "skin", Price: 400, Max: 1},
		{ID: "trail.ion", Name: "Ion Trail", Kind: "trail", Price: 220, Max: 1},
		{ID: "emote.gg", Name: "GG", Kind: "emote", Price: 60, Max: 1},
		{ID: "boost.xp", Name: "XP Booster", Kind: "consumable", Price: 80},
	}
	c := make(Catalog, len(items))
	for _, it := range items {
		c[it.ID] = it
	}
	return c
}

// List returns the catalog in a stable order for an API response.
func (c Catalog) List() []Item {
	out := make([]Item, 0, len(c))
	for _, it := range c {
		out = append(out, it)
	}
	sortItems(out)
	return out
}

func sortItems(v []Item) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j].ID < v[j-1].ID; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

// Rewards is what a finished match pays into a wallet.
//
// Flat participation plus a win bonus plus a little per point, which is the
// shape most games land on for a reason: participation alone pays somebody for
// idling, and a win-only reward makes a losing streak worth nothing at all and
// is how players stop queueing.
type Rewards struct {
	Participation int64
	Win           int64
	PerScore      int64
	// Cap bounds one match's payout. A score is whatever the game counted, and
	// a game that counts wrong — a bug, an exploit, a bot farm — should cost a
	// bounded amount of currency rather than an unbounded one. 0 disables it.
	Cap int64
}

var DefaultRewards = Rewards{Participation: 10, Win: 25, PerScore: 2, Cap: 200}

// Payout is what one result earns.
func (r Rewards) Payout(res Result) int64 {
	v := r.Participation
	if res.Placement == 1 {
		v += r.Win
	}
	if res.Score > 0 {
		v += r.PerScore * int64(res.Score)
	}
	if r.Cap > 0 && v > r.Cap {
		v = r.Cap
	}
	return v
}
