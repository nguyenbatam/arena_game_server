package room

import (
	"context"
	"sync"

	"github.com/nguyenbatam/arena_game_server/internal/sim"
)

type Manager struct {
	mu    sync.RWMutex
	rooms map[string]*Room
}

func NewManager() *Manager {
	return &Manager{rooms: map[string]*Room{}}
}

func RosterFromIDs(ids []uint32, bots int) []sim.Player {
	out := make([]sim.Player, 0, len(ids)+bots)
	for _, id := range ids {
		out = append(out, sim.Player{ID: sim.PlayerID(id)})
	}
	next := uint32(1000)
	for i := 0; i < bots; i++ {
		out = append(out, sim.Player{ID: sim.PlayerID(next + uint32(i)), Bot: true})
	}
	return out
}

func (m *Manager) Start(ctx context.Context, p Params) *Room {
	m.mu.Lock()
	if r, ok := m.rooms[p.ID]; ok {
		m.mu.Unlock()
		return r
	}
	r := New(p)
	m.rooms[p.ID] = r
	m.mu.Unlock()
	go func() {
		r.Run(ctx)
		m.mu.Lock()
		delete(m.rooms, p.ID)
		m.mu.Unlock()
	}()
	return r
}

func (m *Manager) Get(id string) *Room {
	m.mu.RLock()
	r := m.rooms[id]
	m.mu.RUnlock()
	return r
}

func (m *Manager) Count() int {
	m.mu.RLock()
	n := len(m.rooms)
	m.mu.RUnlock()
	return n
}

func (m *Manager) StopAll() {
	m.mu.RLock()
	rooms := make([]*Room, 0, len(m.rooms))
	for _, r := range m.rooms {
		rooms = append(rooms, r)
	}
	m.mu.RUnlock()
	for _, r := range rooms {
		r.Stop()
	}
}
