// Package tradeparams provides an in-memory store for trading parameters
// (targets, stop-losses, etc.) with JSON persistence and pub/sub for SSE push.
package tradeparams

import (
	"encoding/json"
	"log/slog"
	"os"
	"sync"
)

// Event is the wire format for SSE messages.
type Event struct {
	Type  string                        `json:"type"`            // "snapshot", "set", "delete"
	Date  string                        `json:"date,omitempty"`  // set/delete only
	Key   string                        `json:"key,omitempty"`   // set/delete only
	Value float64                       `json:"value,omitempty"` // set only
	Data  map[string]map[string]float64 `json:"data,omitempty"`  // snapshot only
}

// Store holds trading parameters in memory with JSON persistence and pub/sub.
type Store struct {
	mu       sync.RWMutex
	params   map[string]map[string]float64 // date -> key -> value
	filePath string
	log      *slog.Logger

	subsMu    sync.Mutex
	nextSubID int
	subs      map[int]chan Event
}

// NewStore creates a Store, loading persisted state from filePath.
func NewStore(filePath string, log *slog.Logger) *Store {
	s := &Store{
		params:   make(map[string]map[string]float64),
		filePath: filePath,
		log:      log,
		subs:     make(map[int]chan Event),
	}
	s.load()
	return s
}

// Snapshot returns a deep copy of all parameters.
func (s *Store) Snapshot() map[string]map[string]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deepCopy()
}

// Get returns parameters for a single date (nil-safe).
func (s *Store) Get(date string) map[string]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.params[date]
	if m == nil {
		return map[string]float64{}
	}
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Set stores a value, persists to disk, and broadcasts to subscribers.
func (s *Store) Set(date, key string, value float64) {
	s.mu.Lock()
	if s.params[date] == nil {
		s.params[date] = make(map[string]float64)
	}
	s.params[date][key] = value
	s.flush()
	s.mu.Unlock()

	s.broadcast(Event{Type: "set", Date: date, Key: key, Value: value})
}

// Delete removes a value, persists to disk, and broadcasts to subscribers.
func (s *Store) Delete(date, key string) {
	s.mu.Lock()
	if m, ok := s.params[date]; ok {
		delete(m, key)
		if len(m) == 0 {
			delete(s.params, date)
		}
	}
	s.flush()
	s.mu.Unlock()

	s.broadcast(Event{Type: "delete", Date: date, Key: key})
}

// Subscribe returns a channel that receives events. bufSize controls the
// channel buffer; slow consumers will have events dropped.
func (s *Store) Subscribe(bufSize int) (int, <-chan Event) {
	ch := make(chan Event, bufSize)
	s.subsMu.Lock()
	id := s.nextSubID
	s.nextSubID++
	s.subs[id] = ch
	s.subsMu.Unlock()
	return id, ch
}

// Unsubscribe removes a subscriber and closes its channel.
func (s *Store) Unsubscribe(id int) {
	s.subsMu.Lock()
	if ch, ok := s.subs[id]; ok {
		delete(s.subs, id)
		close(ch)
	}
	s.subsMu.Unlock()
}

// broadcast sends an event to all subscribers non-blocking (drop on full).
func (s *Store) broadcast(e Event) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for _, ch := range s.subs {
		select {
		case ch <- e:
		default:
			// Slow consumer — drop event.
		}
	}
}

// load reads the JSON file into memory.
func (s *Store) load() {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return // File doesn't exist yet — start empty.
	}
	var loaded map[string]map[string]float64
	if err := json.Unmarshal(data, &loaded); err != nil {
		s.log.Warn("loading tradeparams file", "error", err)
		return
	}
	s.params = loaded
	s.log.Info("loaded tradeparams", "dates", len(loaded))
}

// flush writes the in-memory state to disk. Must be called with mu held.
func (s *Store) flush() {
	data, err := json.Marshal(s.params)
	if err != nil {
		s.log.Error("marshalling tradeparams", "error", err)
		return
	}
	if err := os.WriteFile(s.filePath, data, 0644); err != nil {
		s.log.Error("writing tradeparams file", "error", err)
	}
}

// deepCopy returns a deep copy of params. Must be called with mu held (read or write).
func (s *Store) deepCopy() map[string]map[string]float64 {
	out := make(map[string]map[string]float64, len(s.params))
	for date, m := range s.params {
		inner := make(map[string]float64, len(m))
		for k, v := range m {
			inner[k] = v
		}
		out[date] = inner
	}
	return out
}
