// Package live provides a shared in-memory model for live trade data,
// with dedup, today/next-day classification, and pub/sub for gRPC streaming.
package live

import (
	"strconv"
	"sync"

	"jupitor/internal/store"
)

// TradeEvent is emitted to subscribers when a new trade is added to the model.
type TradeEvent struct {
	Record  store.TradeRecord
	IsIndex bool
	IsToday bool // true = today's trading day window, false = next day (post-market)
}

// tradeKey uniquely identifies a trade by (ID, Exchange). The same numeric
// trade ID can appear on different exchanges, so both fields are needed.
type tradeKey struct {
	ID       int64
	Exchange string
}

// LiveModel holds live trade data split into today and next-day buckets,
// with dedup by (trade_id, exchange) and pub/sub for streaming to gRPC clients.
type LiveModel struct {
	mu          sync.RWMutex
	todayIndex  []store.TradeRecord
	todayExIdx  []store.TradeRecord
	nextIndex   []store.TradeRecord
	nextExIdx   []store.TradeRecord
	seen        map[tradeKey]bool // (trade_id, exchange) for dedup
	todayCutoff int64             // D 4PM ET as Unix ms

	subsMu    sync.Mutex
	nextSubID int
	subs      map[int]chan TradeEvent
}

// NewLiveModel creates a model with the given cutoff (D 4PM ET in Unix ms).
// Trades with timestamp <= cutoff go to today's bucket; > cutoff to next day.
func NewLiveModel(todayCutoff int64) *LiveModel {
	return &LiveModel{
		seen:        make(map[tradeKey]bool),
		todayCutoff: todayCutoff,
		subs:        make(map[int]chan TradeEvent),
	}
}

// Add inserts a single trade into the model. It deduplicates by trade ID,
// classifies by timestamp, and notifies subscribers. Returns false if duplicate.
func (m *LiveModel) Add(record store.TradeRecord, rawID int64, isIndex bool) bool {
	key := tradeKey{ID: rawID, Exchange: record.Exchange}
	m.mu.Lock()
	if m.seen[key] {
		m.mu.Unlock()
		return false
	}
	m.seen[key] = true

	isToday := record.Timestamp <= m.todayCutoff
	if isToday {
		if isIndex {
			m.todayIndex = append(m.todayIndex, record)
		} else {
			m.todayExIdx = append(m.todayExIdx, record)
		}
	} else {
		if isIndex {
			m.nextIndex = append(m.nextIndex, record)
		} else {
			m.nextExIdx = append(m.nextExIdx, record)
		}
	}
	m.mu.Unlock()

	// Notify subscribers (non-blocking send).
	evt := TradeEvent{Record: record, IsIndex: isIndex, IsToday: isToday}
	m.subsMu.Lock()
	for _, ch := range m.subs {
		select {
		case ch <- evt:
		default:
			// Slow subscriber, drop event.
		}
	}
	m.subsMu.Unlock()

	return true
}

// AddBatch inserts multiple trades in bulk (from backfill). Returns the count
// of new (non-duplicate) trades added. Subscribers are NOT notified for batch
// adds — backfill trades are sent as part of the snapshot instead.
func (m *LiveModel) AddBatch(records []store.TradeRecord, rawIDs []int64, isIndex bool) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	added := 0
	for i := range records {
		key := tradeKey{ID: rawIDs[i], Exchange: records[i].Exchange}
		if m.seen[key] {
			continue
		}
		m.seen[key] = true
		added++

		if records[i].Timestamp <= m.todayCutoff {
			if isIndex {
				m.todayIndex = append(m.todayIndex, records[i])
			} else {
				m.todayExIdx = append(m.todayExIdx, records[i])
			}
		} else {
			if isIndex {
				m.nextIndex = append(m.nextIndex, records[i])
			} else {
				m.nextExIdx = append(m.nextExIdx, records[i])
			}
		}
	}
	return added
}

// TodaySnapshot returns copies of the current trading day's trades.
func (m *LiveModel) TodaySnapshot() (index, exIndex []store.TradeRecord) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	index = make([]store.TradeRecord, len(m.todayIndex))
	copy(index, m.todayIndex)
	exIndex = make([]store.TradeRecord, len(m.todayExIdx))
	copy(exIndex, m.todayExIdx)
	return
}

// NextSnapshot returns copies of the next trading day's trades (post-market).
func (m *LiveModel) NextSnapshot() (index, exIndex []store.TradeRecord) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	index = make([]store.TradeRecord, len(m.nextIndex))
	copy(index, m.nextIndex)
	exIndex = make([]store.TradeRecord, len(m.nextExIdx))
	copy(exIndex, m.nextExIdx)
	return
}

// Counts returns the number of trades in each bucket.
func (m *LiveModel) Counts() (todayIdx, todayExIdx, nextIdx, nextExIdx int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.todayIndex), len(m.todayExIdx), len(m.nextIndex), len(m.nextExIdx)
}

// SeenCount returns the total number of unique trade IDs seen (for logging).
func (m *LiveModel) SeenCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.seen)
}

// SwitchDay advances the model to a new trading day. Old today is disposed,
// next is promoted to today, and the seen map is rebuilt from surviving records.
func (m *LiveModel) SwitchDay(newCutoff int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Dispose old today, promote next → today.
	m.todayIndex = m.nextIndex
	m.todayExIdx = m.nextExIdx
	m.nextIndex = nil
	m.nextExIdx = nil

	// Update cutoff.
	m.todayCutoff = newCutoff

	// Rebuild seen from surviving records (frees old trade IDs from memory).
	m.seen = make(map[tradeKey]bool, len(m.todayIndex)+len(m.todayExIdx))
	for _, r := range m.todayIndex {
		id, _ := strconv.ParseInt(r.ID, 10, 64)
		m.seen[tradeKey{ID: id, Exchange: r.Exchange}] = true
	}
	for _, r := range m.todayExIdx {
		id, _ := strconv.ParseInt(r.ID, 10, 64)
		m.seen[tradeKey{ID: id, Exchange: r.Exchange}] = true
	}
}

// Subscribe creates a new subscription channel for live trade events.
func (m *LiveModel) Subscribe(bufSize int) (id int, ch <-chan TradeEvent) {
	m.subsMu.Lock()
	defer m.subsMu.Unlock()
	id = m.nextSubID
	m.nextSubID++
	c := make(chan TradeEvent, bufSize)
	m.subs[id] = c
	return id, c
}

// Unsubscribe removes a subscription and closes its channel.
func (m *LiveModel) Unsubscribe(id int) {
	m.subsMu.Lock()
	defer m.subsMu.Unlock()
	if ch, ok := m.subs[id]; ok {
		close(ch)
		delete(m.subs, id)
	}
}
