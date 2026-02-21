package cnapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"jupitor/internal/store"
)

// CNServer serves the CN A-share heatmap API.
type CNServer struct {
	dataDir string
	store   *store.ParquetStore
	log     *slog.Logger
	cache   sync.Map // date → *CNHeatmapResponse
	dates   []string // cached date list
	datesMu sync.RWMutex
}

// NewCNServer creates a new CN server.
func NewCNServer(dataDir string, store *store.ParquetStore, log *slog.Logger) *CNServer {
	return &CNServer{
		dataDir: dataDir,
		store:   store,
		log:     log,
	}
}

// Init loads the date list. Call before serving.
func (s *CNServer) Init() error {
	dates, err := ListCNDates(s.dataDir)
	if err != nil {
		return fmt.Errorf("listing CN dates: %w", err)
	}
	s.datesMu.Lock()
	s.dates = dates
	s.datesMu.Unlock()
	s.log.Info("CN dates loaded", "count", len(dates))
	return nil
}

// Handler returns an http.Handler with all routes registered.
func (s *CNServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/cn/heatmap", s.handleHeatmap)
	mux.HandleFunc("GET /api/cn/dates", s.handleDates)
	return corsMiddleware(mux)
}

func (s *CNServer) handleDates(w http.ResponseWriter, r *http.Request) {
	s.datesMu.RLock()
	dates := s.dates
	s.datesMu.RUnlock()

	writeJSON(w, CNDatesResponse{Dates: dates})
}

func (s *CNServer) handleHeatmap(w http.ResponseWriter, r *http.Request) {
	date := r.URL.Query().Get("date")
	if date == "" {
		// Default to latest date.
		s.datesMu.RLock()
		if len(s.dates) > 0 {
			date = s.dates[len(s.dates)-1]
		}
		s.datesMu.RUnlock()
	}
	if date == "" {
		http.Error(w, "no dates available", http.StatusNotFound)
		return
	}

	// Check cache.
	if cached, ok := s.cache.Load(date); ok {
		writeJSON(w, cached.(*CNHeatmapResponse))
		return
	}

	resp, err := s.buildHeatmap(r.Context(), date)
	if err != nil {
		s.log.Error("building heatmap", "date", date, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.cache.Store(date, resp)
	writeJSON(w, resp)
}

func (s *CNServer) buildHeatmap(ctx context.Context, date string) (*CNHeatmapResponse, error) {
	constituents, err := LoadIndexConstituents(s.dataDir, date)
	if err != nil {
		return nil, fmt.Errorf("loading index constituents: %w", err)
	}

	d, err := time.Parse("2006-01-02", date)
	if err != nil {
		return nil, fmt.Errorf("parsing date: %w", err)
	}

	type result struct {
		stock CNHeatmapStock
		ok    bool
	}

	symbols := make([]string, 0, len(constituents))
	for sym := range constituents {
		symbols = append(symbols, sym)
	}

	results := make([]result, len(symbols))
	sem := make(chan struct{}, 32)

	g, gctx := errgroup.WithContext(ctx)

	for i, sym := range symbols {
		i, sym := i, sym
		entry := constituents[sym]
		g.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()

			bars, err := s.store.ReadCNBaoBars(gctx, sym, d, d)
			if err != nil || len(bars) == 0 {
				return nil // skip missing data
			}

			bar := bars[0]
			results[i] = result{
				stock: CNHeatmapStock{
					Symbol: bar.Symbol,
					Name:   entry.Name,
					Index:  entry.Index,
					Turn:   bar.Turn,
					PctChg: bar.PctChg,
					Close:  bar.Close,
					Amount: bar.Amount,
					PeTTM:  bar.PeTTM,
					IsST:   bar.IsST == "1",
				},
				ok: true,
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var stocks []CNHeatmapStock
	for _, r := range results {
		if r.ok {
			stocks = append(stocks, r.stock)
		}
	}

	// Sort by amount descending for consistent treemap layout.
	sort.Slice(stocks, func(i, j int) bool {
		return stocks[i].Amount > stocks[j].Amount
	})

	stats := computeStats(stocks)

	return &CNHeatmapResponse{
		Date:   date,
		Stocks: stocks,
		Stats:  stats,
	}, nil
}

func computeStats(stocks []CNHeatmapStock) CNHeatmapStats {
	if len(stocks) == 0 {
		return CNHeatmapStats{}
	}

	turns := make([]float64, len(stocks))
	for i, s := range stocks {
		turns[i] = s.Turn
	}
	sort.Float64s(turns)

	return CNHeatmapStats{
		TurnP50: percentile(turns, 0.50),
		TurnP90: percentile(turns, 0.90),
		TurnMax: turns[len(turns)-1],
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi || hi >= len(sorted) {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writing JSON response", "error", err)
	}
}
