// Package dashboard provides shared types and aggregation logic for the
// live ex-index trade dashboard, used by both the console and TUI clients.
package dashboard

import (
	"math"
	"sort"

	"jupitor/internal/store"
)

// SymbolStats holds aggregated trade statistics for a single symbol.
type SymbolStats struct {
	Symbol    string
	Trades    int
	High      float64
	Low       float64
	Open      float64 // first trade price (by timestamp)
	Close     float64 // last trade price (by timestamp)
	TotalSize int64
	Turnover  float64 // sum(price * size)
	MaxGain   float64 // max possible gain over all (buy, sell) pairs where sell is after buy
	MaxLoss   float64 // max possible loss over all (buy, sell) pairs where sell is after buy
}

// CombinedStats pairs pre-market and regular stats for a single symbol.
type CombinedStats struct {
	Symbol string
	Pre    *SymbolStats // nil if no pre-market trades
	Reg    *SymbolStats // nil if no regular trades
}

// TierGroup holds sorted symbols for a single tier with a count.
type TierGroup struct {
	Name    string
	Count   int
	Symbols []*CombinedStats
}

// DayData holds all computed data for a single day (today or next).
type DayData struct {
	Label     string
	PreCount  int
	RegCount  int
	Tiers     []TierGroup // ACTIVE, MODERATE, SPORADIC (only non-empty)
}

// AggregateTrades computes per-symbol statistics from a slice of trade records.
// Records are sorted by timestamp per symbol to compute temporal max gain/loss.
func AggregateTrades(records []store.TradeRecord) map[string]*SymbolStats {
	// Group record indices by symbol.
	groups := make(map[string][]int)
	for i := range records {
		groups[records[i].Symbol] = append(groups[records[i].Symbol], i)
	}

	m := make(map[string]*SymbolStats, len(groups))
	for sym, indices := range groups {
		sort.Slice(indices, func(a, b int) bool {
			return records[indices[a]].Timestamp < records[indices[b]].Timestamp
		})

		s := &SymbolStats{
			Symbol: sym,
			Low:    math.MaxFloat64,
		}
		minPrice := math.MaxFloat64
		maxPrice := 0.0

		for j, idx := range indices {
			r := &records[idx]
			s.Trades++
			s.Turnover += r.Price * float64(r.Size)
			s.TotalSize += r.Size
			if r.Price > s.High {
				s.High = r.Price
			}
			if r.Price < s.Low {
				s.Low = r.Price
			}
			if j == 0 {
				s.Open = r.Price
			}
			s.Close = r.Price

			// Max gain: buy at lowest seen so far, sell now.
			if r.Price < minPrice {
				minPrice = r.Price
			}
			if minPrice > 0 {
				if g := (r.Price - minPrice) / minPrice; g > s.MaxGain {
					s.MaxGain = g
				}
			}
			// Max loss: buy at highest seen so far, sell now.
			if r.Price > maxPrice {
				maxPrice = r.Price
			}
			if r.Price > 0 {
				if l := (maxPrice - r.Price) / r.Price; l > s.MaxLoss {
					s.MaxLoss = l
				}
			}
		}
		m[sym] = s
	}
	return m
}

var zeroStats SymbolStats

// SortMode defines the sort order for the dashboard.
const (
	SortPreTrades   = 0 // pre-market by trades (default)
	SortPreGain     = 1 // pre-market by gain%
	SortRegTrades   = 2 // regular by trades
	SortRegGain     = 3 // regular by gain%
	SortPreTurnover = 4 // pre-market by turnover
	SortRegTurnover = 5 // regular by turnover
	SortNews        = 6 // by news count (desc)
	SortModeCount   = 7
)

// SortModeLabel returns a short label for the given sort mode.
func SortModeLabel(mode int) string {
	switch mode {
	case SortPreTrades:
		return "PRE:TRD"
	case SortPreGain:
		return "PRE:GAIN"
	case SortRegTrades:
		return "REG:TRD"
	case SortRegGain:
		return "REG:GAIN"
	case SortPreTurnover:
		return "PRE:TO"
	case SortRegTurnover:
		return "REG:TO"
	case SortNews:
		return "NEWS"
	default:
		return "?"
	}
}

// sessionStats returns the relevant session stats for sorting.
func sessionStats(c *CombinedStats, regular bool) *SymbolStats {
	if regular {
		if c.Reg != nil {
			return c.Reg
		}
		return &zeroStats
	}
	if c.Pre != nil {
		return c.Pre
	}
	return &zeroStats
}

// SplitBySession splits trades into pre-market and regular session based on
// the 9:30 AM ET cutoff (expressed in ET-shifted milliseconds).
func SplitBySession(trades []store.TradeRecord, open930ET int64) (pre, reg []store.TradeRecord) {
	for i := range trades {
		if trades[i].Timestamp < open930ET {
			pre = append(pre, trades[i])
		} else {
			reg = append(reg, trades[i])
		}
	}
	return
}

// sortSymbols sorts a slice of CombinedStats by the given sort mode.
func sortSymbols(ss []*CombinedStats, mode int) {
	regular := mode == SortRegTrades || mode == SortRegGain || mode == SortRegTurnover
	sort.Slice(ss, func(i, j int) bool {
		si, sj := sessionStats(ss[i], regular), sessionStats(ss[j], regular)
		switch mode {
		case SortPreGain, SortRegGain:
			if si.MaxGain != sj.MaxGain {
				return si.MaxGain > sj.MaxGain
			}
			return si.Turnover > sj.Turnover
		case SortPreTurnover, SortRegTurnover:
			if si.Turnover != sj.Turnover {
				return si.Turnover > sj.Turnover
			}
			return si.Trades > sj.Trades
		default: // SortPreTrades, SortRegTrades
			if si.Trades != sj.Trades {
				return si.Trades > sj.Trades
			}
			return si.Turnover > sj.Turnover
		}
	})
}

// ResortDayData re-sorts the symbols within each tier group without
// recomputing aggregation. Used when toggling sort mode.
func ResortDayData(d *DayData, sortMode int) {
	for i := range d.Tiers {
		sortSymbols(d.Tiers[i].Symbols, sortMode)
	}
}

// filterTopN keeps only stocks that are in the top N of any metric
// (trades, turnover, gain%) in either pre or regular session.
func filterTopN(ss []*CombinedStats, n int) []*CombinedStats {
	if len(ss) <= n {
		return ss
	}

	keep := make(map[string]bool)

	// For each session × metric, sort a copy and mark the top N.
	type extractor func(*CombinedStats) float64
	metrics := []extractor{
		func(c *CombinedStats) float64 {
			if c.Pre != nil {
				return float64(c.Pre.Trades)
			}
			return 0
		},
		func(c *CombinedStats) float64 {
			if c.Pre != nil {
				return c.Pre.Turnover
			}
			return 0
		},
		func(c *CombinedStats) float64 {
			if c.Pre != nil {
				return c.Pre.MaxGain
			}
			return 0
		},
		func(c *CombinedStats) float64 {
			if c.Reg != nil {
				return float64(c.Reg.Trades)
			}
			return 0
		},
		func(c *CombinedStats) float64 {
			if c.Reg != nil {
				return c.Reg.Turnover
			}
			return 0
		},
		func(c *CombinedStats) float64 {
			if c.Reg != nil {
				return c.Reg.MaxGain
			}
			return 0
		},
	}

	tmp := make([]*CombinedStats, len(ss))
	for _, fn := range metrics {
		copy(tmp, ss)
		sort.Slice(tmp, func(i, j int) bool {
			return fn(tmp[i]) > fn(tmp[j])
		})
		for i := 0; i < n && i < len(tmp); i++ {
			keep[tmp[i].Symbol] = true
		}
	}

	result := make([]*CombinedStats, 0, len(keep))
	for _, c := range ss {
		if keep[c.Symbol] {
			result = append(result, c)
		}
	}
	return result
}

// ComputeDayData builds a complete DayData for a set of trades. It splits by
// session, aggregates, merges, filters (gain>=10% and trades>=100), groups by
// tier, and sorts within each tier.
func ComputeDayData(label string, trades []store.TradeRecord, tierMap map[string]string, open930ET int64, sortMode int) DayData {
	pre, reg := SplitBySession(trades, open930ET)
	preStats := AggregateTrades(pre)
	regStats := AggregateTrades(reg)

	// Merge into combined stats per symbol.
	combined := make(map[string]*CombinedStats)
	for sym, s := range preStats {
		combined[sym] = &CombinedStats{Symbol: sym, Pre: s}
	}
	for sym, s := range regStats {
		if c, ok := combined[sym]; ok {
			c.Reg = s
		} else {
			combined[sym] = &CombinedStats{Symbol: sym, Reg: s}
		}
	}

	// Group by tier, filtering by gain>=10% and trades>=100.
	tiers := map[string][]*CombinedStats{
		"ACTIVE":   {},
		"MODERATE": {},
		"SPORADIC": {},
	}
	tierCounts := map[string]int{"ACTIVE": 0, "MODERATE": 0, "SPORADIC": 0}

	for sym, c := range combined {
		preOK := c.Pre != nil && c.Pre.MaxGain >= 0.10 && c.Pre.Trades >= 500
		regOK := c.Reg != nil && c.Reg.MaxGain >= 0.10 && c.Reg.Trades >= 500
		if !preOK && !regOK {
			continue
		}
		tier, ok := tierMap[sym]
		if !ok {
			continue
		}
		tiers[tier] = append(tiers[tier], c)
		tierCounts[tier]++
	}

	// Within each tier, keep only stocks in the top N of any metric
	// (trades, turnover, or gain%) in either session.
	tierTopN := map[string]int{"ACTIVE": 8, "MODERATE": 12, "SPORADIC": 12}
	for tier, ss := range tiers {
		tiers[tier] = filterTopN(ss, tierTopN[tier])
	}

	// Sort within each tier.
	for _, ss := range tiers {
		sortSymbols(ss, sortMode)
	}

	var groups []TierGroup
	for _, name := range []string{"ACTIVE", "MODERATE", "SPORADIC"} {
		if len(tiers[name]) > 0 {
			groups = append(groups, TierGroup{
				Name:    name,
				Count:   len(tiers[name]),
				Symbols: tiers[name],
			})
		}
	}

	return DayData{
		Label:    label,
		PreCount: len(pre),
		RegCount: len(reg),
		Tiers:    groups,
	}
}
