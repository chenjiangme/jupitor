// Package dashboard provides shared types and aggregation logic for the
// live ex-index trade dashboard, used by both the console and TUI clients.
package dashboard

import (
	"math"
	"sort"
	"strings"

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
	GainFirst    bool    // true if max gain was reached before max loss
	CloseGain    float64 // (close - low) / vwap using same VWAP logic as MaxGain
	MaxDrawdown  float64 // (peakPrice - minAfterPeak) / vwap — drawdown from max gain point
	TradeProfile    []int   // trade count per 1% VWAP bucket from low to high
	TradeProfile30m [][]int // per-30m-period trade count profile (same buckets as TradeProfile)
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
// An optional sessionStartMS provides the session start time (Unix ms) for
// computing per-30-minute trade profiles (TradeProfile30m).
func AggregateTrades(records []store.TradeRecord, sessionStartMS ...int64) map[string]*SymbolStats {
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

		// --- Pass 1: basic stats + VWAP ---
		s := &SymbolStats{
			Symbol: sym,
			Low:    math.MaxFloat64,
		}
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
		}

		if s.TotalSize == 0 {
			m[sym] = s
			continue
		}
		vwap := s.Turnover / float64(s.TotalSize)

		// --- Outlier detection: trim top/bottom 1% if 3x away from VWAP ---
		outlier := make([]bool, len(indices))
		if len(indices) >= 100 {
			sorted := make([]int, len(indices))
			copy(sorted, indices)
			sort.Slice(sorted, func(a, b int) bool {
				return records[sorted[a]].Price < records[sorted[b]].Price
			})
			trimN := len(sorted) / 100 // 1%
			lowCut := vwap / 3
			highCut := vwap * 3
			// Build a set of outlier record indices.
			outlierIdx := make(map[int]bool)
			for i := 0; i < trimN; i++ {
				if records[sorted[i]].Price < lowCut {
					outlierIdx[sorted[i]] = true
				}
			}
			for i := len(sorted) - trimN; i < len(sorted); i++ {
				if records[sorted[i]].Price > highCut {
					outlierIdx[sorted[i]] = true
				}
			}
			// Map back to timestamp-order positions.
			for j, idx := range indices {
				if outlierIdx[idx] {
					outlier[j] = true
				}
			}
		}

		// --- Pass 2: recompute high/low + max gain/loss excluding outliers ---
		minPrice := math.MaxFloat64
		maxPrice := 0.0
		gainIdx := -1
		lossIdx := -1
		bestGain := 0.0
		bestLoss := 0.0
		hasOutliers := false

		trimmedHigh := 0.0
		trimmedLow := math.MaxFloat64
		for j, idx := range indices {
			if outlier[j] {
				hasOutliers = true
				continue
			}
			r := &records[idx]
			if r.Price > trimmedHigh {
				trimmedHigh = r.Price
			}
			if r.Price < trimmedLow {
				trimmedLow = r.Price
			}
			if r.Price < minPrice {
				minPrice = r.Price
			}
			if g := r.Price - minPrice; g > bestGain {
				bestGain = g
				gainIdx = j
			}
			if r.Price > maxPrice {
				maxPrice = r.Price
			}
			if l := maxPrice - r.Price; l > bestLoss {
				bestLoss = l
				lossIdx = j
			}
		}
		if hasOutliers {
			s.High = trimmedHigh
			s.Low = trimmedLow
		}

		// Track which max was reached first.
		if gainIdx >= 0 && lossIdx >= 0 {
			s.GainFirst = gainIdx <= lossIdx
		} else {
			s.GainFirst = gainIdx >= 0
		}

		// Find max drawdown from the peak (lowest price after gainIdx).
		peakPrice := 0.0
		minAfterPeak := math.MaxFloat64
		if gainIdx >= 0 {
			peakPrice = records[indices[gainIdx]].Price
			for j := gainIdx + 1; j < len(indices); j++ {
				if outlier[j] {
					continue
				}
				if records[indices[j]].Price < minAfterPeak {
					minAfterPeak = records[indices[j]].Price
				}
			}
		}

		// Compute VWAP between the max-gain and max-loss time points,
		// then express gain/loss as percentages relative to that center.
		// effectiveVwap is reused for the trade profile below.
		effectiveVwap := vwap
		if gainIdx >= 0 && lossIdx >= 0 && gainIdx != lossIdx {
			lo, hi := gainIdx, lossIdx
			if lo > hi {
				lo, hi = hi, lo
			}
			var totalValue float64
			var totalSize int64
			for j := lo; j <= hi; j++ {
				if outlier[j] {
					continue
				}
				r := &records[indices[j]]
				totalValue += r.Price * float64(r.Size)
				totalSize += r.Size
			}
			if totalSize > 0 {
				windowVwap := totalValue / float64(totalSize)
				if windowVwap > 0 {
					effectiveVwap = windowVwap
					s.MaxGain = bestGain / windowVwap
					s.MaxLoss = bestLoss / windowVwap
					if s.Close > s.Low {
						s.CloseGain = (s.Close - s.Low) / windowVwap
					}
					if peakPrice > minAfterPeak {
						s.MaxDrawdown = (peakPrice - minAfterPeak) / windowVwap
					}
				}
			}
		} else if vwap > 0 {
			s.MaxGain = bestGain / vwap
			s.MaxLoss = bestLoss / vwap
			if s.Close > s.Low {
				s.CloseGain = (s.Close - s.Low) / vwap
			}
			if peakPrice > minAfterPeak {
				s.MaxDrawdown = (peakPrice - minAfterPeak) / vwap
			}
		}

		// Compute trade profile: 1% buckets using the same VWAP as gain/loss.
		if effectiveVwap > 0 && s.High > s.Low {
			nBuckets := int(math.Ceil((s.High - s.Low) / effectiveVwap * 100))
			if nBuckets > 500 {
				nBuckets = 500
			}
			profile := make([]int, nBuckets)
			scale := 100.0 / effectiveVwap
			for j, idx := range indices {
				if outlier[j] {
					continue
				}
				bucket := int((records[idx].Price - s.Low) * scale)
				if bucket >= nBuckets {
					bucket = nBuckets - 1
				}
				if bucket >= 0 {
					profile[bucket]++
				}
			}
			s.TradeProfile = profile

			// Compute per-hour trade profiles if session start is provided.
			// Aligned to clock-hour boundaries for clean hour markers.
			if len(sessionStartMS) > 0 && sessionStartMS[0] > 0 {
				const periodHour int64 = 60 * 60 * 1000
				const maxPeriods = 20
				// Floor session start to nearest clock hour.
				hourFloor := (sessionStartMS[0] / periodHour) * periodHour
				var profileHourly [][]int
				for j, idx := range indices {
					if outlier[j] {
						continue
					}
					r := &records[idx]
					p := int((r.Timestamp - hourFloor) / periodHour)
					if p < 0 {
						p = 0
					}
					if p >= maxPeriods {
						p = maxPeriods - 1
					}
					// Grow slice as needed.
					for len(profileHourly) <= p {
						profileHourly = append(profileHourly, make([]int, nBuckets))
					}
					bucket := int((r.Price - s.Low) * scale)
					if bucket >= nBuckets {
						bucket = nBuckets - 1
					}
					if bucket >= 0 {
						profileHourly[p][bucket]++
					}
				}
				if len(profileHourly) > 0 {
					s.TradeProfile30m = profileHourly
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
			vi, vj := fn(tmp[i]), fn(tmp[j])
			if vi != vj {
				return vi > vj
			}
			return tmp[i].Symbol < tmp[j].Symbol
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

// FilterTradesBySymbol returns only the trades matching the given symbol.
func FilterTradesBySymbol(trades []store.TradeRecord, symbol string) []store.TradeRecord {
	var out []store.TradeRecord
	for i := range trades {
		if trades[i].Symbol == symbol {
			out = append(out, trades[i])
		}
	}
	return out
}

// allowedConds for exchange/condition filter matching consolidated trade files.
var allowedConds = map[string]bool{" ": true, "@": true, "T": true, "F": true}

// FilterTradeRecords applies the same exchange/condition filter used by the
// consolidated stock-trades files: exchange != "D", all conditions in {" ","@","T","F"}.
func FilterTradeRecords(trades []store.TradeRecord) []store.TradeRecord {
	out := make([]store.TradeRecord, 0, len(trades))
	for i := range trades {
		r := &trades[i]
		if r.Exchange == "D" {
			continue
		}
		if r.Conditions != "" {
			ok := true
			for _, c := range strings.Split(r.Conditions, ",") {
				if !allowedConds[c] {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
		}
		out = append(out, trades[i])
	}
	return out
}

// ComputeDayData builds a complete DayData for a set of trades. It splits by
// session, aggregates, merges, filters (gain>=10% and trades>=100), groups by
// tier, and sorts within each tier.
func ComputeDayData(label string, trades []store.TradeRecord, tierMap map[string]string, open930ET int64, sortMode int) DayData {
	pre, reg := SplitBySession(trades, open930ET)
	preStartET := open930ET - 330*60*1000 // 4:00 AM ET (5.5 hours before 9:30)
	preStats := AggregateTrades(pre, preStartET)
	regStats := AggregateTrades(reg, open930ET)

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
	tierTopN := map[string]int{"ACTIVE": 5, "MODERATE": 8, "SPORADIC": 8}
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
