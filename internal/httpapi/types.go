// Package httpapi provides an HTTP REST API for the us-stream dashboard,
// serving the same data as the TUI client in JSON format.
package httpapi

import (
	"jupitor/internal/dashboard"
)

// SymbolStatsJSON is the JSON representation of per-symbol session stats.
type SymbolStatsJSON struct {
	Symbol    string  `json:"symbol"`
	Trades    int     `json:"trades"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Open      float64 `json:"open"`
	Close     float64 `json:"close"`
	Size      int64   `json:"size"`
	Turnover  float64 `json:"turnover"`
	MaxGain   float64 `json:"maxGain"`
	MaxLoss   float64 `json:"maxLoss"`
	GainFirst   bool    `json:"gainFirst,omitempty"`
	CloseGain   float64 `json:"closeGain,omitempty"`
	MaxDrawdown float64 `json:"maxDrawdown,omitempty"`
}

// CombinedStatsJSON pairs pre-market and regular session stats.
type CombinedStatsJSON struct {
	Symbol string           `json:"symbol"`
	Tier   string           `json:"tier"`
	Pre    *SymbolStatsJSON `json:"pre,omitempty"`
	Reg    *SymbolStatsJSON `json:"reg,omitempty"`
	News   int              `json:"news,omitempty"`   // non-StockTwits article count
	StPre  int              `json:"stPre,omitempty"`  // StockTwits before 9:30 AM ET
	StReg  int              `json:"stReg,omitempty"`  // StockTwits 9:30 AM – 4 PM ET
	StPost int              `json:"stPost,omitempty"` // StockTwits after 4 PM ET
}

// SymbolNewsCounts holds per-symbol news counts broken down by source and session.
type SymbolNewsCounts struct {
	News   int // non-StockTwits articles
	StPre  int // StockTwits before 9:30 AM ET
	StReg  int // StockTwits 9:30 AM – 4 PM ET
	StPost int // StockTwits after 4 PM ET
}

// TierGroupJSON holds sorted symbols for one tier.
type TierGroupJSON struct {
	Name    string              `json:"name"`
	Count   int                 `json:"count"`
	Symbols []CombinedStatsJSON `json:"symbols"`
}

// DayDataJSON holds computed data for a single day.
type DayDataJSON struct {
	Label    string          `json:"label"`
	Date     string          `json:"date,omitempty"`
	PreCount int             `json:"preCount"`
	RegCount int             `json:"regCount"`
	Tiers    []TierGroupJSON `json:"tiers"`
}

// DashboardResponse is the top-level JSON response for dashboard endpoints.
type DashboardResponse struct {
	Date      string      `json:"date"`
	Today     DayDataJSON `json:"today"`
	Next      *DayDataJSON `json:"next,omitempty"`
	SortMode  int         `json:"sortMode"`
	SortLabel string      `json:"sortLabel"`
}

// DatesResponse lists available history dates.
type DatesResponse struct {
	Dates []string `json:"dates"`
}

// WatchlistResponse lists watchlist symbols.
type WatchlistResponse struct {
	Symbols []string `json:"symbols"`
}

// NewsArticleJSON is a single news article.
type NewsArticleJSON struct {
	Time     int64  `json:"time"`
	Source   string `json:"source"`
	Headline string `json:"headline"`
	Content  string `json:"content,omitempty"`
}

// NewsResponse holds news articles for a symbol.
type NewsResponse struct {
	Symbol   string            `json:"symbol"`
	Date     string            `json:"date"`
	Articles []NewsArticleJSON `json:"articles"`
}

// SymbolDateStats holds pre/reg stats for a symbol on a single date.
type SymbolDateStats struct {
	Date string           `json:"date"`
	Pre  *SymbolStatsJSON `json:"pre,omitempty"`
	Reg  *SymbolStatsJSON `json:"reg,omitempty"`
}

// SymbolHistoryResponse is the response for the symbol history endpoint.
type SymbolHistoryResponse struct {
	Symbol  string            `json:"symbol"`
	Dates   []SymbolDateStats `json:"dates"`
	HasMore bool              `json:"hasMore"`
}

// convertSymbolStats converts a dashboard.SymbolStats to JSON.
func convertSymbolStats(s *dashboard.SymbolStats) *SymbolStatsJSON {
	if s == nil {
		return nil
	}
	return &SymbolStatsJSON{
		Symbol:    s.Symbol,
		Trades:    s.Trades,
		High:      s.High,
		Low:       s.Low,
		Open:      s.Open,
		Close:     s.Close,
		Size:      s.TotalSize,
		Turnover:  s.Turnover,
		MaxGain:   s.MaxGain,
		MaxLoss:   s.MaxLoss,
		GainFirst:   s.GainFirst,
		CloseGain:   s.CloseGain,
		MaxDrawdown: s.MaxDrawdown,
	}
}

// convertDayData converts a dashboard.DayData to JSON, enriching with
// tier names and optional news counts.
func convertDayData(d dashboard.DayData, newsCounts map[string]*SymbolNewsCounts) DayDataJSON {
	tiers := make([]TierGroupJSON, 0, len(d.Tiers))
	for _, tier := range d.Tiers {
		symbols := make([]CombinedStatsJSON, 0, len(tier.Symbols))
		for _, c := range tier.Symbols {
			cs := CombinedStatsJSON{
				Symbol: c.Symbol,
				Tier:   tier.Name,
				Pre:    convertSymbolStats(c.Pre),
				Reg:    convertSymbolStats(c.Reg),
			}
			if nc := newsCounts[c.Symbol]; nc != nil {
				cs.News = nc.News
				cs.StPre = nc.StPre
				cs.StReg = nc.StReg
				cs.StPost = nc.StPost
			}
			symbols = append(symbols, cs)
		}
		tiers = append(tiers, TierGroupJSON{
			Name:    tier.Name,
			Count:   tier.Count,
			Symbols: symbols,
		})
	}
	return DayDataJSON{
		Label:    d.Label,
		PreCount: d.PreCount,
		RegCount: d.RegCount,
		Tiers:    tiers,
	}
}
