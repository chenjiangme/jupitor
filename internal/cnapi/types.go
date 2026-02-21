package cnapi

// CNHeatmapStock is one stock in the heatmap response.
type CNHeatmapStock struct {
	Symbol string  `json:"symbol"`
	Name   string  `json:"name"`
	Index  string  `json:"index"` // "csi300" or "csi500"
	Turn   float64 `json:"turn"`  // turnover rate %
	PctChg float64 `json:"pctChg"`
	Close  float64 `json:"close"`
	Amount float64 `json:"amount"` // trading amount (CNY)
	PeTTM  float64 `json:"peTTM"`
	IsST   bool    `json:"isST"`
}

// CNHeatmapStats holds percentile statistics for turnover rates.
type CNHeatmapStats struct {
	TurnP50 float64 `json:"turnP50"`
	TurnP90 float64 `json:"turnP90"`
	TurnMax float64 `json:"turnMax"`
}

// CNHeatmapResponse is the full heatmap API response.
type CNHeatmapResponse struct {
	Date   string           `json:"date"`
	Stocks []CNHeatmapStock `json:"stocks"`
	Stats  CNHeatmapStats   `json:"stats"`
}

// CNDatesResponse lists available dates.
type CNDatesResponse struct {
	Dates []string `json:"dates"`
}

// CNSymbolDay is one trading day in symbol history.
type CNSymbolDay struct {
	Date   string  `json:"date"`
	Turn   float64 `json:"turn"`
	PctChg float64 `json:"pctChg"`
	Close  float64 `json:"close"`
}

// CNSymbolHistoryResponse is the symbol history API response.
type CNSymbolHistoryResponse struct {
	Symbol string         `json:"symbol"`
	Name   string         `json:"name"`
	Days   []CNSymbolDay  `json:"days"`
}
