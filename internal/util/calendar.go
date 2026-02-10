package util

import (
	"time"

	"jupitor/internal/domain"
)

// TradingCalendar provides market-hours awareness for a specific market.
type TradingCalendar struct {
	market domain.Market
}

// NewTradingCalendar creates a TradingCalendar for the given market.
func NewTradingCalendar(market domain.Market) *TradingCalendar {
	return &TradingCalendar{
		market: market,
	}
}

// IsMarketOpen returns whether the market is open at time t.
func (tc *TradingCalendar) IsMarketOpen(_ time.Time) bool {
	// TODO: implement market-hours check for US (NYSE 9:30-16:00 ET) and
	// CN (SSE 9:30-11:30, 13:00-15:00 CST), accounting for holidays.
	return false
}

// NextOpen returns the next market open time at or after t.
func (tc *TradingCalendar) NextOpen(_ time.Time) time.Time {
	// TODO: compute next trading session open based on market calendar
	return time.Time{}
}

// NextClose returns the next market close time at or after t.
func (tc *TradingCalendar) NextClose(_ time.Time) time.Time {
	// TODO: compute next trading session close based on market calendar
	return time.Time{}
}
