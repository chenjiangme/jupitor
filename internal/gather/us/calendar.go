package us

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
)

// LatestFinishedTradingDay returns the most recent trading day whose market
// session has ended (i.e. after 20:05 ET to account for extended hours data
// settling). It uses the Alpaca trading calendar API.
func LatestFinishedTradingDay(apiKey, apiSecret, baseURL string) (time.Time, error) {
	client := alpaca.NewClient(alpaca.ClientOpts{
		APIKey:    apiKey,
		APISecret: apiSecret,
		BaseURL:   baseURL,
	})

	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		return time.Time{}, fmt.Errorf("loading ET timezone: %w", err)
	}

	now := time.Now().In(et)
	start := now.AddDate(0, 0, -7)

	calendar, err := client.GetCalendar(alpaca.GetCalendarRequest{
		Start: start,
		End:   now,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("GetCalendar: %w", err)
	}

	if len(calendar) == 0 {
		return time.Time{}, fmt.Errorf("no trading days returned from calendar")
	}

	today := now.Format("2006-01-02")
	cutoff := time.Date(now.Year(), now.Month(), now.Day(), 20, 5, 0, 0, et)

	for i := len(calendar) - 1; i >= 0; i-- {
		day := calendar[i]
		if day.Date == today {
			if now.After(cutoff) {
				t, _ := time.Parse("2006-01-02", day.Date)
				return t, nil
			}
			continue
		}
		dayDate, err := time.Parse("2006-01-02", day.Date)
		if err != nil {
			continue
		}
		if dayDate.Before(now) {
			return dayDate, nil
		}
	}

	return time.Time{}, fmt.Errorf("could not determine latest finished trading day")
}
