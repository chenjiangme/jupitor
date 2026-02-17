// Package news provides shared news fetching from multiple sources:
// Alpaca, Google News RSS, GlobeNewswire RSS, and StockTwits.
package news

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v3/marketdata"
)

// Article is a single news article from any source.
type Article struct {
	Time     time.Time
	Source   string
	Headline string
	Content  string
}

// --- HTTP client ---

var httpClient = &http.Client{Timeout: 10 * time.Second}

// --- Alpaca ---

// FetchAlpacaNews fetches news from the Alpaca marketdata API.
func FetchAlpacaNews(mdc *marketdata.Client, symbol string, start, end time.Time) ([]Article, error) {
	alpacaNews, err := mdc.GetNews(marketdata.GetNewsRequest{
		Symbols:            []string{symbol},
		Start:              start,
		End:                end,
		TotalLimit:         50,
		IncludeContent:     true,
		ExcludeContentless: true,
		Sort:               marketdata.SortAsc,
	})
	if err != nil {
		return nil, err
	}

	articles := make([]Article, 0, len(alpacaNews))
	for _, a := range alpacaNews {
		body := ""
		if a.Content != "" {
			body = ExtractSymbolContent(a.Content, symbol)
		} else if a.Summary != "" {
			body = a.Summary
		}
		articles = append(articles, Article{
			Time:     a.CreatedAt,
			Source:   "alpaca",
			Headline: a.Headline,
			Content:  body,
		})
	}
	return articles, nil
}

// --- Google News RSS ---

type rssResponse struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title   string `xml:"title"`
	PubDate string `xml:"pubDate"`
	Desc    string `xml:"description"`
}

// FetchGoogleNews fetches news from Google News RSS.
func FetchGoogleNews(symbol string, start, end time.Time) ([]Article, error) {
	q := url.QueryEscape(symbol + " stock")
	u := "https://news.google.com/rss/search?q=" + q + "&hl=en-US&gl=US&ceid=US:en"

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rss rssResponse
	if err := xml.NewDecoder(resp.Body).Decode(&rss); err != nil {
		return nil, err
	}

	var articles []Article
	for _, item := range rss.Channel.Items {
		t, err := time.Parse(time.RFC1123Z, item.PubDate)
		if err != nil {
			t, err = time.Parse(time.RFC1123, item.PubDate)
			if err != nil {
				continue
			}
		}
		if t.Before(start) || t.After(end) {
			continue
		}
		headline := item.Title
		if idx := strings.LastIndex(headline, " - "); idx > 0 {
			headline = headline[:idx]
		}
		articles = append(articles, Article{
			Time:     t,
			Source:   "google",
			Headline: headline,
			Content:  StripHTML(item.Desc),
		})
	}
	return articles, nil
}

// --- GlobeNewswire RSS ---

// FetchGlobeNewswire fetches news from GlobeNewswire RSS.
func FetchGlobeNewswire(symbol string, start, end time.Time) ([]Article, error) {
	u := "https://www.globenewswire.com/RssFeed/keyword/" + url.PathEscape(symbol) + "/feedTitle/GlobeNewswire.xml"

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rss rssResponse
	if err := xml.NewDecoder(resp.Body).Decode(&rss); err != nil {
		return nil, err
	}

	var articles []Article
	for _, item := range rss.Channel.Items {
		t, err := time.Parse("Mon, 02 Jan 2006 15:04 MST", item.PubDate)
		if err != nil {
			t, err = time.Parse(time.RFC1123Z, item.PubDate)
			if err != nil {
				t, err = time.Parse(time.RFC1123, item.PubDate)
				if err != nil {
					continue
				}
			}
		}
		if t.Before(start) || t.After(end) {
			continue
		}
		articles = append(articles, Article{
			Time:     t,
			Source:   "globenewswire",
			Headline: item.Title,
			Content:  StripHTML(item.Desc),
		})
	}
	return articles, nil
}

// --- StockTwits ---

type stocktwitsResponse struct {
	Response struct {
		Status int `json:"status"`
	} `json:"response"`
	Messages []stocktwitsMessage `json:"messages"`
}

type stocktwitsMessage struct {
	ID        int    `json:"id"`
	Body      string `json:"body"`
	CreatedAt string `json:"created_at"`
	User      struct {
		Username string `json:"username"`
	} `json:"user"`
}

// FetchStockTwits fetches StockTwits messages for a symbol. If paginate is
// true, it pages backwards using the cursor until all messages in the
// [start, end] window are fetched (up to 100 pages). Otherwise it fetches a
// single page (~30 messages). The limiter controls request rate.
func FetchStockTwits(symbol string, start, end time.Time, paginate bool, limiter *time.Ticker) ([]Article, error) {
	baseURL := "https://api.stocktwits.com/api/2/streams/symbol/" + url.PathEscape(symbol) + ".json"

	var all []Article
	seen := make(map[int]bool)
	maxPages := 1
	if paginate {
		maxPages = 100
	}

	cursor := 0
	for page := 0; page < maxPages; page++ {
		<-limiter.C

		u := baseURL
		if cursor > 0 {
			u += fmt.Sprintf("?max=%d", cursor)
		}

		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			return all, err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0")

		resp, err := httpClient.Do(req)
		if err != nil {
			return all, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return all, err
		}

		var st stocktwitsResponse
		if err := json.Unmarshal(body, &st); err != nil {
			return all, err
		}
		if st.Response.Status != 200 {
			return all, fmt.Errorf("stocktwits status %d", st.Response.Status)
		}
		if len(st.Messages) == 0 {
			break
		}

		oldestInWindow := false
		for _, msg := range st.Messages {
			if seen[msg.ID] {
				continue
			}
			seen[msg.ID] = true
			t, err := time.Parse("2006-01-02T15:04:05Z", msg.CreatedAt)
			if err != nil {
				continue
			}
			if t.Before(start) {
				oldestInWindow = true
				continue
			}
			if t.After(end) {
				continue
			}
			text := html.UnescapeString(msg.Body)
			all = append(all, Article{
				Time:     t,
				Source:   "stocktwits",
				Headline: "@" + msg.User.Username,
				Content:  text,
			})
		}

		if oldestInWindow {
			break
		}

		cursor = st.Messages[len(st.Messages)-1].ID
	}

	return all, nil
}

// --- HTML helpers ---

var htmlTagRe = regexp.MustCompile(`<[^>]*>`)
var htmlParaRe = regexp.MustCompile(`(?i)</?(p|br|div|li|h[1-6])\b[^>]*>`)

// StripHTML removes HTML tags and normalizes whitespace.
func StripHTML(s string) string {
	s = htmlTagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

// ExtractSymbolContent extracts paragraphs mentioning the symbol from HTML content.
// Falls back to full stripped HTML if no paragraphs mention the symbol.
func ExtractSymbolContent(rawHTML, symbol string) string {
	chunks := htmlParaRe.Split(rawHTML, -1)
	var matched []string
	upper := strings.ToUpper(symbol)
	for _, chunk := range chunks {
		plain := StripHTML(chunk)
		if plain == "" {
			continue
		}
		if strings.Contains(strings.ToUpper(plain), upper) {
			matched = append(matched, plain)
		}
	}
	if len(matched) > 0 {
		return strings.Join(matched, " ")
	}
	return StripHTML(rawHTML)
}
