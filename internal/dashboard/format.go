package dashboard

import (
	"fmt"
	"math"
	"strings"
)

// FormatInt formats an integer with comma separators.
func FormatInt(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	start := len(s) % 3
	if start > 0 {
		b.WriteString(s[:start])
	}
	for i := start; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

// FormatTurnover formats a dollar turnover value with B/M/K suffixes.
func FormatTurnover(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.1fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fK", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

// FormatPrice formats a price value as $X.XX, or "-" for zero/max.
func FormatPrice(p float64) string {
	if p == math.MaxFloat64 || p == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", p)
}

// FormatGain formats a gain percentage as "+X.X%", or "" if zero.
// Drops decimal for values >= 100% to keep width compact.
func FormatGain(g float64) string {
	if g <= 0 {
		return ""
	}
	pct := g * 100
	if pct >= 100 {
		return fmt.Sprintf("+%.0f%%", pct)
	}
	return fmt.Sprintf("+%.1f%%", pct)
}

// FormatLoss formats a loss percentage as "-X.X%", or "" if zero.
// Drops decimal for values >= 100% to keep width compact.
func FormatLoss(l float64) string {
	if l <= 0 {
		return ""
	}
	pct := l * 100
	if pct >= 100 {
		return fmt.Sprintf("-%.0f%%", pct)
	}
	return fmt.Sprintf("-%.1f%%", pct)
}

// FormatCount formats a trade count, using K suffix for large values.
func FormatCount(n int) string {
	if n >= 100_000 {
		return fmt.Sprintf("%.0fK", float64(n)/1e3)
	}
	return FormatInt(n)
}
