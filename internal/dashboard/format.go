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
		return fmt.Sprintf("$%.1fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("$%.1fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("$%.1fK", v/1e3)
	default:
		return fmt.Sprintf("$%.0f", v)
	}
}

// FormatPrice formats a price value as $X.XX, or "-" for zero/max.
func FormatPrice(p float64) string {
	if p == math.MaxFloat64 || p == 0 {
		return "-"
	}
	return fmt.Sprintf("$%.2f", p)
}

// FormatGain formats a gain percentage as "+X.X%", or "" if zero.
func FormatGain(g float64) string {
	if g > 0 {
		return fmt.Sprintf("+%.1f%%", g*100)
	}
	return ""
}

// FormatLoss formats a loss percentage as "-X.X%", or "" if zero.
func FormatLoss(l float64) string {
	if l > 0 {
		return fmt.Sprintf("-%.1f%%", l*100)
	}
	return ""
}
