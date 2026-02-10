package us

import (
	"encoding/csv"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
)

// GenerateBruteSymbols returns all A-Z combinations of lengths 1 through 4.
// Total: 26 + 676 + 17576 + 456976 = 475254 symbols.
func GenerateBruteSymbols() []string {
	total := 26 + 26*26 + 26*26*26 + 26*26*26*26
	symbols := make([]string, 0, total)
	var buf [4]byte

	for a := byte('A'); a <= 'Z'; a++ {
		buf[0] = a
		symbols = append(symbols, string(buf[:1]))
		for b := byte('A'); b <= 'Z'; b++ {
			buf[1] = b
			symbols = append(symbols, string(buf[:2]))
			for c := byte('A'); c <= 'Z'; c++ {
				buf[2] = c
				symbols = append(symbols, string(buf[:3]))
				for d := byte('A'); d <= 'Z'; d++ {
					buf[3] = d
					symbols = append(symbols, string(buf[:4]))
				}
			}
		}
	}
	return symbols
}

// LoadCSVSymbols reads the first column ("symbol") from a CSV file and returns
// all symbols found. The file must have a header row.
func LoadCSVSymbols(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening CSV %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading CSV %s: %w", path, err)
	}

	if len(records) < 2 {
		return nil, nil
	}

	symbols := make([]string, 0, len(records)-1)
	for _, row := range records[1:] {
		if len(row) > 0 {
			sym := strings.TrimSpace(row[0])
			if sym != "" {
				symbols = append(symbols, strings.ToUpper(sym))
			}
		}
	}
	return symbols, nil
}

// AllBruteSymbols combines brute-force A-Z symbols (1-4 chars) with CSV
// symbols (5+ chars), deduplicates, and shuffles the result.
func AllBruteSymbols(csvPath string) ([]string, error) {
	brute := GenerateBruteSymbols()

	seen := make(map[string]struct{}, len(brute))
	for _, s := range brute {
		seen[s] = struct{}{}
	}

	csvSymbols, err := LoadCSVSymbols(csvPath)
	if err != nil {
		return nil, err
	}
	for _, s := range csvSymbols {
		if _, ok := seen[s]; !ok {
			brute = append(brute, s)
			seen[s] = struct{}{}
		}
	}

	rand.Shuffle(len(brute), func(i, j int) {
		brute[i], brute[j] = brute[j], brute[i]
	})
	return brute, nil
}
