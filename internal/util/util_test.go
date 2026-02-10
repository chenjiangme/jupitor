package util

import (
	"context"
	"errors"
	"testing"

	"jupitor/internal/domain"
)

func TestRetry(t *testing.T) {
	attempts := 0
	targetAttempts := 3

	err := Retry(context.Background(), 5, 0, func() error {
		attempts++
		if attempts < targetAttempts {
			return errors.New("transient error")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Retry returned unexpected error: %v", err)
	}
	if attempts != targetAttempts {
		t.Errorf("Retry called fn %d times, want %d", attempts, targetAttempts)
	}
}

func TestRetryAllFail(t *testing.T) {
	attempts := 0
	maxAttempts := 3

	err := Retry(context.Background(), maxAttempts, 0, func() error {
		attempts++
		return errors.New("persistent error")
	})

	if err == nil {
		t.Fatal("Retry should return error when all attempts fail")
	}
	if attempts != maxAttempts {
		t.Errorf("Retry called fn %d times, want %d", attempts, maxAttempts)
	}
}

func TestRateLimiterNew(t *testing.T) {
	rl := NewRateLimiter(60)
	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}
}

func TestTradingCalendarNew(t *testing.T) {
	cal := NewTradingCalendar(domain.MarketUS)
	if cal == nil {
		t.Fatal("NewTradingCalendar returned nil")
	}
}
