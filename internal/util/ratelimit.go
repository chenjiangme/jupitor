package util

import (
	"context"
	"sync"
	"time"
)

// RateLimiter implements a token-bucket rate limiter that replenishes tokens
// at a fixed rate.
type RateLimiter struct {
	rate     float64 // tokens per second
	tokens   float64
	lastTime time.Time
	mu       sync.Mutex
}

// NewRateLimiter creates a RateLimiter that allows perMinute operations per
// minute.
func NewRateLimiter(perMinute int) *RateLimiter {
	return &RateLimiter{
		rate:     float64(perMinute) / 60.0,
		tokens:   1, // start with one token available
		lastTime: time.Now(),
	}
}

// Wait blocks until a rate-limit token is available or the context is
// cancelled.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	for {
		rl.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(rl.lastTime).Seconds()
		rl.tokens += elapsed * rl.rate
		if rl.tokens > 1 {
			rl.tokens = 1
		}
		rl.lastTime = now

		if rl.tokens >= 1 {
			rl.tokens -= 1
			rl.mu.Unlock()
			return nil
		}
		rl.mu.Unlock()

		// Wait a short interval before checking again.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
