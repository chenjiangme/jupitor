package util

import (
	"context"
	"time"
)

// Retry calls fn up to maxAttempts times with exponential backoff starting at
// baseDelay. It returns nil on the first successful call, or the last error
// if all attempts fail. The function respects context cancellation between
// retries.
func Retry(ctx context.Context, maxAttempts int, baseDelay time.Duration, fn func() error) error {
	var err error
	delay := baseDelay

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}

		// Don't sleep after the last failed attempt.
		if attempt < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}
	}

	return err
}
