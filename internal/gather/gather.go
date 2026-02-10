package gather

import (
	"context"
	"time"
)

// Gatherer is the interface for all data gathering processes.
type Gatherer interface {
	// Name returns the gatherer identifier.
	Name() string
	// Run starts the data gathering process. It blocks until ctx is cancelled.
	Run(ctx context.Context) error
}

// DateRange represents a time range for data fetching.
type DateRange struct {
	Start time.Time
	End   time.Time
}
