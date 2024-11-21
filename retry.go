package rabbit

import (
	"fmt"
	"time"
)

const RetryUnlimited = -1

type RetryPolicy struct {
	DelayMS     []time.Duration
	MaxAttempts int
	RetryCount  int // Default: unlimited (-1)
}

// DefaultAckPolicy is the default backoff policy for acknowledging messages.
func DefaultAckPolicy() *RetryPolicy {
	return &RetryPolicy{
		DelayMS: []time.Duration{
			50 * time.Millisecond,
			100 * time.Millisecond,
			500 * time.Millisecond,
		},
		MaxAttempts: RetryUnlimited,
	}
}

// NewRetryPolicy returns a new backoff policy with the given delays.
func NewRetryPolicy(maxAttempts int, t ...time.Duration) *RetryPolicy {
	times := make([]time.Duration, 0)
	for _, d := range t {
		times = append(times, d)
	}
	return &RetryPolicy{
		DelayMS:     times,
		MaxAttempts: maxAttempts,
	}
}

// Duration returns the duration for the given attempt number
// If the attempt number exceeds the number of delays, the last delay is returned
func (b *RetryPolicy) Duration(n int) time.Duration {
	b.RetryCount++
	if n >= len(b.DelayMS) {
		n = len(b.DelayMS) - 1
	}

	return b.DelayMS[n]
}

// ShouldRetry returns true if the current retry count is less than the max attempts
func (b *RetryPolicy) ShouldRetry() bool {
	return b.MaxAttempts == RetryUnlimited || b.RetryCount < b.MaxAttempts
}

// Reset resets the current retry count to 0
func (b *RetryPolicy) Reset() {
	b.RetryCount = 0
}

// AttemptCount returns the current attempt count as a string, for use with log messages
func (b *RetryPolicy) AttemptCount() string {
	maxAttempts := fmt.Sprintf("%d", b.MaxAttempts)
	if b.MaxAttempts == RetryUnlimited {
		maxAttempts = "Unlimited"
	}

	return fmt.Sprintf("%d/%s", b.RetryCount+1, maxAttempts)
}
