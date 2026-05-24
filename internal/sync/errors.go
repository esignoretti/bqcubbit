package sync

import (
	"context"
	"strings"
	"time"
)

type ErrorClass int

const (
	ErrorUnknown ErrorClass = iota
	ErrorBQQuota
	ErrorBQAuth
	ErrorCubbitUnavailable
	ErrorTransient
	ErrorPermanent
)

func ClassifyError(err error) ErrorClass {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "quota"):
		return ErrorBQQuota
	case strings.Contains(msg, "unauthenticated"), strings.Contains(msg, "permission"):
		return ErrorBQAuth
	case strings.Contains(msg, "connection refused"), strings.Contains(msg, "no such host"):
		return ErrorCubbitUnavailable
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline"):
		return ErrorTransient
	default:
		return ErrorUnknown
	}
}

type CircuitBreaker struct {
	failures    int
	maxFailures int
	backoff     time.Duration
	maxBackoff  time.Duration
}

func NewCircuitBreaker(maxFailures int, maxBackoff time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures: maxFailures,
		maxBackoff:  maxBackoff,
		backoff:     time.Second,
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.failures++
	cb.backoff *= 2
	if cb.backoff > cb.maxBackoff {
		cb.backoff = cb.maxBackoff
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.failures = 0
	cb.backoff = time.Second
}

func (cb *CircuitBreaker) IsOpen() bool {
	return cb.failures >= cb.maxFailures
}

func (cb *CircuitBreaker) Wait(ctx context.Context) error {
	if !cb.IsOpen() {
		return nil
	}
	select {
	case <-time.After(cb.backoff):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
