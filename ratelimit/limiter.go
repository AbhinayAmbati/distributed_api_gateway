package ratelimit

import (
	"context"
	"errors"
	"time"
)

var (
	ErrCacheUnavailable = errors.New("distributed cache is unreachable")
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

// Result contains the outcome of a rate limiting check.
type Result struct {
	Allowed   bool          // Whether the request is allowed
	Remaining int64         // Remaining requests allowed in the current window
	Limit     int64         // The total limit configured
	ResetIn   time.Duration // Time remaining until the limit resets
	IsShadow  bool          // Whether this is a shadow limit (log/monitor only)
}

// RateLimiter is the interface that all rate limiting algorithms must implement.
type RateLimiter interface {
	Allow(ctx context.Context, clientID string, routeID string) (Result, error)
}

// ScalableRateLimiter extends RateLimiter to allow dynamically resizing the capacity limit.
type ScalableRateLimiter interface {
	RateLimiter
	SetScale(factor float64)
}
