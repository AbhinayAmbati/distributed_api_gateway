package ratelimit

import (
	"context"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// CircuitBreaker wraps cache calls to prevent overloading a failing state store.
type CircuitBreaker struct {
	mu             sync.RWMutex
	state          State
	failures       int
	threshold      int
	cooldown       time.Duration
	lastStateChange time.Time
}

func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:           StateClosed,
		threshold:       threshold,
		cooldown:        cooldown,
		lastStateChange: time.Now(),
	}
}

func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if cb.state == StateOpen {
		if now.Sub(cb.lastStateChange) > cb.cooldown {
			cb.state = StateHalfOpen
			cb.lastStateChange = now
			return true
		}
		return false
	}
	return true
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	if cb.state == StateHalfOpen {
		cb.state = StateClosed
		cb.lastStateChange = time.Now()
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	if cb.failures >= cb.threshold && cb.state == StateClosed {
		cb.state = StateOpen
		cb.lastStateChange = time.Now()
	}
}

// CircuitBreakingRateLimiter wraps any RateLimiter with a CircuitBreaker for cache failures.
type CircuitBreakingRateLimiter struct {
	limiter     RateLimiter
	cb          *CircuitBreaker
	failureMode string // "fail_open" or "fail_closed"
}

func NewCircuitBreakingRateLimiter(rl RateLimiter, cbThreshold int, cbCooldown time.Duration, failureMode string) *CircuitBreakingRateLimiter {
	return &CircuitBreakingRateLimiter{
		limiter:     rl,
		cb:          NewCircuitBreaker(cbThreshold, cbCooldown),
		failureMode: failureMode,
	}
}

func (cbrl *CircuitBreakingRateLimiter) Allow(ctx context.Context, clientID string, routeID string) (Result, error) {
	if !cbrl.cb.Allow() {
		// Circuit is OPEN, execute failure policy
		if cbrl.failureMode == "fail_open" {
			return Result{Allowed: true, Remaining: 0, Limit: 0, ResetIn: 0}, nil
		}
		return Result{}, ErrCacheUnavailable
	}

	res, err := cbrl.limiter.Allow(ctx, clientID, routeID)
	if err != nil {
		cbrl.cb.RecordFailure()
		if cbrl.failureMode == "fail_open" {
			return Result{Allowed: true, Remaining: 0, Limit: 0, ResetIn: 0}, nil
		}
		return Result{}, err
	}

	cbrl.cb.RecordSuccess()
	return res, nil
}
