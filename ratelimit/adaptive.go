package ratelimit

import (
	"context"
	"sync"
	"time"
)

// AdaptiveEngine monitors latency and error rates of backends and adjusts scale factors.
type AdaptiveEngine struct {
	mu                 sync.RWMutex
	targetLatency      time.Duration
	errorRateThreshold float64
	emaLatency         time.Duration
	requestCount       int64
	errorCount         int64
	scaleFactor        float64
	lastUpdate         time.Time
}

func NewAdaptiveEngine(targetLatencyMS int64, errorRateThreshold float64) *AdaptiveEngine {
	return &AdaptiveEngine{
		targetLatency:      time.Duration(targetLatencyMS) * time.Millisecond,
		errorRateThreshold: errorRateThreshold,
		scaleFactor:        1.0,
		lastUpdate:         time.Now(),
	}
}

func (ae *AdaptiveEngine) RecordRequest(duration time.Duration, statusCode int, err error) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	ae.requestCount++

	// Latency EMA (smoothing factor alpha = 0.1)
	if ae.emaLatency == 0 {
		ae.emaLatency = duration
	} else {
		ae.emaLatency = time.Duration(0.9*float64(ae.emaLatency) + 0.1*float64(duration))
	}

	// Count errors (HTTP 5xx or system error)
	if err != nil || (statusCode >= 500 && statusCode < 600) {
		ae.errorCount++
	}

	// Update scale factor at most once per 100ms
	now := time.Now()
	if now.Sub(ae.lastUpdate) >= 100*time.Millisecond {
		ae.adjustScaleFactor()
		ae.lastUpdate = now
		ae.requestCount = 0
		ae.errorCount = 0
	}
}

func (ae *AdaptiveEngine) adjustScaleFactor() {
	if ae.requestCount == 0 {
		return
	}

	errorRate := float64(ae.errorCount) / float64(ae.requestCount)

	// AIMD Feedback Loop
	if ae.emaLatency > ae.targetLatency || errorRate > ae.errorRateThreshold {
		// Congested: Multiplicative Decrease
		ae.scaleFactor *= 0.85
		if ae.scaleFactor < 0.1 {
			ae.scaleFactor = 0.1 // Allow at least 10% capacity
		}
	} else {
		// Healthy: Additive Increase
		ae.scaleFactor += 0.05
		if ae.scaleFactor > 1.0 {
			ae.scaleFactor = 1.0
		}
	}
}

func (ae *AdaptiveEngine) GetScaleFactor() float64 {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return ae.scaleFactor
}

// AdaptiveRateLimiter wraps a ScalableRateLimiter and applies the scale factor dynamically.
type AdaptiveRateLimiter struct {
	limiter ScalableRateLimiter
	engine  *AdaptiveEngine
}

func NewAdaptiveRateLimiter(rl ScalableRateLimiter, engine *AdaptiveEngine) *AdaptiveRateLimiter {
	return &AdaptiveRateLimiter{
		limiter: rl,
		engine:  engine,
	}
}

func (arl *AdaptiveRateLimiter) Allow(ctx context.Context, clientID string, routeID string) (Result, error) {
	// Dynamically scale limit
	factor := arl.engine.GetScaleFactor()
	arl.limiter.SetScale(factor)
	return arl.limiter.Allow(ctx, clientID, routeID)
}

func (arl *AdaptiveRateLimiter) SetScale(factor float64) {
	arl.limiter.SetScale(factor)
}

func (arl *AdaptiveRateLimiter) Unwrap() RateLimiter {
	return arl.limiter
}

func (arl *AdaptiveRateLimiter) RecordRequest(duration time.Duration, statusCode int, err error) {
	arl.engine.RecordRequest(duration, statusCode, err)
}
