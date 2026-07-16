package ratelimit

import (
	"context"
)

// ShadowRateLimiter intercepts throttling decisions, converting hard-blocks
// into soft flags (setting Allowed = true, IsShadow = true).
type ShadowRateLimiter struct {
	limiter RateLimiter
}

func NewShadowRateLimiter(rl RateLimiter) *ShadowRateLimiter {
	return &ShadowRateLimiter{limiter: rl}
}

func (srl *ShadowRateLimiter) Allow(ctx context.Context, clientID string, routeID string) (Result, error) {
	res, err := srl.limiter.Allow(ctx, clientID, routeID)
	if err != nil {
		return Result{}, err
	}

	if !res.Allowed {
		res.Allowed = true
		res.IsShadow = true
	}

	return res, nil
}

func (srl *ShadowRateLimiter) SetScale(factor float64) {
	if scalable, ok := srl.limiter.(ScalableRateLimiter); ok {
		scalable.SetScale(factor)
	}
}
