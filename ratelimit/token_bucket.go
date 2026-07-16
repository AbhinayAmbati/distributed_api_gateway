package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/client"
)

type TokenBucketState struct {
	Tokens     float64 `json:"tokens"`
	LastRefill int64   `json:"last_refill"` // Epoch ms in ClockSync timeline
	Version    int64   `json:"version"`
	LastWriter string  `json:"last_writer"`
}

type TokenBucket struct {
	mu          sync.RWMutex
	cacheClient *client.Client
	clock       *ClockSync
	baseLimit   int64
	limit       int64
	window      time.Duration
	baseBurst   int64
	burst       int64
	gatewayID   string
	failureMode string
}

func NewTokenBucket(cc *client.Client, cs *ClockSync, limit int64, window time.Duration, burst int64, gatewayID string, failureMode string) *TokenBucket {
	if burst == 0 {
		burst = limit
	}
	return &TokenBucket{
		cacheClient: cc,
		clock:       cs,
		baseLimit:   limit,
		limit:       limit,
		window:      window,
		baseBurst:   burst,
		burst:       burst,
		gatewayID:   gatewayID,
		failureMode: failureMode,
	}
}

func (tb *TokenBucket) SetScale(factor float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.limit = int64(math.Max(1, float64(tb.baseLimit)*factor))
	tb.burst = int64(math.Max(1, float64(tb.baseBurst)*factor))
}

func (tb *TokenBucket) Allow(ctx context.Context, clientID string, routeID string) (Result, error) {
	key := fmt.Sprintf("rl:tb:%s:%s", clientID, routeID)
	now := tb.clock.Now().UnixMilli()

	tb.mu.RLock()
	limit := tb.limit
	burst := tb.burst
	refillRate := float64(limit) / float64(tb.window.Milliseconds())
	tb.mu.RUnlock()
	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 1. Get current state
		var state TokenBucketState
		data, found, err := tb.cacheClient.Get(ctx, key)
		if err != nil {
			if tb.failureMode == "fail_open" {
				return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
			}
			return Result{}, fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
		}

		if !found {
			// Initialize new state
			state = TokenBucketState{
				Tokens:     float64(burst),
				LastRefill: now,
				Version:    1,
				LastWriter: tb.gatewayID,
			}
		} else {
			if err := json.Unmarshal(data, &state); err != nil {
				// Corrupt data, reset
				state = TokenBucketState{
					Tokens:     float64(burst),
					LastRefill: now,
					Version:    1,
					LastWriter: tb.gatewayID,
				}
			}
		}

		// 2. Refill tokens based on elapsed time
		elapsed := now - state.LastRefill
		if elapsed < 0 {
			elapsed = 0 // Ignore negative time jumps due to sync adjustments
		}

		refill := float64(elapsed) * refillRate
		newTokens := math.Min(float64(burst), state.Tokens+refill)

		// 3. Evaluate limit
		var allowed bool
		var remaining int64
		if newTokens >= 1.0 {
			allowed = true
			newTokens -= 1.0
			remaining = int64(math.Floor(newTokens))
		} else {
			allowed = false
			remaining = 0
		}

		// Calculate reset duration
		neededTokens := float64(burst) - newTokens
		var resetIn time.Duration
		if refillRate > 0 {
			resetIn = time.Duration(neededTokens/refillRate) * time.Millisecond
		}

		// 4. If request is not allowed, no need to write back, return early
		if !allowed {
			return Result{
				Allowed:   false,
				Remaining: remaining,
				Limit:     limit,
				ResetIn:   resetIn,
			}, nil
		}

		// 5. Update state
		state.Tokens = newTokens
		state.LastRefill = now
		state.Version++
		state.LastWriter = tb.gatewayID

		updatedData, err := json.Marshal(state)
		if err != nil {
			return Result{}, err
		}

		// Set with TTL (slightly larger than window to clean up inactive states)
		ttl := tb.window * 2
		err = tb.cacheClient.Set(ctx, key, updatedData, ttl)
		if err != nil {
			if tb.failureMode == "fail_open" {
				return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
			}
			return Result{}, fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
		}

		// 6. Verify write (optimistic concurrency control verification)
		verifyData, found, err := tb.cacheClient.Get(ctx, key)
		if err == nil && found {
			var verifyState TokenBucketState
			if err := json.Unmarshal(verifyData, &verifyState); err == nil {
				if verifyState.LastWriter == tb.gatewayID && verifyState.Version == state.Version {
					// We successfully updated the bucket!
					return Result{
						Allowed:   true,
						Remaining: remaining,
						Limit:     limit,
						ResetIn:   resetIn,
					}, nil
				}
			}
		}

		// Collision detected, add jitter and retry
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Millisecond)
	}

	// If we exhausted retries, fallback: let request pass or fail based on failure policy
	if tb.failureMode == "fail_open" {
		return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
	}
	return Result{}, ErrRateLimitExceeded
}
