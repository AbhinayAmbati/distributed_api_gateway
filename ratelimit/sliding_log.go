package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/client"
)

type SlidingLogState struct {
	Timestamps []int64 `json:"timestamps"`
	Version    int64   `json:"version"`
	LastWriter string  `json:"last_writer"`
}

type SlidingLog struct {
	cacheClient *client.Client
	clock       *ClockSync
	limit       int64
	window      time.Duration
	gatewayID   string
	failureMode string
}

func NewSlidingLog(cc *client.Client, cs *ClockSync, limit int64, window time.Duration, gatewayID string, failureMode string) *SlidingLog {
	return &SlidingLog{
		cacheClient: cc,
		clock:       cs,
		limit:       limit,
		window:      window,
		gatewayID:   gatewayID,
		failureMode: failureMode,
	}
}

func (sl *SlidingLog) Allow(ctx context.Context, clientID string, routeID string) (Result, error) {
	key := fmt.Sprintf("rl:sl:%s:%s", clientID, routeID)
	now := sl.clock.Now().UnixMilli()
	windowMS := sl.window.Milliseconds()
	threshold := now - windowMS

	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 1. Fetch log state
		var state SlidingLogState
		data, found, err := sl.cacheClient.Get(ctx, key)
		if err != nil {
			if sl.failureMode == "fail_open" {
				return Result{Allowed: true, Remaining: 0, Limit: sl.limit, ResetIn: 0}, nil
			}
			return Result{}, fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
		}

		if !found {
			state = SlidingLogState{
				Timestamps: []int64{},
				Version:    1,
				LastWriter: sl.gatewayID,
			}
		} else {
			if err := json.Unmarshal(data, &state); err != nil {
				state = SlidingLogState{
					Timestamps: []int64{},
					Version:    1,
					LastWriter: sl.gatewayID,
				}
			}
		}

		// 2. Filter old timestamps
		var filtered []int64
		for _, ts := range state.Timestamps {
			if ts > threshold {
				filtered = append(filtered, ts)
			}
		}

		// 3. Determine if request is allowed
		allowed := int64(len(filtered)) < sl.limit
		var remaining int64
		var resetIn time.Duration

		if allowed {
			filtered = append(filtered, now)
			remaining = sl.limit - int64(len(filtered))
			if remaining < 0 {
				remaining = 0
			}
		} else {
			remaining = 0
		}

		// Calculate reset duration
		if len(filtered) > 0 {
			oldest := filtered[0]
			resetIn = time.Duration(oldest+windowMS-now) * time.Millisecond
		} else {
			resetIn = sl.window
		}

		if !allowed {
			return Result{
				Allowed:   false,
				Remaining: remaining,
				Limit:     sl.limit,
				ResetIn:   resetIn,
			}, nil
		}

		// 4. Update state
		state.Timestamps = filtered
		state.Version++
		state.LastWriter = sl.gatewayID

		updatedData, err := json.Marshal(state)
		if err != nil {
			return Result{}, err
		}

		// Set with TTL equal to window
		err = sl.cacheClient.Set(ctx, key, updatedData, sl.window)
		if err != nil {
			if sl.failureMode == "fail_open" {
				return Result{Allowed: true, Remaining: 0, Limit: sl.limit, ResetIn: 0}, nil
			}
			return Result{}, fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
		}

		// 5. Verify write (optimistic locking verification)
		verifyData, found, err := sl.cacheClient.Get(ctx, key)
		if err == nil && found {
			var verifyState SlidingLogState
			if err := json.Unmarshal(verifyData, &verifyState); err == nil {
				if verifyState.LastWriter == sl.gatewayID && verifyState.Version == state.Version {
					return Result{
						Allowed:   true,
						Remaining: remaining,
						Limit:     sl.limit,
						ResetIn:   resetIn,
					}, nil
				}
			}
		}

		// Collision, sleep and retry
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Millisecond)
	}

	if sl.failureMode == "fail_open" {
		return Result{Allowed: true, Remaining: 0, Limit: sl.limit, ResetIn: 0}, nil
	}
	return Result{}, ErrRateLimitExceeded
}
