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

type SlidingCounterState struct {
	PrevWindowID int64  `json:"prev_window_id"`
	PrevCount    int64  `json:"prev_count"`
	CurrWindowID int64  `json:"curr_window_id"`
	CurrCount    int64  `json:"curr_count"`
	Version      int64  `json:"version"`
	LastWriter   string `json:"last_writer"`
}

type SlidingCounter struct {
	mu          sync.RWMutex
	cacheClient *client.Client
	clock       *ClockSync
	baseLimit   int64
	limit       int64
	window      time.Duration
	gatewayID   string
	failureMode string
}

func NewSlidingCounter(cc *client.Client, cs *ClockSync, limit int64, window time.Duration, gatewayID string, failureMode string) *SlidingCounter {
	return &SlidingCounter{
		cacheClient: cc,
		clock:       cs,
		baseLimit:   limit,
		limit:       limit,
		window:      window,
		gatewayID:   gatewayID,
		failureMode: failureMode,
	}
}

func (sc *SlidingCounter) SetScale(factor float64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.limit = int64(math.Max(1, float64(sc.baseLimit)*factor))
}

func (sc *SlidingCounter) Allow(ctx context.Context, clientID string, routeID string) (Result, error) {
	key := fmt.Sprintf("rl:sc:%s:%s", clientID, routeID)
	now := sc.clock.Now().UnixMilli()
	windowMS := sc.window.Milliseconds()
	currentWindowID := now / windowMS

	sc.mu.RLock()
	limit := sc.limit
	sc.mu.RUnlock()

	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 1. Fetch counter state
		var state SlidingCounterState
		data, found, err := sc.cacheClient.Get(ctx, key)
		if err != nil {
			if sc.failureMode == "fail_open" {
				return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
			}
			return Result{}, fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
		}

		if !found {
			state = SlidingCounterState{
				PrevWindowID: currentWindowID - 1,
				PrevCount:    0,
				CurrWindowID: currentWindowID,
				CurrCount:    0,
				Version:      1,
				LastWriter:   sc.gatewayID,
			}
		} else {
			if err := json.Unmarshal(data, &state); err != nil {
				state = SlidingCounterState{
					PrevWindowID: currentWindowID - 1,
					PrevCount:    0,
					CurrWindowID: currentWindowID,
					CurrCount:    0,
					Version:      1,
					LastWriter:   sc.gatewayID,
				}
			} else {
				// Adjust state based on elapsed windows
				if currentWindowID == state.CurrWindowID {
					// We are in the same window, no change
				} else if currentWindowID == state.CurrWindowID+1 {
					// Roll forward by one window
					state.PrevWindowID = state.CurrWindowID
					state.PrevCount = state.CurrCount
					state.CurrWindowID = currentWindowID
					state.CurrCount = 0
				} else {
					// Gap is larger than 1 window, reset both
					state.PrevWindowID = currentWindowID - 1
					state.PrevCount = 0
					state.CurrWindowID = currentWindowID
					state.CurrCount = 0
				}
			}
		}

		// 2. Calculate weight and estimated count
		currentWindowStart := currentWindowID * windowMS
		elapsedInCurrentWindow := now - currentWindowStart
		weight := 1.0 - (float64(elapsedInCurrentWindow) / float64(windowMS))

		var prevCountContribution float64
		if state.PrevWindowID == currentWindowID-1 {
			prevCountContribution = float64(state.PrevCount) * weight
		}

		estimatedCount := prevCountContribution + float64(state.CurrCount)

		// 3. Evaluate limit
		allowed := int64(math.Floor(estimatedCount)) < limit
		var remaining int64
		if allowed {
			state.CurrCount++
			remaining = limit - int64(math.Floor(estimatedCount)) - 1
			if remaining < 0 {
				remaining = 0
			}
		} else {
			remaining = 0
		}

		// Calculate reset duration (time until current window ends)
		resetIn := time.Duration(currentWindowStart+windowMS-now) * time.Millisecond

		if !allowed {
			return Result{
				Allowed:   false,
				Remaining: remaining,
				Limit:     limit,
				ResetIn:   resetIn,
			}, nil
		}

		// 4. Update state
		state.Version++
		state.LastWriter = sc.gatewayID

		updatedData, err := json.Marshal(state)
		if err != nil {
			return Result{}, err
		}

		// Set with TTL = 2 * window size
		err = sc.cacheClient.Set(ctx, key, updatedData, sc.window*2)
		if err != nil {
			if sc.failureMode == "fail_open" {
				return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
			}
			return Result{}, fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
		}

		// 5. Verify write (optimistic concurrency verification)
		verifyData, found, err := sc.cacheClient.Get(ctx, key)
		if err == nil && found {
			var verifyState SlidingCounterState
			if err := json.Unmarshal(verifyData, &verifyState); err == nil {
				if verifyState.LastWriter == sc.gatewayID && verifyState.Version == state.Version {
					return Result{
						Allowed:   true,
						Remaining: remaining,
						Limit:     limit,
						ResetIn:   resetIn,
					}, nil
				}
			}
		}

		// Collision, retry with jitter
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Millisecond)
	}

	if sc.failureMode == "fail_open" {
		return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
	}
	return Result{}, ErrRateLimitExceeded
}
