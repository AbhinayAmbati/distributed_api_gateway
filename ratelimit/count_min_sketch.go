package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/client"
)

const (
	CMSDepth = 4
	CMSWidth = 2048
)

type CountMinSketchState struct {
	Grid       [][]uint32 `json:"grid"`
	WindowID   int64      `json:"window_id"`
	Version    int64      `json:"version"`
	LastWriter string     `json:"last_writer"`
}

type CountMinSketch struct {
	mu          sync.RWMutex
	cacheClient *client.Client
	clock       *ClockSync
	baseLimit   int64
	limit       int64
	window      time.Duration
	gatewayID   string
	failureMode string
}

func NewCountMinSketch(cc *client.Client, cs *ClockSync, limit int64, window time.Duration, gatewayID string, failureMode string) *CountMinSketch {
	return &CountMinSketch{
		cacheClient: cc,
		clock:       cs,
		baseLimit:   limit,
		limit:       limit,
		window:      window,
		gatewayID:   gatewayID,
		failureMode: failureMode,
	}
}

func (cms *CountMinSketch) SetScale(factor float64) {
	cms.mu.Lock()
	defer cms.mu.Unlock()
	cms.limit = int64(math.Max(1, float64(cms.baseLimit)*factor))
}

// getHashIndices returns the column index for each of the Depth rows.
func (cms *CountMinSketch) getHashIndices(key string) []int {
	indices := make([]int, CMSDepth)
	for i := 0; i < CMSDepth; i++ {
		h := fnv.New32a()
		_, _ = h.Write([]byte(key + ":" + strconv.Itoa(i)))
		indices[i] = int(h.Sum32() % CMSWidth)
	}
	return indices
}

func (cms *CountMinSketch) Allow(ctx context.Context, clientID string, routeID string) (Result, error) {
	now := cms.clock.Now().UnixMilli()
	windowMS := cms.window.Milliseconds()
	currentWindowID := now / windowMS

	cms.mu.RLock()
	limit := cms.limit
	cms.mu.RUnlock()

	// The CMS is global for the route, tracking all clients in a single probabilistic grid
	key := fmt.Sprintf("rl:cms:%s", routeID)
	indices := cms.getHashIndices(clientID)

	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		var state CountMinSketchState
		data, found, err := cms.cacheClient.Get(ctx, key)
		if err != nil {
			if cms.failureMode == "fail_open" {
				return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
			}
			return Result{}, fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
		}

		// Initialize grid if not found or if window changed
		if !found {
			state = cms.newGrid(currentWindowID)
		} else {
			if err := json.Unmarshal(data, &state); err != nil || state.WindowID != currentWindowID {
				// Reset grid for a new window
				state = cms.newGrid(currentWindowID)
			}
		}

		// 1. Estimate current request frequency for this client
		var est uint32 = math.MaxUint32
		for row := 0; row < CMSDepth; row++ {
			col := indices[row]
			if state.Grid[row][col] < est {
				est = state.Grid[row][col]
			}
		}

		// 2. Evaluate rate limit
		allowed := int64(est) < limit
		var remaining int64
		if allowed {
			// Increment counters at the hash locations
			for row := 0; row < CMSDepth; row++ {
				col := indices[row]
				state.Grid[row][col]++
			}
			remaining = limit - int64(est) - 1
			if remaining < 0 {
				remaining = 0
			}
		} else {
			remaining = 0
		}

		// Reset duration is time until the end of the current window
		currentWindowStart := currentWindowID * windowMS
		resetIn := time.Duration(currentWindowStart+windowMS-now) * time.Millisecond

		if !allowed {
			return Result{
				Allowed:   false,
				Remaining: remaining,
				Limit:     limit,
				ResetIn:   resetIn,
			}, nil
		}

		// 3. Update state in the cache
		state.Version++
		state.LastWriter = cms.gatewayID

		updatedData, err := json.Marshal(state)
		if err != nil {
			return Result{}, err
		}

		// Store in cache with TTL = 2 * window size
		err = cms.cacheClient.Set(ctx, key, updatedData, cms.window*2)
		if err != nil {
			if cms.failureMode == "fail_open" {
				return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
			}
			return Result{}, fmt.Errorf("%w: %v", ErrCacheUnavailable, err)
		}

		// 4. Verify write (optimistic concurrency checking)
		verifyData, found, err := cms.cacheClient.Get(ctx, key)
		if err == nil && found {
			var verifyState CountMinSketchState
			if err := json.Unmarshal(verifyData, &verifyState); err == nil {
				if verifyState.LastWriter == cms.gatewayID && verifyState.Version == state.Version {
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

	if cms.failureMode == "fail_open" {
		return Result{Allowed: true, Remaining: 0, Limit: limit, ResetIn: 0}, nil
	}
	return Result{}, ErrRateLimitExceeded
}

func (cms *CountMinSketch) newGrid(windowID int64) CountMinSketchState {
	grid := make([][]uint32, CMSDepth)
	for i := 0; i < CMSDepth; i++ {
		grid[i] = make([]uint32, CMSWidth)
	}
	return CountMinSketchState{
		Grid:       grid,
		WindowID:   windowID,
		Version:    1,
		LastWriter: cms.gatewayID,
	}
}
