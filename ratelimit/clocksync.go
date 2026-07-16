package ratelimit

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/AbhinayAmbati/distributed_cache_system/client"
)

// ClockSync manages latency-corrected cluster synchronization to eliminate wall-clock skew.
type ClockSync struct {
	mu           sync.RWMutex
	cacheClient  *client.Client
	refNodeID    string
	bootTime     time.Time
	synced       bool
	syncInterval time.Duration
	stopChan     chan struct{}
}

// NewClockSync creates a new clock synchronizer referencing a specific cache node.
func NewClockSync(cacheClient *client.Client, refNodeID string, syncInterval time.Duration) *ClockSync {
	return &ClockSync{
		cacheClient:  cacheClient,
		refNodeID:    refNodeID,
		syncInterval: syncInterval,
		stopChan:     make(chan struct{}),
	}
}

// Start runs the periodic background synchronization.
func (cs *ClockSync) Start(ctx context.Context) {
	if err := cs.sync(ctx); err != nil {
		log.Printf("[clocksync] initial sync with node %s failed: %v. Falling back to local clock.", cs.refNodeID, err)
	} else {
		cs.mu.RLock()
		bt := cs.bootTime
		cs.mu.RUnlock()
		log.Printf("[clocksync] initial sync successful with node %s. Est Boot Time: %v", cs.refNodeID, bt)
	}

	ticker := time.NewTicker(cs.syncInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := cs.sync(syncCtx); err != nil {
					log.Printf("[clocksync] periodic sync failed: %v", err)
				}
				cancel()
			case <-cs.stopChan:
				ticker.Stop()
				return
			}
		}
	}()
}

// Stop terminates the synchronizer.
func (cs *ClockSync) Stop() {
	close(cs.stopChan)
}

func (cs *ClockSync) sync(ctx context.Context) error {
	t1 := time.Now()
	resp, err := cs.cacheClient.Ping(ctx, cs.refNodeID)
	if err != nil {
		return err
	}
	t2 := time.Now()
	rtt := t2.Sub(t1)

	// Uptime returned by the node in milliseconds.
	nodeUptime := time.Duration(resp.UptimeMs) * time.Millisecond
	// Estimate uptime at the exact instant t2 (when response is parsed locally)
	nodeUptimeAtT2 := nodeUptime + rtt/2

	cs.mu.Lock()
	// bootTime is t2 minus the node's uptime, representing the node's boot wall-clock time
	cs.bootTime = t2.Add(-nodeUptimeAtT2)
	cs.synced = true
	cs.mu.Unlock()

	return nil
}

// Now returns the cluster-synchronized time. If synchronization is not active or fails,
// it falls back to the local wall-clock time.
func (cs *ClockSync) Now() time.Time {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if !cs.synced {
		return time.Now()
	}

	// The synchronized time is the epoch plus the reference node's current uptime.
	// Current uptime = time.Since(bootTime)
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	uptime := time.Since(cs.bootTime)
	return epoch.Add(uptime)
}
