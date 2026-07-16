package tests

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AbhinayAmbati/api_gateway/config"
	"github.com/AbhinayAmbati/api_gateway/gateway"
	"github.com/AbhinayAmbati/api_gateway/plugins"
)

func TestIntegration(t *testing.T) {
	// 1. Start Mock Backend Server
	var backendLatencyMS int64 = 0
	mockBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		latency := atomic.LoadInt64(&backendLatencyMS)
		if latency > 0 {
			time.Sleep(time.Duration(latency) * time.Millisecond)
		}
		
		// Return injected headers back for validation
		for k, v := range r.Header {
			w.Header().Set("Echo-"+k, v[0])
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Hello from Backend"))
	}))
	defer mockBackend.Close()

	// 2. Start Distributed Cache Node
	cacheCmd := exec.Command(
		"C:/distributed_cache_system/cachenode.exe",
		"-node-id", "node-1",
		"-grpc-addr", "localhost:7001",
		"-http-addr", "localhost:9001",
	)
	
	// Direct stdout/stderr to files or ignore
	cacheCmd.Stdout = os.Stdout
	cacheCmd.Stderr = os.Stderr

	t.Log("Starting cache node...")
	if err := cacheCmd.Start(); err != nil {
		t.Fatalf("Failed to start cache node: %v. Make sure cachenode.exe exists at C:/distributed_cache_system/cachenode.exe", err)
	}
	defer func() {
		t.Log("Stopping cache node...")
		_ = cacheCmd.Process.Kill()
	}()

	// Wait for cache node to start
	time.Sleep(2 * time.Second)

	// 3. Setup Gateway Configuration
	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:     "localhost:8080",
			LogLevel: "debug",
		},
		Cache: config.CacheConfig{
			Nodes: map[string]string{
				"node-1": "localhost:7001",
			},
			DialTimeout:    "1s",
			RequestTimeout: "1s",
		},
		Routes: []config.RouteConfig{
			{
				Path:       "/api/v1/users",
				Method:     "GET",
				BackendURL: mockBackend.URL,
				RateLimit: config.RateLimitConfig{
					Algorithm:   "token_bucket",
					Limit:       5,
					Window:      "2s",
					Burst:       5,
					FailureMode: "fail_open",
				},
				Adaptive: config.AdaptiveConfig{
					Enabled:            true,
					TargetLatencyMS:    50,
					ErrorRateThreshold: 0.05,
				},
			},
			{
				Path:       "/api/v1/payments",
				Method:     "POST",
				BackendURL: mockBackend.URL,
				RateLimit: config.RateLimitConfig{
					Algorithm:   "sliding_counter",
					Limit:       3,
					Window:      "2s",
					FailureMode: "fail_closed",
				},
				Adaptive: config.AdaptiveConfig{Enabled: false},
			},
			{
				Path:       "/api/v1/shadow",
				Method:     "GET",
				BackendURL: mockBackend.URL,
				RateLimit: config.RateLimitConfig{
					Algorithm:   "count_min_sketch",
					Limit:       2,
					Window:      "2s",
					FailureMode: "fail_open",
					ShadowOnly:  true,
				},
				Adaptive: config.AdaptiveConfig{Enabled: false},
			},
			{
				Path:       "/api/v1/custom",
				Method:     "GET",
				BackendURL: mockBackend.URL,
				RateLimit: config.RateLimitConfig{
					Algorithm:   "token_bucket",
					Limit:       10,
					Window:      "5s",
					FailureMode: "fail_open",
				},
				Adaptive:   config.AdaptiveConfig{Enabled: false},
				WasmPlugin: "../wasm_plugins/header_injector.wasm",
			},
		},
	}

	// Parse duration config fields
	cfg.Cache.ParsedDialTimeout = time.Second
	cfg.Cache.ParsedRequestTimeout = time.Second
	for i := range cfg.Routes {
		r := &cfg.Routes[i]
		r.RateLimit.ParsedWindow, _ = time.ParseDuration(r.RateLimit.Window)
		if r.RateLimit.Burst == 0 {
			r.RateLimit.Burst = r.RateLimit.Limit
		}
	}

	// 4. Initialize Gateway Server
	server, err := gateway.NewServer(cfg, "gw-test")
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	var activePlugins []gateway.Plugin
	activePlugins = append(activePlugins, plugins.NewLoggingPlugin())
	activePlugins = append(activePlugins, plugins.NewAuthPlugin())

	// Load Wasm plugin
	wasmPlug, err := plugins.NewWasmPlugin(context.Background(), "../wasm_plugins/header_injector.wasm")
	if err != nil {
		t.Fatalf("Failed to load wasm plugin: %v", err)
	}
	activePlugins = append(activePlugins, wasmPlug)
	defer func() {
		_ = wasmPlug.Close(context.Background())
	}()

	activePlugins = append(activePlugins, plugins.NewRateLimitPlugin(server.GetLimiters()))
	server.SetPlugins(activePlugins...)

	// Start Gateway Test Server Listener
	gwServer := httptest.NewServer(server)
	defer gwServer.Close()

	client := &http.Client{Timeout: 2 * time.Second}

	// Test Route MATCHING & PROXYING
	t.Run("RouteMatchingAndProxying", func(t *testing.T) {
		req, _ := http.NewRequest("GET", gwServer.URL+"/api/v1/users", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "Hello from Backend" {
			t.Errorf("Expected 'Hello from Backend', got '%s'", body)
		}
	})

	// Test WASM PLUGIN Header Injection
	t.Run("WasmPluginHeaderInjection", func(t *testing.T) {
		req, _ := http.NewRequest("GET", gwServer.URL+"/api/v1/custom", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected 200, got %d", resp.StatusCode)
		}
		
		// The mock backend echoes headers with "Echo-" prefix
		headerVal := resp.Header.Get("Echo-X-Wasm-Custom-Header")
		if headerVal != "Hello-From-WebAssembly" {
			t.Errorf("Expected X-Wasm-Custom-Header: Hello-From-WebAssembly, got: %s", headerVal)
		}
	})

	// Test TOKEN BUCKET RATE LIMITING
	t.Run("TokenBucketRateLimiting", func(t *testing.T) {
		// Exhaust the 5 requests limit (window is 2s)
		// We already did 1 request in the matching test. Let's do more.
		var allowedCount int
		var blockedCount int

		for i := 0; i < 7; i++ {
			req, _ := http.NewRequest("GET", gwServer.URL+"/api/v1/users", nil)
			req.Header.Set("X-API-Key", "test-token-client")
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				allowedCount++
			} else if resp.StatusCode == http.StatusTooManyRequests {
				blockedCount++
			}
		}

		// First 5 requests should pass (including burst)
		if allowedCount != 5 {
			t.Errorf("Expected 5 allowed requests, got %d", allowedCount)
		}
		if blockedCount != 2 {
			t.Errorf("Expected 2 blocked requests, got %d", blockedCount)
		}
	})

	// Test SHADOW RATE LIMITING (observability only, never blocks)
	t.Run("ShadowRateLimiting", func(t *testing.T) {
		// Route limits is 2. Let's make 4 requests
		var shadowHeaderCount int
		for i := 0; i < 4; i++ {
			req, _ := http.NewRequest("GET", gwServer.URL+"/api/v1/shadow", nil)
			req.Header.Set("X-API-Key", "test-shadow-client")
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Shadow limit should not block request. Got status: %d", resp.StatusCode)
			}

			if resp.Header.Get("X-Shadow-Rate-Limited") == "true" {
				shadowHeaderCount++
			}
		}

		if shadowHeaderCount != 2 {
			t.Errorf("Expected exactly 2 shadow limited headers, got %d", shadowHeaderCount)
		}
	})

	// Test ADAPTIVE RATE LIMITING (AIMD scale down)
	t.Run("AdaptiveRateLimiting", func(t *testing.T) {
		// Set high latency on backend: 150ms (target is 50ms)
		atomic.StoreInt64(&backendLatencyMS, 150)
		defer atomic.StoreInt64(&backendLatencyMS, 0)

		// Make 10 requests to trigger adaptation in the engine
		for i := 0; i < 10; i++ {
			req, _ := http.NewRequest("GET", gwServer.URL+"/api/v1/users", nil)
			req.Header.Set("X-API-Key", "adaptive-warmup")
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}

		// The adaptive scale factor should have decreased.
		// Let's verify by testing if the limits are lower than configured (5).
		// Since we warmed up, if we wait for 2 seconds (window resets) but make 3 requests,
		// let's check if we get throttled! (Since limit is scaled down from 5 to e.g. 2)
		time.Sleep(2500 * time.Millisecond)

		var allowedCount int
		for i := 0; i < 4; i++ {
			req, _ := http.NewRequest("GET", gwServer.URL+"/api/v1/users", nil)
			req.Header.Set("X-API-Key", "adaptive-test")
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				allowedCount++
			}
		}

		if allowedCount >= 4 {
			t.Errorf("Adaptive rate limit did not scale down! Allowed 4 requests under stress (normal limit is 5).")
		} else {
			t.Logf("Adaptive rate limiter successfully scaled limit down. Allowed: %d/4", allowedCount)
		}
	})

	// Test FAILURE MODES (Stop cache node)
	t.Run("FailureModesAndCircuitBreaker", func(t *testing.T) {
		t.Log("Stopping cache node to test failure modes...")
		_ = cacheCmd.Process.Kill()
		
		// Let TCP connection fail / wait a bit
		time.Sleep(500 * time.Millisecond)

		// 1. /api/v1/users is fail_open: should return 200 OK
		reqOpen, _ := http.NewRequest("GET", gwServer.URL+"/api/v1/users", nil)
		reqOpen.Header.Set("X-API-Key", "fail-open-test")
		respOpen, err := client.Do(reqOpen)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		respOpen.Body.Close()
		if respOpen.StatusCode != http.StatusOK {
			t.Errorf("Expected fail_open route to return 200 OK, got: %d", respOpen.StatusCode)
		}

		// 2. /api/v1/payments is fail_closed: should block with error (503 Service Unavailable)
		reqClosed, _ := http.NewRequest("POST", gwServer.URL+"/api/v1/payments", nil)
		reqClosed.Header.Set("X-API-Key", "fail-closed-test")
		respClosed, err := client.Do(reqClosed)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		respClosed.Body.Close()
		if respClosed.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("Expected fail_closed route to return 503 Service Unavailable, got: %d", respClosed.StatusCode)
		}
	})
}
