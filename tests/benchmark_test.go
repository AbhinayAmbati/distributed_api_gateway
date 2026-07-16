package tests

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"testing"
	"time"

	"github.com/AbhinayAmbati/api_gateway/config"
	"github.com/AbhinayAmbati/api_gateway/gateway"
	"github.com/AbhinayAmbati/api_gateway/plugins"
)

func BenchmarkGatewayThroughput(b *testing.B) {
	// 1. Start Mock Backend Server
	mockBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Hello from Backend"))
	}))
	defer mockBackend.Close()

	// 2. Start Distributed Cache Node
	cacheCmd := exec.Command(
		"C:/distributed_cache_system/cachenode.exe",
		"-node-id", "node-1",
		"-grpc-addr", "localhost:7002",
		"-http-addr", "localhost:9002",
	)
	cacheCmd.Stdout = nil
	cacheCmd.Stderr = nil

	if err := cacheCmd.Start(); err != nil {
		b.Fatalf("Failed to start cache node: %v", err)
	}
	defer func() {
		_ = cacheCmd.Process.Kill()
	}()

	time.Sleep(1 * time.Second)

	// 3. Setup Gateway Configuration (large limit to avoid early rate limiting in benchmarks)
	cfg := &config.Config{
		Server: config.ServerConfig{
			Addr:     "localhost:8081",
			LogLevel: "info",
		},
		Cache: config.CacheConfig{
			Nodes: map[string]string{
				"node-1": "localhost:7002",
			},
			DialTimeout:    "1s",
			RequestTimeout: "1s",
		},
		Routes: []config.RouteConfig{
			{
				Path:       "/api/v1/bench",
				Method:     "GET",
				BackendURL: mockBackend.URL,
				RateLimit: config.RateLimitConfig{
					Algorithm:   "token_bucket",
					Limit:       1000000,
					Window:      "1h",
					Burst:       1000000,
					FailureMode: "fail_open",
				},
				Adaptive: config.AdaptiveConfig{Enabled: false},
			},
		},
	}

	cfg.Cache.ParsedDialTimeout = time.Second
	cfg.Cache.ParsedRequestTimeout = time.Second
	for i := range cfg.Routes {
		r := &cfg.Routes[i]
		r.RateLimit.ParsedWindow, _ = time.ParseDuration(r.RateLimit.Window)
		r.RateLimit.Burst = r.RateLimit.Limit
	}

	// 4. Initialize Gateway Server
	server, err := gateway.NewServer(cfg, "gw-bench")
	if err != nil {
		b.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	var activePlugins []gateway.Plugin
	activePlugins = append(activePlugins, plugins.NewAuthPlugin())
	activePlugins = append(activePlugins, plugins.NewRateLimitPlugin(server.GetLimiters()))
	server.SetPlugins(activePlugins...)

	// Start Gateway Test Server Listener
	gwServer := httptest.NewServer(server)
	defer gwServer.Close()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 2 * time.Second,
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		req, _ := http.NewRequest("GET", gwServer.URL+"/api/v1/bench", nil)
		req.Header.Set("X-API-Key", "bench-client")
		
		for pb.Next() {
			resp, err := client.Do(req)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}
