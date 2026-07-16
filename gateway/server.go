package gateway

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"

	"github.com/AbhinayAmbati/api_gateway/config"
	"github.com/AbhinayAmbati/api_gateway/ratelimit"
	"github.com/AbhinayAmbati/distributed_cache_system/client"
)

type Server struct {
	cfg         *config.Config
	cacheClient *client.Client
	clockSync   *ratelimit.ClockSync
	router      *Router
	limiters    map[string]ratelimit.RateLimiter
	proxies     map[string]*httputil.ReverseProxy
	proxiesMu   sync.RWMutex
	gatewayID   string
	chain       *Chain
}

// NewServer initializes the Gateway Server.
func NewServer(cfg *config.Config, gatewayID string) (*Server, error) {
	// 1. Initialize Cache Client
	cc, err := client.NewClient(cfg.Cache.Nodes,
		client.WithDialTimeout(cfg.Cache.ParsedDialTimeout),
		client.WithRequestTimeout(cfg.Cache.ParsedRequestTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cache client: %v", err)
	}

	// 2. Initialize ClockSync (using first node ID as reference)
	var refNodeID string
	for nodeID := range cfg.Cache.Nodes {
		refNodeID = nodeID
		break
	}
	cs := ratelimit.NewClockSync(cc, refNodeID, 30*time.Second)
	cs.Start(context.Background())

	// 3. Initialize Router
	router := NewRouter(cfg.Routes)

	// 4. Initialize Limiters
	limiters := make(map[string]ratelimit.RateLimiter)
	for _, rc := range cfg.Routes {
		var rl ratelimit.RateLimiter
		switch rc.RateLimit.Algorithm {
		case "token_bucket":
			rl = ratelimit.NewTokenBucket(cc, cs, rc.RateLimit.Limit, rc.RateLimit.ParsedWindow, rc.RateLimit.Burst, gatewayID, rc.RateLimit.FailureMode)
		case "sliding_log":
			rl = ratelimit.NewSlidingLog(cc, cs, rc.RateLimit.Limit, rc.RateLimit.ParsedWindow, gatewayID, rc.RateLimit.FailureMode)
		case "sliding_counter":
			rl = ratelimit.NewSlidingCounter(cc, cs, rc.RateLimit.Limit, rc.RateLimit.ParsedWindow, gatewayID, rc.RateLimit.FailureMode)
		case "count_min_sketch":
			rl = ratelimit.NewCountMinSketch(cc, cs, rc.RateLimit.Limit, rc.RateLimit.ParsedWindow, gatewayID, rc.RateLimit.FailureMode)
		default:
			rl = ratelimit.NewTokenBucket(cc, cs, rc.RateLimit.Limit, rc.RateLimit.ParsedWindow, rc.RateLimit.Burst, gatewayID, rc.RateLimit.FailureMode)
		}
		limiters[rc.Path] = rl
	}

	s := &Server{
		cfg:         cfg,
		cacheClient: cc,
		clockSync:   cs,
		router:      router,
		limiters:    limiters,
		proxies:     make(map[string]*httputil.ReverseProxy),
		gatewayID:   gatewayID,
	}

	return s, nil
}

// SetPlugins registers the middleware plugin execution chain.
func (s *Server) SetPlugins(plugins ...Plugin) {
	s.chain = NewChain(plugins...)
}

// Close releases server resources.
func (s *Server) Close() {
	if s.clockSync != nil {
		s.clockSync.Stop()
	}
	if s.cacheClient != nil {
		_ = s.cacheClient.Close()
	}
}

// GetProxy retrieves or initializes a ReverseProxy for a route.
func (s *Server) GetProxy(rc *config.RouteConfig) (*httputil.ReverseProxy, error) {
	s.proxiesMu.RLock()
	p, exists := s.proxies[rc.Path]
	s.proxiesMu.RUnlock()

	if exists {
		return p, nil
	}

	s.proxiesMu.Lock()
	defer s.proxiesMu.Unlock()

	// Double check
	if p, exists := s.proxies[rc.Path]; exists {
		return p, nil
	}

	// Performance Optimization: Sync.Pool byte buffer size set to 32KB
	proxy, err := NewProxy(rc.BackendURL, 32*1024, func(duration time.Duration, statusCode int, err error) {
		// Callback for monitoring backend latency and error rates (Phase 4: Adaptive Rate Limiting)
		s.recordBackendMetrics(rc.Path, duration, statusCode, err)
	})
	if err != nil {
		return nil, err
	}

	s.proxies[rc.Path] = proxy
	return proxy, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Route Matching
	rc, matched := s.router.Match(r)
	if !matched {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"No route matches the request"}`))
		return
	}

	// 2. Resolve proxy
	proxy, err := s.GetProxy(rc)
	if err != nil {
		log.Printf("[gateway] proxy error: %v", err)
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	// 3. Define the final forwarding handler
	finalHandler := func(w2 http.ResponseWriter, r2 *http.Request) {
		proxy.ServeHTTP(w2, r2)
	}

	// 4. Wrap with middleware plugin chain if registered
	if s.chain != nil {
		reqCtx := context.WithValue(r.Context(), "route", rc)
		r = r.WithContext(reqCtx)
		s.chain.Then(finalHandler)(w, r)
	} else {
		finalHandler(w, r)
	}
}

// recordBackendMetrics receives callbacks from the proxy transport.
func (s *Server) recordBackendMetrics(routePath string, duration time.Duration, statusCode int, err error) {
	// Will be implemented in Phase 4: Adaptive Rate Limiting
}
