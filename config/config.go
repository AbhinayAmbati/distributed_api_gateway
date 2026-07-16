package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server ServerConfig `yaml:"server"`
	Cache  CacheConfig  `yaml:"cache"`
	Routes []RouteConfig `yaml:"routes"`
}

type ServerConfig struct {
	Addr     string `yaml:"addr"`
	LogLevel string `yaml:"log_level"`
}

type CacheConfig struct {
	Nodes          map[string]string `yaml:"nodes"`
	DialTimeout    string            `yaml:"dial_timeout"`
	RequestTimeout string            `yaml:"request_timeout"`

	ParsedDialTimeout    time.Duration `yaml:-`
	ParsedRequestTimeout time.Duration `yaml:-`
}

type RouteConfig struct {
	Path       string            `yaml:"path"`
	Method     string            `yaml:"method"`
	BackendURL string            `yaml:"backend_url"`
	RateLimit  RateLimitConfig   `yaml:"rate_limit"`
	Adaptive   AdaptiveConfig    `yaml:"adaptive"`
	WasmPlugin string            `yaml:"wasm_plugin"`
}

type RateLimitConfig struct {
	Algorithm   string `yaml:"algorithm"`    // "token_bucket", "sliding_log", "sliding_counter", "count_min_sketch"
	Limit       int64  `yaml:"limit"`        // Max requests
	Window      string `yaml:"window"`       // e.g. "1s", "1m"
	Burst       int64  `yaml:"burst"`        // Optional burst size for token bucket
	FailureMode string `yaml:"failure_mode"` // "fail_open", "fail_closed"
	ShadowOnly  bool   `yaml:"shadow_only"`  // Flag for shadow rate limiting

	ParsedWindow time.Duration `yaml:-`
}

type AdaptiveConfig struct {
	Enabled            bool    `yaml:"enabled"`
	TargetLatencyMS    int64   `yaml:"target_latency_ms"`
	ErrorRateThreshold float64 `yaml:"error_rate_threshold"`
}

// LoadConfig reads the YAML configuration file and parses it.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Post-process string durations
	if cfg.Cache.DialTimeout != "" {
		if d, err := time.ParseDuration(cfg.Cache.DialTimeout); err == nil {
			cfg.Cache.ParsedDialTimeout = d
		} else {
			cfg.Cache.ParsedDialTimeout = 5 * time.Second
		}
	} else {
		cfg.Cache.ParsedDialTimeout = 5 * time.Second
	}

	if cfg.Cache.RequestTimeout != "" {
		if d, err := time.ParseDuration(cfg.Cache.RequestTimeout); err == nil {
			cfg.Cache.ParsedRequestTimeout = d
		} else {
			cfg.Cache.ParsedRequestTimeout = 3 * time.Second
		}
	} else {
		cfg.Cache.ParsedRequestTimeout = 3 * time.Second
	}

	for i := range cfg.Routes {
		r := &cfg.Routes[i]
		if r.RateLimit.Window != "" {
			if d, err := time.ParseDuration(r.RateLimit.Window); err == nil {
				r.RateLimit.ParsedWindow = d
			} else {
				r.RateLimit.ParsedWindow = time.Second
			}
		} else {
			r.RateLimit.ParsedWindow = time.Second
		}
		// Default burst for Token Bucket if not set or 0
		if r.RateLimit.Burst == 0 {
			r.RateLimit.Burst = r.RateLimit.Limit
		}
	}

	return &cfg, nil
}
