package plugins

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/AbhinayAmbati/api_gateway/config"
	"github.com/AbhinayAmbati/api_gateway/gateway"
	"github.com/AbhinayAmbati/api_gateway/ratelimit"
)

type RateLimitPlugin struct {
	limiters map[string]ratelimit.RateLimiter
}

// NewRateLimitPlugin initializes the rate limiting plugin with the mapped limiters.
func NewRateLimitPlugin(limiters map[string]ratelimit.RateLimiter) *RateLimitPlugin {
	return &RateLimitPlugin{limiters: limiters}
}

func (p *RateLimitPlugin) Name() string {
	return "rate_limit"
}

func (p *RateLimitPlugin) Handle(ctx *gateway.Context, next http.HandlerFunc) {
	routeVal := ctx.Request.Context().Value("route")
	if routeVal == nil {
		next(ctx.Writer, ctx.Request)
		return
	}
	rc, ok := routeVal.(*config.RouteConfig)
	if !ok {
		next(ctx.Writer, ctx.Request)
		return
	}

	clientIDVal := ctx.Request.Context().Value("client_id")
	clientID, ok := clientIDVal.(string)
	if !ok {
		clientID = "anonymous"
	}

	limiter, exists := p.limiters[rc.Path]
	if !exists {
		next(ctx.Writer, ctx.Request)
		return
	}

	// Apply Rate Limiter
	res, err := limiter.Allow(ctx.Request.Context(), clientID, rc.Path)
	if err != nil {
		// Log rate limiter error and default to failure mode
		// If failure mode is fail_open, we pass the request. If fail_closed, we block.
		if rc.RateLimit.FailureMode == "fail_open" {
			next(ctx.Writer, ctx.Request)
			return
		}

		ctx.Writer.Header().Set("Content-Type", "application/json")
		ctx.Writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = ctx.Writer.Write([]byte(`{"error":"Rate limiting state store unavailable"}`))
		return
	}

	// Add Rate Limit Headers
	ctx.Writer.Header().Set("X-RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
	ctx.Writer.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))
	ctx.Writer.Header().Set("X-RateLimit-Reset", strconv.FormatInt(int64(res.ResetIn.Seconds()), 10))

	if !res.Allowed {
		// Check if it's a Shadow Rate Limit (observability only)
		if rc.RateLimit.ShadowOnly || res.IsShadow {
			ctx.Writer.Header().Set("X-Shadow-Rate-Limited", "true")
			next(ctx.Writer, ctx.Request)
			return
		}

		// Hard rate limit exceeded
		ctx.Writer.Header().Set("Retry-After", strconv.FormatInt(int64(res.ResetIn.Seconds()), 10))
		ctx.Writer.Header().Set("Content-Type", "application/json")
		ctx.Writer.WriteHeader(http.StatusTooManyRequests)
		
		respBytes, _ := json.Marshal(map[string]string{
			"error":       "Too Many Requests",
			"retry_after": res.ResetIn.String(),
		})
		_, _ = ctx.Writer.Write(respBytes)
		return
	}

	next(ctx.Writer, ctx.Request)
}
