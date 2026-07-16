package gateway

import (
	"net/http"
	"strings"

	"github.com/AbhinayAmbati/api_gateway/config"
)

// Router handles path and method matching for API gateway routes.
type Router struct {
	routes []config.RouteConfig
}

// NewRouter initializes a Router with the configured routes.
func NewRouter(routes []config.RouteConfig) *Router {
	return &Router{routes: routes}
}

// Match checks the incoming request against all configured routes.
// It supports exact matching, prefix matching (e.g., "/api/*"), and method wildcards ("*").
func (router *Router) Match(r *http.Request) (*config.RouteConfig, bool) {
	path := r.URL.Path
	method := r.Method

	for _, rc := range router.routes {
		matchPath := false
		if strings.HasSuffix(rc.Path, "*") {
			prefix := strings.TrimSuffix(rc.Path, "*")
			matchPath = strings.HasPrefix(path, prefix)
		} else {
			matchPath = (rc.Path == path)
		}

		if matchPath {
			if rc.Method == "*" || strings.EqualFold(rc.Method, method) {
				return &rc, true
			}
		}
	}
	return nil, false
}
