package plugins

import (
	"context"
	"net/http"
	"strings"

	"github.com/AbhinayAmbati/api_gateway/gateway"
)

type AuthPlugin struct{}

// NewAuthPlugin initializes the auth plugin.
func NewAuthPlugin() *AuthPlugin {
	return &AuthPlugin{}
}

func (p *AuthPlugin) Name() string {
	return "auth"
}

func (p *AuthPlugin) Handle(ctx *gateway.Context, next http.HandlerFunc) {
	// Extract client ID from X-API-Key, Bearer Token, or IP address fallback
	clientID := ctx.Request.Header.Get("X-API-Key")
	if clientID == "" {
		authHeader := ctx.Request.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			clientID = strings.TrimPrefix(authHeader, "Bearer ")
		}
	}

	if clientID == "" {
		// Use IP address, stripping port and brackets (for IPv6)
		ip := ctx.Request.RemoteAddr
		if idx := strings.LastIndex(ip, ":"); idx != -1 {
			ip = ip[:idx]
		}
		ip = strings.Trim(ip, "[]")
		clientID = ip
	}

	// Store client ID in request context for downstream plugins to read
	importCtx := context.WithValue(ctx.Request.Context(), "client_id", clientID)
	ctx.Request = ctx.Request.WithContext(importCtx)

	next(ctx.Writer, ctx.Request)
}
