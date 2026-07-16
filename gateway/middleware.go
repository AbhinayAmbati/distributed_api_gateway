package gateway

import (
	"net/http"
)

// Context holds request/response state shared across the plugin pipeline.
type Context struct {
	Request   *http.Request
	Writer    http.ResponseWriter
	Variables map[string]interface{}
}

// NewContext initializes a pipeline context.
func NewContext(w http.ResponseWriter, r *http.Request) *Context {
	return &Context{
		Request:   r,
		Writer:    w,
		Variables: make(map[string]interface{}),
	}
}

// Plugin defines the interface for a Kong-style API Gateway middleware.
type Plugin interface {
	Name() string
	Handle(ctx *Context, next http.HandlerFunc)
}

// Chain represents a sequence of Plugins.
type Chain struct {
	plugins []Plugin
}

// NewChain creates a plugin pipeline chain.
func NewChain(plugins ...Plugin) *Chain {
	return &Chain{plugins: plugins}
}

// Then wraps the final handler with the middleware chain.
func (c *Chain) Then(final http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := NewContext(w, r)
		
		// Build the recursive handler chain
		var buildChain func(index int) http.HandlerFunc
		buildChain = func(index int) http.HandlerFunc {
			if index >= len(c.plugins) {
				return func(w2 http.ResponseWriter, r2 *http.Request) {
					// Update context request in case a plugin modified it
					ctx.Request = r2
					ctx.Writer = w2
					final(w2, r2)
				}
			}
			
			return func(w2 http.ResponseWriter, r2 *http.Request) {
				ctx.Request = r2
				ctx.Writer = w2
				c.plugins[index].Handle(ctx, func(w3 http.ResponseWriter, r3 *http.Request) {
					nextHandler := buildChain(index + 1)
					nextHandler(w3, r3)
				})
			}
		}
		
		// Start execution
		buildChain(0)(w, r)
	}
}
