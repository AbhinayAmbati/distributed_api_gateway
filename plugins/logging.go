package plugins

import (
	"log"
	"net/http"
	"time"

	"github.com/AbhinayAmbati/api_gateway/gateway"
)

type LoggingPlugin struct{}

// NewLoggingPlugin initializes the logging plugin.
func NewLoggingPlugin() *LoggingPlugin {
	return &LoggingPlugin{}
}

func (p *LoggingPlugin) Name() string {
	return "logging"
}

// loggingResponseWriter captures the HTTP status code written by downstream handlers.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	return lrw.ResponseWriter.Write(b)
}

func (p *LoggingPlugin) Handle(ctx *gateway.Context, next http.HandlerFunc) {
	start := time.Now()

	lrw := &loggingResponseWriter{
		ResponseWriter: ctx.Writer,
		statusCode:     http.StatusOK, // Default status code is 200 OK
	}

	next(lrw, ctx.Request)

	duration := time.Since(start)
	log.Printf("[gateway] %s %s -> Status: %d | Duration: %v | ClientIP: %s",
		ctx.Request.Method,
		ctx.Request.URL.Path,
		lrw.statusCode,
		duration,
		ctx.Request.RemoteAddr,
	)
}
