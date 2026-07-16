package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbhinayAmbati/api_gateway/config"
	"github.com/AbhinayAmbati/api_gateway/gateway"
	"github.com/AbhinayAmbati/api_gateway/plugins"
)

func main() {
	configPath := flag.String("config", "./config.yaml", "Path to configuration file")
	gatewayID := flag.String("id", "gw-1", "API Gateway Instance ID")
	flag.Parse()

	// 1. Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. Initialize gateway server
	server, err := gateway.NewServer(cfg, *gatewayID)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}
	defer server.Close()

	// 3. Register plugins
	var activePlugins []gateway.Plugin
	activePlugins = append(activePlugins, plugins.NewLoggingPlugin())
	activePlugins = append(activePlugins, plugins.NewAuthPlugin())

	// Initialize Wasm plugins if configured
	for _, route := range cfg.Routes {
		if route.WasmPlugin != "" {
			wasmPlug, err := plugins.NewWasmPlugin(context.Background(), route.WasmPlugin)
			if err != nil {
				log.Fatalf("Failed to initialize Wasm plugin for route %s: %v", route.Path, err)
			}
			activePlugins = append(activePlugins, wasmPlug)
			defer func(p *plugins.WasmPlugin) {
				_ = p.Close(context.Background())
			}(wasmPlug)
		}
	}

	// Rate limiting plugin enforcer
	activePlugins = append(activePlugins, plugins.NewRateLimitPlugin(server.GetLimiters()))

	server.SetPlugins(activePlugins...)

	// 4. Start HTTP Server
	httpServer := &http.Server{
		Addr:    cfg.Server.Addr,
		Handler: server,
	}

	go func() {
		log.Printf("[gateway] starting on %s", cfg.Server.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("[gateway] shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("[gateway] stopped.")
}
