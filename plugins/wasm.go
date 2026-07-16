package plugins

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/AbhinayAmbati/api_gateway/gateway"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

type WasmPlugin struct {
	mu           sync.Mutex
	wasmPath     string
	runtime      wazero.Runtime
	code         wazero.CompiledModule
	moduleConfig wazero.ModuleConfig
}

// NewWasmPlugin loads and compiles the WebAssembly module.
func NewWasmPlugin(ctx context.Context, wasmPath string) (*WasmPlugin, error) {
	if wasmPath == "" {
		return nil, fmt.Errorf("wasm path is empty")
	}

	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read wasm file: %v", err)
	}

	r := wazero.NewRuntime(ctx)
	
	// Close runtime on error
	var ok bool
	defer func() {
		if !ok {
			_ = r.Close(ctx)
		}
	}()

	// Instantiate WASI to support basic printing/imports if guest uses TinyGo/Rust standard library
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		return nil, fmt.Errorf("failed to instantiate WASI: %v", err)
	}

	// Compile the Wasm binary
	code, err := r.CompileModule(ctx, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile wasm module: %v", err)
	}

	moduleConfig := wazero.NewModuleConfig().WithStdout(os.Stdout).WithStderr(os.Stderr)
	ok = true

	return &WasmPlugin{
		wasmPath:     wasmPath,
		runtime:      r,
		code:         code,
		moduleConfig: moduleConfig,
	}, nil
}

func (wp *WasmPlugin) Name() string {
	return "wasm_" + wp.wasmPath
}

func (wp *WasmPlugin) Close(ctx context.Context) error {
	if wp.runtime != nil {
		return wp.runtime.Close(ctx)
	}
	return nil
}

func (wp *WasmPlugin) Handle(ctx *gateway.Context, next http.HandlerFunc) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	req := ctx.Request

	// Create a new wazero environment module configuration
	builder := wp.runtime.NewHostModuleBuilder("env")
	
	// Export "set_header" host function so Wasm guest can set HTTP request headers
	builder.NewFunctionBuilder().WithFunc(func(hCtx context.Context, mod api.Module, keyPtr, keyLen, valPtr, valLen uint32) {
		mem := mod.Memory()
		keyBytes, ok := mem.Read(keyPtr, keyLen)
		if !ok {
			log.Printf("[wasm] failed to read key from Wasm memory")
			return
		}
		valBytes, ok := mem.Read(valPtr, valLen)
		if !ok {
			log.Printf("[wasm] failed to read val from Wasm memory")
			return
		}
		
		key := string(keyBytes)
		val := string(valBytes)
		req.Header.Set(key, val)
	}).Export("set_header")

	// Instantiate the host functions in the "env" namespace
	envModule, err := builder.Instantiate(req.Context())
	if err != nil {
		log.Printf("[wasm] failed to instantiate env host module: %v", err)
		next(ctx.Writer, req)
		return
	}
	defer func() {
		_ = envModule.Close(req.Context())
	}()

	// Instantiate the compiled guest module
	// We instantiate a new instance per request to ensure isolation and thread-safety
	mod, err := wp.runtime.InstantiateModule(req.Context(), wp.code, wp.moduleConfig)
	if err != nil {
		log.Printf("[wasm] failed to instantiate Wasm guest module: %v", err)
		next(ctx.Writer, req)
		return
	}
	defer func() {
		_ = mod.Close(req.Context())
	}()

	// Look up the exported handle_request function
	handleReqFn := mod.ExportedFunction("handle_request")
	if handleReqFn != nil {
		_, err := handleReqFn.Call(req.Context())
		if err != nil {
			log.Printf("[wasm] error calling handle_request in Wasm: %v", err)
		}
	} else {
		log.Printf("[wasm] handle_request function not exported by Wasm module")
	}

	next(ctx.Writer, req)
}
