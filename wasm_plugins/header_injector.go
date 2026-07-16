//go:build ignore

package main

import (
	"unsafe"
)

//go:wasmimport env set_header
func setHeader(keyPtr, keyLen, valPtr, valLen uint32)

//go:export handle_request
func handleRequest() {
	key := "X-Wasm-Custom-Header"
	val := "Hello-From-WebAssembly"

	keyBytes := []byte(key)
	valBytes := []byte(val)

	var keyPtr, valPtr uint32
	if len(keyBytes) > 0 {
		keyPtr = uint32(uintptr(unsafe.Pointer(&keyBytes[0])))
	}
	if len(valBytes) > 0 {
		valPtr = uint32(uintptr(unsafe.Pointer(&valBytes[0])))
	}

	setHeader(keyPtr, uint32(len(keyBytes)), valPtr, uint32(len(valBytes)))
}

func main() {}
