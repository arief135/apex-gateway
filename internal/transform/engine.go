// Package transform provides a sandboxed JavaScript execution engine for
// dynamic payload mapping. Scripts are compiled once via goja and the
// compiled program object is cached in memory. Cache entries are keyed by
// transformerID+updatedAt, so updating a transformer via the admin API
// automatically invalidates the old entry — zero restarts required.
package transform

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"github.com/dop251/goja"
	"github.com/rs/zerolog/log"
)

const (
	// scriptTimeout is the maximum wall-clock time a single script execution
	// may consume. Prevents infinite loops from starving the request pool.
	scriptTimeout = 50 * time.Millisecond

	// maxPayloadBytes is the largest body the engine will attempt to transform.
	// Larger payloads are passed through untouched.
	maxPayloadBytes = 2 * 1024 * 1024 // 2MB
)

// ── Compiled Script Cache ──────────────────────────────────────────────────

// cacheKey uniquely identifies a compiled version of a script.
// Using UpdatedAt as part of the key means any script edit produces a new key,
// letting the old compiled program be GC'd naturally.
type cacheKey struct {
	id        string
	updatedAt time.Time
}

type compiledEntry struct {
	program *goja.Program
}

var (
	cache   sync.Map // map[cacheKey]*compiledEntry
	metrics sync.Map // map[string]*ScriptMetrics  (keyed by transformer ID)
)

// ScriptMetrics tracks runtime statistics for a transformer script.
type ScriptMetrics struct {
	mu          sync.Mutex
	Executions  int64
	Errors      int64
	TotalMs     int64
	LastErrorAt time.Time
	LastError   string
}

func (m *ScriptMetrics) record(elapsed time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Executions++
	m.TotalMs += elapsed.Milliseconds()
	if err != nil {
		m.Errors++
		m.LastError = err.Error()
		m.LastErrorAt = time.Now()
	}
}

// GetMetrics returns the ScriptMetrics for the given transformer ID, or nil.
func GetMetrics(transformerID string) *ScriptMetrics {
	v, ok := metrics.Load(transformerID)
	if !ok {
		return nil
	}
	return v.(*ScriptMetrics)
}

// compile compiles a JavaScript source string into a reusable goja.Program.
// The compiled program is safe to run concurrently across multiple goroutines
// (each goroutine creates its own runtime from the shared program).
func compile(source string) (*goja.Program, error) {
	// Wrap the user script so we can call transform() by name
	wrapped := source + "\n;(function(){ return transform; })()"

	prog, err := goja.Compile("transformer", wrapped, true)
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}
	return prog, nil
}

// getCompiled returns (or compiles and caches) the program for t.
func getCompiled(t *models.Transformer) (*goja.Program, error) {
	key := cacheKey{id: t.ID, updatedAt: t.UpdatedAt}
	if v, ok := cache.Load(key); ok {
		return v.(*compiledEntry).program, nil
	}

	prog, err := compile(t.Script)
	if err != nil {
		return nil, err
	}
	cache.Store(key, &compiledEntry{program: prog})
	log.Debug().Str("transformer", t.Name).Str("id", t.ID).Msg("script compiled and cached")
	return prog, nil
}

// InvalidateCache evicts the cached compilation for a specific transformer ID.
// Called by the admin API after a script update.
func InvalidateCache(transformerID string) {
	cache.Range(func(k, _ any) bool {
		if k.(cacheKey).id == transformerID {
			cache.Delete(k)
		}
		return true
	})
}

// ── Engine ─────────────────────────────────────────────────────────────────

// Engine executes transformer scripts against JSON payloads.
// It is stateless and safe for concurrent use.
type Engine struct{}

// New returns a ready Engine.
func NewEngine() *Engine { return &Engine{} }

// Apply runs a single transformer script against the given JSON payload.
//
//   - payload: raw JSON bytes (object, array, or any JSON value)
//   - tctx:    request metadata exposed to the script as `ctx`
//
// Returns the transformed JSON bytes. On any script error the original
// payload is returned unchanged and the error is logged (fail-open policy).
func (e *Engine) Apply(t *models.Transformer, payload []byte, tctx *models.TransformContext) ([]byte, error) {
	if len(payload) == 0 || len(payload) > maxPayloadBytes {
		return payload, nil
	}

	start := time.Now()
	out, err := e.runScript(t, payload, tctx)
	elapsed := time.Since(start)

	// Update metrics
	m, _ := metrics.LoadOrStore(t.ID, &ScriptMetrics{})
	m.(*ScriptMetrics).record(elapsed, err)

	if err != nil {
		log.Warn().
			Err(err).
			Str("transformer", t.Name).
			Str("direction", string(t.Direction)).
			Int64("elapsed_ms", elapsed.Milliseconds()).
			Msg("transform script error — passing through original payload")
		return payload, err
	}

	log.Trace().
		Str("transformer", t.Name).
		Int64("elapsed_ms", elapsed.Milliseconds()).
		Int("in_bytes", len(payload)).
		Int("out_bytes", len(out)).
		Msg("transform applied")

	return out, nil
}

// ApplyChain runs a slice of transformers in order against the same payload,
// piping the output of each into the input of the next.
// Individual script errors are fail-open: the payload continues unchanged.
func (e *Engine) ApplyChain(transformers []*models.Transformer, payload []byte, tctx *models.TransformContext) []byte {
	current := payload
	for _, t := range transformers {
		if !t.Enabled {
			continue
		}
		result, err := e.Apply(t, current, tctx)
		if err == nil {
			current = result
		}
		// on error: current stays unchanged, next transformer gets unmodified data
	}
	return current
}

// Test compiles and executes a script without persisting anything.
// Used by the admin "dry-run" endpoint.
func (e *Engine) Test(script string, payload []byte, tctx *models.TransformContext) ([]byte, time.Duration, error) {
	t := &models.Transformer{
		ID:        "_test_",
		Script:    script,
		UpdatedAt: time.Now(),
		Enabled:   true,
	}
	start := time.Now()
	out, err := e.runScript(t, payload, tctx)
	return out, time.Since(start), err
}

// ── Core execution ─────────────────────────────────────────────────────────

// runScript creates a fresh goja runtime, loads the compiled program, parses
// the payload into a JS value, injects the ctx object, calls transform(), and
// serialises the return value back to JSON.
//
// A fresh runtime per execution ensures scripts cannot share state across
// requests (no global variable leakage).
func (e *Engine) runScript(t *models.Transformer, payload []byte, tctx *models.TransformContext) ([]byte, error) {
	prog, err := getCompiled(t)
	if err != nil {
		return nil, err
	}

	vm := goja.New()

	// ── Sandbox: whitelist safe globals, remove dangerous ones ────────────
	vm.Set("console", consoleObject(vm, t.Name))
	vm.GlobalObject().Delete("eval")     // no dynamic eval
	vm.GlobalObject().Delete("Function") // no dynamic function construction

	// ── Inject timeout interrupt ───────────────────────────────────────────
	timer := time.AfterFunc(scriptTimeout, func() {
		vm.Interrupt(fmt.Errorf("script timeout after %s", scriptTimeout))
	})
	defer timer.Stop()

	// ── Run the program — retrieves the transform function ─────────────────
	transformFn, err := vm.RunProgram(prog)
	if err != nil {
		return nil, fmt.Errorf("script load error: %w", err)
	}

	callable, ok := goja.AssertFunction(transformFn)
	if !ok {
		return nil, fmt.Errorf("script must export a `transform(payload, ctx)` function")
	}

	// ── Parse payload JSON into a JS value ────────────────────────────────
	var rawPayload any
	if err := json.Unmarshal(payload, &rawPayload); err != nil {
		return nil, fmt.Errorf("payload is not valid JSON: %w", err)
	}
	jsPayload := vm.ToValue(rawPayload)

	// ── Build ctx object ─────────────────────────────────────────────────
	var ctxMap any = map[string]any{}
	if tctx != nil {
		ctxJSON, _ := json.Marshal(tctx)
		json.Unmarshal(ctxJSON, &ctxMap)
	}
	jsCtx := vm.ToValue(ctxMap)

	// ── Call transform(payload, ctx) ──────────────────────────────────────
	result, err := callable(goja.Undefined(), jsPayload, jsCtx)
	if err != nil {
		if ex, ok := err.(*goja.InterruptedError); ok {
			return nil, fmt.Errorf("execution interrupted: %v", ex.Value())
		}
		return nil, fmt.Errorf("runtime error: %w", err)
	}

	if goja.IsUndefined(result) || goja.IsNull(result) {
		return []byte("null"), nil
	}

	// ── Export result back to Go and serialise ────────────────────────────
	exported := result.Export()
	out, err := json.Marshal(exported)
	if err != nil {
		return nil, fmt.Errorf("failed to serialise transform output: %w", err)
	}
	return out, nil
}

// ── Console shim ──────────────────────────────────────────────────────────

// consoleObject returns a JS object with console.log / console.error bound
// to zerolog so script authors can debug their transforms in the gateway logs.
func consoleObject(vm *goja.Runtime, scriptName string) *goja.Object {
	obj := vm.NewObject()
	obj.Set("log", func(args ...goja.Value) {
		parts := make([]any, len(args))
		for i, a := range args {
			parts[i] = a.Export()
		}
		log.Debug().Str("script", scriptName).Interface("args", parts).Msg("[transform:log]")
	})
	obj.Set("error", func(args ...goja.Value) {
		parts := make([]any, len(args))
		for i, a := range args {
			parts[i] = a.Export()
		}
		log.Warn().Str("script", scriptName).Interface("args", parts).Msg("[transform:error]")
	})
	obj.Set("warn", obj.Get("error"))
	return obj
}
