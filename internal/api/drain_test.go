package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"raincut/internal/lifecycle"
)

// serve is a small helper that runs one request against a mux sharing gate.
func serve(t *testing.T, mux http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	var rdr *strings.Reader
	if body == "" {
		rdr = strings.NewReader("")
	} else {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("%s %s: response is not JSON: %v (%q)", method, path, err, rec.Body.String())
	}
	return rec.Code, parsed
}

func TestReadyzAccepting(t *testing.T) {
	status, body := serve(t, NewMux(), http.MethodGet, "/readyz", "")
	if status != http.StatusOK || body["status"] != "accepting" {
		t.Fatalf("readyz = %d %v, want 200 accepting", status, body)
	}
}

// TestBarrierRejectsBusinessRoutes drives a drain and checks that every
// business route answers a stable 503 without entering its handler, while
// the probes bypass the barrier.
func TestBarrierRejectsBusinessRoutes(t *testing.T) {
	gate := lifecycle.NewGate()
	mux := NewMuxWithGate(gate)
	gate.BeginDrain()

	valid := `{"n":2,"edges":[{"from":0,"to":1,"cost":3}],"sources":[0],"sinks":[1]}`
	for _, path := range []string{"/mincut", "/incidents", "/incidents/inc_x/advance"} {
		status, body := serve(t, mux, http.MethodPost, path, valid)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("POST %s = %d, want 503 (body=%v)", path, status, body)
		}
		errObj, ok := body["error"].(map[string]any)
		if !ok || errObj["code"] != "draining" || errObj["message"] == "" {
			t.Fatalf("POST %s: want stable draining error, got %v", path, body)
		}
	}

	status, body := serve(t, mux, http.MethodGet, "/healthz", "")
	if status != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz during drain = %d %v, want 200 ok", status, body)
	}
	status, body = serve(t, mux, http.MethodGet, "/readyz", "")
	if status != http.StatusServiceUnavailable || body["status"] != "draining" {
		t.Fatalf("readyz during drain = %d %v, want 503 draining", status, body)
	}
}

// TestInFlightLeaseOutlivesBarrier simulates a request that took its lease
// before the barrier: the gate must not drain until that lease is released,
// and requests decided after the barrier are rejected.
func TestInFlightLeaseOutlivesBarrier(t *testing.T) {
	gate := lifecycle.NewGate()
	release, ok := gate.Acquire()
	if !ok {
		t.Fatal("lease before the barrier must be granted")
	}
	gate.BeginDrain()
	select {
	case <-gate.Drained():
		t.Fatal("gate drained while a pre-barrier lease is outstanding")
	default:
	}
	mux := NewMuxWithGate(gate)
	status, _ := serve(t, mux, http.MethodPost, "/mincut",
		`{"n":2,"edges":[{"from":0,"to":1,"cost":3}],"sources":[0],"sinks":[1]}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("post-barrier request = %d, want 503", status)
	}
	release()
	select {
	case <-gate.Drained():
	default:
		t.Fatal("gate must drain once the pre-barrier lease is released")
	}
}
