// Package api exposes the HTTP interface of the raincut service.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"raincut/internal/flow"
	"raincut/internal/incident"
	"raincut/internal/lifecycle"
)

const (
	maxNodes     = 20000
	maxEdges     = 100000
	maxCost      = int64(1_000_000_000)
	maxBodyBytes = 32 << 20
)

// EdgeInput is one directed pipe in the request payload.
type EdgeInput struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
	Cost int64 `json:"cost"`
}

// SolveRequest is the payload accepted by POST /mincut.
type SolveRequest struct {
	N       int64       `json:"n"`
	Edges   []EdgeInput `json:"edges"`
	Sources []int64     `json:"sources"`
	Sinks   []int64     `json:"sinks"`
}

// SolveResponse carries the minimum shutdown cost.
type SolveResponse struct {
	MinimumShutdownCost int64 `json:"minimum_shutdown_cost"`
}

// ErrorResponse is the stable error structure returned for any rejected
// request.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes why a request was rejected.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// server holds the shared, concurrency-safe dependencies of the routes.
type server struct {
	incidents *incident.Store
	gate      *lifecycle.Gate
}

// NewMux builds the HTTP routing table with its own lifecycle gate. Tests
// that never drive a drain use it directly; the real process shares one gate
// between the mux and the signal handler via NewMuxWithGate.
func NewMux() http.Handler {
	return NewMuxWithGate(lifecycle.NewGate())
}

// NewMuxWithGate builds the HTTP routing table sharing the given admission
// gate. Business routes (/mincut, /incidents, /incidents/{id}/advance) are
// wrapped by the barrier; the /healthz and /readyz probes bypass it.
func NewMuxWithGate(g *lifecycle.Gate) http.Handler {
	mux := http.NewServeMux()
	s := &server{incidents: incident.NewStore(), gate: g}
	mux.HandleFunc("/mincut", s.admit(handleMinCut))
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/incidents", s.admit(s.handleCreateIncident))
	mux.HandleFunc("/incidents/{id}/advance", s.admit(s.handleAdvance))
	return mux
}

// admit enforces the drain barrier on a business route. While the gate is
// accepting the request takes a lease and runs to completion — even if its
// body is still arriving when the barrier falls. Once draining, the request
// never enters the handler, mutates no state, and gets a stable 503.
func (s *server) admit(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		release, ok := s.gate.Acquire()
		if !ok {
			writeError(w, http.StatusServiceUnavailable, "draining",
				"service is draining and no longer accepts new requests")
			return
		}
		defer release()
		h(w, r)
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz reports readiness: 200 while the gate accepts requests, 503
// with status "draining" once the drain barrier has fallen.
func (s *server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.gate.State() == lifecycle.Accepting {
		writeJSON(w, http.StatusOK, map[string]string{"status": "accepting"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
}

func handleMinCut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req SolveRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid_json",
			"body must be a single JSON object with integer fields: "+err.Error())
		return
	}
	if dec.More() {
		writeError(w, http.StatusUnprocessableEntity, "invalid_json",
			"body must contain exactly one JSON value")
		return
	}
	edges, sources, sinks, msg := validate(&req)
	if msg != "" {
		writeError(w, http.StatusUnprocessableEntity, "invalid_input", msg)
		return
	}
	cost := flow.MinShutdownCost(int(req.N), edges, sources, sinks)
	writeJSON(w, http.StatusOK, SolveResponse{MinimumShutdownCost: cost})
}

// validate checks every constraint; on failure it returns a human-readable
// message and the request never reaches the solver.
func validate(req *SolveRequest) ([]flow.Edge, []int, []int, string) {
	if req.N < 2 || req.N > maxNodes {
		return nil, nil, nil, fmt.Sprintf("n must satisfy 2 <= n <= %d, got %d", maxNodes, req.N)
	}
	n := int(req.N)
	if len(req.Edges) > maxEdges {
		return nil, nil, nil, fmt.Sprintf("at most %d edges allowed, got %d", maxEdges, len(req.Edges))
	}
	edges := make([]flow.Edge, 0, len(req.Edges))
	for i, e := range req.Edges {
		switch {
		case e.From < 0 || e.From >= req.N:
			return nil, nil, nil, fmt.Sprintf("edges[%d].from=%d is out of range [0,%d)", i, e.From, n)
		case e.To < 0 || e.To >= req.N:
			return nil, nil, nil, fmt.Sprintf("edges[%d].to=%d is out of range [0,%d)", i, e.To, n)
		case e.Cost < 1 || e.Cost > maxCost:
			return nil, nil, nil, fmt.Sprintf("edges[%d].cost=%d must satisfy 1 <= cost <= %d", i, e.Cost, maxCost)
		}
		edges = append(edges, flow.Edge{From: int(e.From), To: int(e.To), Cost: e.Cost})
	}
	if len(req.Sources) == 0 {
		return nil, nil, nil, "sources must be a non-empty array of node ids"
	}
	if len(req.Sinks) == 0 {
		return nil, nil, nil, "sinks must be a non-empty array of node ids"
	}
	isSource := make([]bool, n)
	sources := make([]int, 0, len(req.Sources))
	for i, id := range req.Sources {
		if id < 0 || id >= req.N {
			return nil, nil, nil, fmt.Sprintf("sources[%d]=%d is out of range [0,%d)", i, id, n)
		}
		isSource[int(id)] = true
		sources = append(sources, int(id))
	}
	sinks := make([]int, 0, len(req.Sinks))
	for i, id := range req.Sinks {
		if id < 0 || id >= req.N {
			return nil, nil, nil, fmt.Sprintf("sinks[%d]=%d is out of range [0,%d)", i, id, n)
		}
		if isSource[int(id)] {
			return nil, nil, nil, fmt.Sprintf("node %d cannot be both a source and a sink", id)
		}
		sinks = append(sinks, int(id))
	}
	return edges, sources, sinks, ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorResponse{Error: ErrorDetail{Code: code, Message: message}})
}
