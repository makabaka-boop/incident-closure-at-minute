package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func simulate(t *testing.T, mux http.Handler, id, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/incidents/"+id+"/simulate", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("response is not JSON: %v (%q)", err, rec.Body.String())
	}
	return rec.Code, parsed
}

func mustCreate(t *testing.T, mux http.Handler, body string) string {
	t.Helper()
	status, resp := createIncident(t, mux, body)
	if status != http.StatusOK {
		t.Fatalf("create status=%d body=%v", status, resp)
	}
	return resp["id"].(string)
}

// TestSimulateEndpoint walks the full rehearsal contract over HTTP: the
// recorded snapshot, both plans' arrivals, the per-node changes and the
// intake conclusions, plus the read-only guarantee.
func TestSimulateEndpoint(t *testing.T) {
	mux := NewMux()
	// 0 -2-> 1 -2-> 2 (intake), release 0@0, deadline 10.
	id := mustCreate(t, mux,
		`{"n":3,"pipes":[{"from":0,"to":1,"minutes":2},{"from":1,"to":2,"minutes":2}],`+
			`"releases":[{"node":0,"at":0}],"intakes":[2],"deadline":10}`)
	st, resp := advance(t, mux, id, `{"minute":2}`)
	if st != http.StatusOK {
		t.Fatalf("advance status=%d body=%v", st, resp)
	}

	// Close pipe 1 (1->2) at minute 2: the departure from node 1 would
	// start exactly now and is blocked; the intake stays dry.
	st, sim := simulate(t, mux, id, `{"closed_pipes":[1]}`)
	if st != http.StatusOK {
		t.Fatalf("simulate status=%d body=%v", st, sim)
	}
	if sim["event_minute"].(float64) != 2 || sim["status"] != "propagating" {
		t.Fatalf("event minute/status=%v/%v, want 2/propagating", sim["event_minute"], sim["status"])
	}
	arrived := sim["arrived_nodes"].(map[string]any)
	if len(arrived) != 2 || arrived["0"].(float64) != 0 || arrived["1"].(float64) != 2 {
		t.Fatalf("arrived_nodes=%v, want {0:0, 1:2}", arrived)
	}
	snap := sim["snapshot"].(map[string]any)
	if snap["current_minute"].(float64) != 2 || snap["status"] != "propagating" {
		t.Fatalf("snapshot=%v, want minute 2 propagating", snap)
	}
	if got := sim["closed_pipes"].([]any); len(got) != 1 || got[0].(float64) != 1 {
		t.Fatalf("closed_pipes=%v, want [1]", got)
	}
	baseline := sim["baseline_arrivals"].(map[string]any)
	if len(baseline) != 3 || baseline["2"].(float64) != 4 {
		t.Fatalf("baseline_arrivals=%v, want {0:0, 1:2, 2:4}", baseline)
	}
	shutdown := sim["shutdown_arrivals"].(map[string]any)
	if len(shutdown) != 2 || shutdown["0"].(float64) != 0 || shutdown["1"].(float64) != 2 {
		t.Fatalf("shutdown_arrivals=%v, want {0:0, 1:2}", shutdown)
	}
	changes := sim["changes"].([]any)
	if len(changes) != 1 {
		t.Fatalf("changes=%v, want exactly one entry", changes)
	}
	ch := changes[0].(map[string]any)
	if ch["node"].(float64) != 2 || ch["baseline"].(float64) != 4 {
		t.Fatalf("change=%v, want node 2 baseline 4", ch)
	}
	if _, present := ch["shutdown"]; !present {
		t.Fatalf("change=%v, want an explicit null shutdown field", ch)
	}
	if ch["shutdown"] != nil {
		t.Fatalf("change shutdown=%v, want null (unreachable by the deadline)", ch["shutdown"])
	}
	intakes := sim["intakes"].([]any)
	if len(intakes) != 1 {
		t.Fatalf("intakes=%v, want one entry", intakes)
	}
	in0 := intakes[0].(map[string]any)
	if in0["node"].(float64) != 2 || in0["baseline_at"].(float64) != 4 ||
		in0["shutdown_at"] != nil ||
		in0["baseline_breached"] != true || in0["shutdown_breached"] != false {
		t.Fatalf("intake=%v, want node 2 baseline 4 breached / shutdown safe", in0)
	}
	if sim["baseline_status"] != "breached" || sim["shutdown_status"] != "contained" {
		t.Fatalf("statuses=%v/%v, want breached/contained",
			sim["baseline_status"], sim["shutdown_status"])
	}
	if sim["protected"] != true {
		t.Fatalf("protected=%v, want true", sim["protected"])
	}

	// The rehearsal is read-only: the clock, the recorded arrivals and the
	// advance behaviour are exactly as before.
	st, resp = advance(t, mux, id, `{"minute":2}`)
	if st != http.StatusOK || len(resp["new_arrivals"].([]any)) != 0 {
		t.Fatalf("same-minute retry after simulate: status=%d body=%v", st, resp)
	}
	st, resp = advance(t, mux, id, `{"minute":4}`)
	if st != http.StatusOK {
		t.Fatalf("advance after simulate: status=%d body=%v", st, resp)
	}
	if got := resp["new_arrivals"].([]any); len(got) != 1 ||
		got[0].(map[string]any)["node"].(float64) != 2 {
		t.Fatalf("new_arrivals after simulate=%v, want node 2 only", got)
	}
	if snapshotOf(t, resp)["status"] != "breached" {
		t.Fatalf("status after real advance=%v, want breached (simulate must not close pipes)",
			snapshotOf(t, resp)["status"])
	}
}

// TestSimulateEmptyClosureIsBaseline rehearses with no pipes closed: both
// plans coincide and there are no changes.
func TestSimulateEmptyClosureIsBaseline(t *testing.T) {
	mux := NewMux()
	id := mustCreate(t, mux,
		`{"n":2,"pipes":[{"from":0,"to":1,"minutes":3}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":5}`)
	st, sim := simulate(t, mux, id, `{"closed_pipes":[]}`)
	if st != http.StatusOK {
		t.Fatalf("status=%d body=%v", st, sim)
	}
	if len(sim["changes"].([]any)) != 0 {
		t.Fatalf("empty closure produced changes: %v", sim["changes"])
	}
	if sim["baseline_status"] != "breached" || sim["shutdown_status"] != "breached" ||
		sim["protected"] != false {
		t.Fatalf("empty closure conclusions=%v/%v protected=%v, want breached/breached/false",
			sim["baseline_status"], sim["shutdown_status"], sim["protected"])
	}
	if sim["event_minute"].(float64) != 0 {
		t.Fatalf("event_minute=%v, want 0 before any advance", sim["event_minute"])
	}
}

func TestSimulateRejectsDuplicateAndOutOfRange(t *testing.T) {
	mux := NewMux()
	id := mustCreate(t, mux,
		`{"n":3,"pipes":[{"from":0,"to":1,"minutes":1},{"from":1,"to":2,"minutes":1}],`+
			`"releases":[{"node":0,"at":0}],"intakes":[2],"deadline":10}`)
	for _, tc := range []struct{ name, body string }{
		{"duplicate", `{"closed_pipes":[0,0]}`},
		{"duplicate later", `{"closed_pipes":[1,0,1]}`},
		{"negative", `{"closed_pipes":[-1]}`},
		{"out of range", `{"closed_pipes":[2]}`},
		{"far out of range", `{"closed_pipes":[100]}`},
		{"non-integer", `{"closed_pipes":[0.5]}`},
		{"malformed json", `{"closed_pipes":[0,`},
		{"unknown field", `{"closed_pipes":[0],"pipes":[0]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, resp := simulate(t, mux, id, tc.body)
			if st != http.StatusUnprocessableEntity {
				t.Fatalf("status=%d want 422 body=%v", st, resp)
			}
			errObj := resp["error"].(map[string]any)
			if errObj["code"] == "" || errObj["message"] == "" {
				t.Fatalf("incomplete error: %v", errObj)
			}
		})
	}
	// Rejections never mutate the incident: the next advance behaves as if
	// nothing happened.
	st, resp := advance(t, mux, id, `{"minute":2}`)
	if st != http.StatusOK || snapshotOf(t, resp)["status"] != "breached" {
		t.Fatalf("advance after rejected simulates: status=%d body=%v", st, resp)
	}
}

func TestSimulateTerminalConflict(t *testing.T) {
	mux := NewMux()
	id := mustCreate(t, mux,
		`{"n":2,"pipes":[{"from":0,"to":1,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":5}`)
	st, _ := advance(t, mux, id, `{"minute":5}`)
	if st != http.StatusOK {
		t.Fatalf("advance to terminal: %d", st)
	}
	st, resp := simulate(t, mux, id, `{"closed_pipes":[0]}`)
	if st != http.StatusConflict {
		t.Fatalf("status=%d want 409 body=%v", st, resp)
	}
	if resp["error"].(map[string]any)["code"] != "incident_terminal" {
		t.Fatalf("error=%v, want incident_terminal", resp["error"])
	}
}

func TestSimulateUnknownIncident(t *testing.T) {
	st, resp := simulate(t, NewMux(), "inc_missing", `{"closed_pipes":[0]}`)
	if st != http.StatusNotFound {
		t.Fatalf("status=%d want 404 body=%v", st, resp)
	}
	if resp["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("error=%v, want not_found", resp["error"])
	}
}

func TestSimulateMethodNotAllowed(t *testing.T) {
	mux := NewMux()
	id := mustCreate(t, mux,
		`{"n":2,"pipes":[{"from":0,"to":1,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":5}`)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/incidents/"+id+"/simulate", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want 405", rec.Code)
	}
}

// TestSimulateConcurrentWithAdvances races rehearsals against advances and
// checks that every rehearsal is internally consistent with exactly one
// committed snapshot (never a mix of two states).
func TestSimulateConcurrentWithAdvances(t *testing.T) {
	mux := NewMux()
	id := mustCreate(t, mux,
		`{"n":6,"pipes":[{"from":0,"to":1,"minutes":2},{"from":1,"to":2,"minutes":2},`+
			`{"from":2,"to":3,"minutes":2},{"from":3,"to":4,"minutes":2},{"from":4,"to":5,"minutes":2}],`+
			`"releases":[{"node":0,"at":0}],"intakes":[5],"deadline":20}`)

	var wg sync.WaitGroup
	for m := 1; m <= 9; m++ {
		wg.Add(1)
		go func(minute int) {
			defer wg.Done()
			advance(t, mux, id, fmt.Sprintf(`{"minute":%d}`, minute))
		}(m)
	}
	errs := make(chan string, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, sim := simulate(t, mux, id, `{"closed_pipes":[2]}`)
			if st == http.StatusConflict {
				return // the event turned terminal mid-race: a legal rejection
			}
			if st != http.StatusOK {
				errs <- fmt.Sprintf("status=%d body=%v", st, sim)
				return
			}
			minute := sim["event_minute"].(float64)
			snap := sim["snapshot"].(map[string]any)
			if snap["current_minute"].(float64) != minute {
				errs <- fmt.Sprintf("snapshot minute %v != event_minute %v", snap["current_minute"], minute)
				return
			}
			arrived := sim["arrived_nodes"].(map[string]any)
			snapArr := snap["earliest_arrivals"].(map[string]any)
			if len(arrived) != len(snapArr) {
				errs <- fmt.Sprintf("arrived_nodes %v != snapshot arrivals %v", arrived, snapArr)
				return
			}
			for node, at := range arrived {
				if at.(float64) > minute {
					errs <- fmt.Sprintf("node %s arrived at %v ahead of minute %v", node, at, minute)
					return
				}
			}
			baseline := sim["baseline_arrivals"].(map[string]any)
			for _, c := range sim["changes"].([]any) {
				ch := c.(map[string]any)
				node := fmt.Sprintf("%v", ch["node"].(float64))
				if b, ok := ch["baseline"].(float64); ok && baseline[node] != b {
					errs <- fmt.Sprintf("change baseline for node %s inconsistent with map", node)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Fatal(msg)
	}
}
