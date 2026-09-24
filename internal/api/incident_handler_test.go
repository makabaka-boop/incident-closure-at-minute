package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func doIncident(t *testing.T, body string) (int, map[string]any) {
	t.Helper()
	return createIncident(t, NewMux(), body)
}

func createIncident(t *testing.T, mux http.Handler, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/incidents", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("response is not JSON: %v (%q)", err, rec.Body.String())
	}
	return rec.Code, parsed
}

func advance(t *testing.T, mux http.Handler, id, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/incidents/"+id+"/advance", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("response is not JSON: %v (%q)", err, rec.Body.String())
	}
	return rec.Code, parsed
}

func shutdownPreview(t *testing.T, mux http.Handler, id, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/incidents/"+id+"/shutdown-preview", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("response is not JSON: %v (%q)", err, rec.Body.String())
	}
	return rec.Code, parsed
}

func snapshotOf(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	snap, ok := body["snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("missing snapshot: %v", body)
	}
	return snap
}

func TestCreateIncidentInitialSnapshot(t *testing.T) {
	body := `{"n":3,"pipes":[{"from":0,"to":1,"minutes":2},{"from":1,"to":2,"minutes":3}],"releases":[{"node":0,"at":1}],"intakes":[2],"deadline":10}`
	status, resp := doIncident(t, body)
	if status != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%v", status, resp)
	}
	id, _ := resp["id"].(string)
	if id == "" {
		t.Fatalf("missing id: %v", resp)
	}
	snap := snapshotOf(t, resp)
	if snap["current_minute"].(float64) != 0 || snap["status"] != "scheduled" {
		t.Fatalf("initial snapshot=%v, want minute 0 scheduled", snap)
	}
	arrivals, _ := snap["earliest_arrivals"].(map[string]any)
	if len(arrivals) != 0 {
		t.Fatalf("initial arrivals=%v, want {}", arrivals)
	}
}

func TestAdvanceSegmentedArrivals(t *testing.T) {
	mux := NewMux()
	body := `{"n":4,"pipes":[{"from":0,"to":1,"minutes":2},{"from":1,"to":2,"minutes":2},{"from":2,"to":3,"minutes":2}],"releases":[{"node":0,"at":0}],"intakes":[3],"deadline":10}`
	status, created := createIncident(t, mux, body)
	if status != http.StatusOK {
		t.Fatalf("create status=%d body=%v", status, created)
	}
	id := created["id"].(string)

	// t=1: before the first arrival (node 0 at 0 is already released, so it
	// arrives now), status propagating.
	st, resp := advance(t, mux, id, `{"minute":1}`)
	if st != http.StatusOK {
		t.Fatalf("advance status=%d body=%v", st, resp)
	}
	snap := snapshotOf(t, resp)
	if snap["status"] != "propagating" || snap["current_minute"].(float64) != 1 {
		t.Fatalf("snapshot=%v", snap)
	}
	newArr := resp["new_arrivals"].([]any)
	if len(newArr) != 1 {
		t.Fatalf("new_arrivals=%v, want [{node 0}]", newArr)
	}

	// Same minute retry: empty increment, same snapshot.
	st, retry := advance(t, mux, id, `{"minute":1}`)
	if st != http.StatusOK {
		t.Fatalf("retry status=%d body=%v", st, retry)
	}
	if len(retry["new_arrivals"].([]any)) != 0 {
		t.Fatalf("retry new_arrivals=%v, want []", retry["new_arrivals"])
	}
	if snapshotOf(t, retry)["current_minute"].(float64) != 1 {
		t.Fatalf("retry changed clock: %v", retry)
	}

	// Backwards: stable 409, snapshot untouched.
	st, conflict := advance(t, mux, id, `{"minute":0}`)
	if st != http.StatusConflict {
		t.Fatalf("backwards status=%d, want 409 body=%v", st, conflict)
	}
	if conflict["error"].(map[string]any)["code"] != "clock_regression" {
		t.Fatalf("error code=%v", conflict["error"])
	}

	// t=4: nodes 1 (2) and 2 (4) newly arrive.
	st, resp = advance(t, mux, id, `{"minute":4}`)
	if st != http.StatusOK {
		t.Fatalf("advance status=%d body=%v", st, resp)
	}
	newArr = resp["new_arrivals"].([]any)
	if len(newArr) != 2 {
		t.Fatalf("new_arrivals=%v, want 2", newArr)
	}
	first := newArr[0].(map[string]any)
	if first["node"].(float64) != 1 || first["at_minute"].(float64) != 2 {
		t.Fatalf("first new arrival=%v", first)
	}

	// t=6: intake node 3 arrives exactly at minute 6 -> breached.
	st, resp = advance(t, mux, id, `{"minute":6}`)
	if st != http.StatusOK {
		t.Fatalf("advance status=%d body=%v", st, resp)
	}
	snap = snapshotOf(t, resp)
	if snap["status"] != "breached" {
		t.Fatalf("status=%s, want breached", snap["status"])
	}

	// Terminal forward move: 409.
	st, conflict = advance(t, mux, id, `{"minute":7}`)
	if st != http.StatusConflict || conflict["error"].(map[string]any)["code"] != "incident_terminal" {
		t.Fatalf("terminal advance status=%d body=%v", st, conflict)
	}
	// Same-minute retry on terminal: still 200 with empty increment.
	st, resp = advance(t, mux, id, `{"minute":6}`)
	if st != http.StatusOK || len(resp["new_arrivals"].([]any)) != 0 {
		t.Fatalf("terminal retry status=%d body=%v", st, resp)
	}
}

func TestAdvanceContained(t *testing.T) {
	mux := NewMux()
	body := `{"n":3,"pipes":[{"from":0,"to":1,"minutes":1},{"from":2,"to":1,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[2],"deadline":5}`
	status, created := createIncident(t, mux, body)
	if status != http.StatusOK {
		t.Fatalf("create status=%d body=%v", status, created)
	}
	id := created["id"].(string)

	st, resp := advance(t, mux, id, `{"minute":5}`)
	if st != http.StatusOK {
		t.Fatalf("advance status=%d body=%v", st, resp)
	}
	snap := snapshotOf(t, resp)
	if snap["status"] != "contained" {
		t.Fatalf("status=%s, want contained at deadline", snap["status"])
	}
	arrivals := snap["earliest_arrivals"].(map[string]any)
	if _, has2 := arrivals["2"]; has2 {
		t.Fatalf("unreachable intake appeared: %v", arrivals)
	}
}

func TestAdvanceUnknownIncident(t *testing.T) {
	st, resp := advance(t, NewMux(), "does_not_exist", `{"minute":3}`)
	if st != http.StatusNotFound {
		t.Fatalf("status=%d, want 404 body=%v", st, resp)
	}
	if resp["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("error code=%v", resp["error"])
	}
}

func TestIncidentValidation422(t *testing.T) {
	cases := []struct{ name, body string }{
		{"n too small", `{"n":1,"pipes":[],"releases":[{"node":0,"at":0}],"intakes":[0],"deadline":3}`},
		{"pipe endpoint out of range", `{"n":2,"pipes":[{"from":0,"to":5,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3}`},
		{"negative minutes", `{"n":2,"pipes":[{"from":0,"to":1,"minutes":0}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3}`},
		{"non-integer minutes", `{"n":2,"pipes":[{"from":0,"to":1,"minutes":1.5}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3}`},
		{"release node out of range", `{"n":2,"pipes":[],"releases":[{"node":7,"at":0}],"intakes":[1],"deadline":3}`},
		{"release after deadline", `{"n":2,"pipes":[],"releases":[{"node":0,"at":9}],"intakes":[1],"deadline":3}`},
		{"empty releases", `{"n":2,"pipes":[],"releases":[],"intakes":[1],"deadline":3}`},
		{"empty intakes", `{"n":2,"pipes":[],"releases":[{"node":0,"at":0}],"intakes":[],"deadline":3}`},
		{"negative deadline", `{"n":2,"pipes":[],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":-1}`},
		{"malformed json", `{"n":2,`},
		{"unknown field", `{"n":2,"pipes":[],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3,"x":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, resp := doIncident(t, tc.body)
			if status != http.StatusUnprocessableEntity {
				t.Fatalf("status=%d want 422 body=%v", status, resp)
			}
			errObj := resp["error"].(map[string]any)
			if errObj["code"] == "" || errObj["message"] == "" {
				t.Fatalf("incomplete error: %v", errObj)
			}
		})
	}
}

func TestAdvancePastDeadline409(t *testing.T) {
	mux := NewMux()
	body := `{"n":2,"pipes":[{"from":0,"to":1,"minutes":10}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":5}`
	status, created := createIncident(t, mux, body)
	if status != http.StatusOK {
		t.Fatalf("create=%d %v", status, created)
	}
	id := created["id"].(string)
	st, resp := advance(t, mux, id, `{"minute":6}`)
	if st != http.StatusConflict || resp["error"].(map[string]any)["code"] != "past_deadline" {
		t.Fatalf("status=%d body=%v, want 409 past_deadline", st, resp)
	}
	// Failed advance must not change the snapshot.
	st, resp = advance(t, mux, id, `{"minute":5}`)
	if st != http.StatusOK || snapshotOf(t, resp)["current_minute"].(float64) != 5 {
		t.Fatalf("snapshot after failed advance: %d %v", st, resp)
	}
}

func TestAdvanceMalformedBody422(t *testing.T) {
	mux := NewMux()
	body := `{"n":2,"pipes":[{"from":0,"to":1,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":5}`
	status, created := createIncident(t, mux, body)
	if status != http.StatusOK {
		t.Fatalf("create=%d %v", status, created)
	}
	id, _ := created["id"].(string)
	st, resp := advance(t, mux, id, `{"minute":1.5}`)
	if st != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d want 422 body=%v", st, resp)
	}
}

func TestShutdownPreview(t *testing.T) {
	mux := NewMux()
	// 0@0 ->1 length2 ->2 length2 ->3 intake. At minute 2 closing pipe 1
	// blocks the traversal that starts exactly at that minute.
	body := `{"n":4,"pipes":[{"from":0,"to":1,"minutes":2},{"from":1,"to":2,"minutes":2},{"from":2,"to":3,"minutes":2}],"releases":[{"node":0,"at":0}],"intakes":[3],"deadline":10}`
	_, created := createIncident(t, mux, body)
	id := created["id"].(string)
	st, resp := advance(t, mux, id, `{"minute":2}`)
	if st != http.StatusOK {
		t.Fatalf("advance status=%d body=%v", st, resp)
	}

	st, resp = shutdownPreview(t, mux, id, `{"closed_pipe_indices":[1]}`)
	if st != http.StatusOK {
		t.Fatalf("preview status=%d body=%v", st, resp)
	}
	if resp["event_minute"].(float64) != 2 {
		t.Fatalf("event_minute=%v, want 2", resp["event_minute"])
	}
	if snapshotOf(t, resp)["current_minute"].(float64) != 2 {
		t.Fatalf("preview snapshot=%v", resp["snapshot"])
	}
	shutdown := resp["shutdown_plan"].(map[string]any)
	if shutdown["status"] != "contained" || resp["all_intakes_protected"] != true {
		t.Fatalf("preview=%v, want protected contained", resp)
	}
	arrivals := shutdown["earliest_arrivals"].(map[string]any)
	_, has2 := arrivals["2"]
	_, has3 := arrivals["3"]
	if has2 || has3 {
		t.Fatalf("closed pipe downstream arrived: %v", arrivals)
	}
	indices := resp["closed_pipe_indices"].([]any)
	if len(indices) != 1 || indices[0].(float64) != 1 {
		t.Fatalf("closed indices=%v", indices)
	}
	changes := resp["arrival_changes"].([]any)
	first := changes[0].(map[string]any)
	if first["node"].(float64) != 2 || first["shutdown_arrival_minute"] != nil {
		t.Fatalf("first change=%v, want node 2 to null", first)
	}
	conclusion := resp["intake_conclusions"].([]any)[0].(map[string]any)
	if conclusion["node"].(float64) != 3 || conclusion["shutdown_protected"] != true ||
		conclusion["original_arrival_minute"].(float64) != 6 || conclusion["shutdown_arrival_minute"] != nil {
		t.Fatalf("conclusion=%v", conclusion)
	}

	// A later preview at minute 3 on a fresh event cannot recall the packet
	// that entered the pipe at minute 2; it still reaches the intake at 6.
	_, created2 := createIncident(t, mux, body)
	id2 := created2["id"].(string)
	if st, resp = advance(t, mux, id2, `{"minute":3}`); st != http.StatusOK {
		t.Fatalf("advance second event: %d %v", st, resp)
	}
	st, resp = shutdownPreview(t, mux, id2, `{"closed_pipe_indices":[1]}`)
	if st != http.StatusOK {
		t.Fatalf("second preview status=%d body=%v", st, resp)
	}
	shutdown = resp["shutdown_plan"].(map[string]any)
	if shutdown["status"] != "breached" {
		t.Fatalf("shutdown status=%v, want breached because packet is in flight", shutdown["status"])
	}
	arrivals = shutdown["earliest_arrivals"].(map[string]any)
	if arrivals["2"].(float64) != 4 || arrivals["3"].(float64) != 6 {
		t.Fatalf("in-flight arrivals=%v, want node2=4 node3=6", arrivals)
	}

	// Read-only: advancing afterwards still reveals the original propagation.
	st, resp = advance(t, mux, id2, `{"minute":6}`)
	if st != http.StatusOK || snapshotOf(t, resp)["status"] != "breached" {
		t.Fatalf("original advance after preview: %d %v", st, resp)
	}
}

func TestShutdownPreviewErrors(t *testing.T) {
	mux := NewMux()
	body := `{"n":2,"pipes":[{"from":0,"to":1,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3}`
	_, created := createIncident(t, mux, body)
	id := created["id"].(string)
	if st, resp := advance(t, mux, id, `{"minute":1}`); st != http.StatusOK {
		t.Fatalf("advance: %d %v", st, resp)
	}
	for _, tc := range []struct{ name, payload, code string }{
		{"out of range", `{"closed_pipe_indices":[1]}`, "invalid_input"},
		{"negative", `{"closed_pipe_indices":[-1]}`, "invalid_input"},
		{"duplicate", `{"closed_pipe_indices":[0,0]}`, "invalid_input"},
		{"unknown field", `{"closed_pipe_indices":[0],"x":1}`, "invalid_json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, resp := shutdownPreview(t, mux, id, tc.payload)
			if st != http.StatusUnprocessableEntity {
				t.Fatalf("status=%d want 422 body=%v", st, resp)
			}
			if resp["error"].(map[string]any)["code"] != tc.code {
				t.Fatalf("error=%v, want code %s", resp["error"], tc.code)
			}
		})
	}
	if st, resp := shutdownPreview(t, mux, "missing", `{"closed_pipe_indices":[0]}`); st != http.StatusNotFound {
		t.Fatalf("unknown status=%d body=%v", st, resp)
	}
	terminalBody := `{"n":3,"pipes":[{"from":0,"to":1,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[2],"deadline":3}`
	_, terminalCreated := createIncident(t, mux, terminalBody)
	terminalID := terminalCreated["id"].(string)
	if st, resp := advance(t, mux, terminalID, `{"minute":3}`); st != http.StatusOK {
		t.Fatalf("advance terminal: %d %v", st, resp)
	}
	st, resp := shutdownPreview(t, mux, terminalID, `{"closed_pipe_indices":[0]}`)
	if st != http.StatusConflict || resp["error"].(map[string]any)["code"] != "incident_terminal" {
		t.Fatalf("terminal preview status=%d body=%v", st, resp)
	}
}
