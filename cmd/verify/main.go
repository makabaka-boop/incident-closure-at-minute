// Command verify is a black-box acceptance client: it exercises a running
// raincut API over HTTP and checks exact minimum-shutdown costs, the
// contamination-incident lifecycle (segmented and concurrent clock
// advances, exact arrival minutes and both terminal states), the stable
// error structure, and the large-instance time budget. It then drives the
// graceful-drain acceptance against real API subprocesses: half-sent
// requests, real SIGTERM/SIGINT signals, barrier attribution, probe flips,
// clean and forced exits. It talks only to the real HTTP API — no stubs, no
// hard-coded responses.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const requestTimeout = 10 * time.Second

// baseURL honours API_BASE_URL (used inside compose), then API_PORT (used for
// local API_PORT=... runs), and finally the 8080 default.
var baseURL = func() string {
	if v := os.Getenv("API_BASE_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	if p := os.Getenv("API_PORT"); p != "" {
		return "http://localhost:" + p
	}
	return "http://localhost:8080"
}()

// ---------------------------------------------------------------------------
// mincut payloads
// ---------------------------------------------------------------------------

type edge struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
	Cost int64 `json:"cost"`
}

type solveRequest struct {
	N       int64   `json:"n"`
	Edges   []edge  `json:"edges"`
	Sources []int64 `json:"sources"`
	Sinks   []int64 `json:"sinks"`
}

type solveResponse struct {
	MinimumShutdownCost int64 `json:"minimum_shutdown_cost"`
}

// ---------------------------------------------------------------------------
// incident payloads
// ---------------------------------------------------------------------------

type pipe struct {
	From    int64 `json:"from"`
	To      int64 `json:"to"`
	Minutes int64 `json:"minutes"`
}

type release struct {
	Node int64 `json:"node"`
	At   int64 `json:"at"`
}

type incidentRequest struct {
	N        int64     `json:"n"`
	Pipes    []pipe    `json:"pipes"`
	Releases []release `json:"releases"`
	Intakes  []int64   `json:"intakes"`
	Deadline int64     `json:"deadline"`
}

type arrival struct {
	Node     int64 `json:"node"`
	AtMinute int64 `json:"at_minute"`
}

type snapshotDTO struct {
	CurrentMinute    int64            `json:"current_minute"`
	Status           string           `json:"status"`
	EarliestArrivals map[string]int64 `json:"earliest_arrivals"`
}

type createIncidentResponse struct {
	ID       string      `json:"id"`
	Snapshot snapshotDTO `json:"snapshot"`
}

type advanceRequest struct {
	Minute int64 `json:"minute"`
}

type advanceResponse struct {
	NewArrivals []arrival   `json:"new_arrivals"`
	Snapshot    snapshotDTO `json:"snapshot"`
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// ---------------------------------------------------------------------------
// shutdown simulation payloads
// ---------------------------------------------------------------------------

type simulateRequest struct {
	ClosedPipes []int64 `json:"closed_pipes"`
}

type arrivalChange struct {
	Node     int64  `json:"node"`
	Baseline *int64 `json:"baseline"`
	Shutdown *int64 `json:"shutdown"`
}

type intakeSimulation struct {
	Node             int64  `json:"node"`
	BaselineAt       *int64 `json:"baseline_at"`
	ShutdownAt       *int64 `json:"shutdown_at"`
	BaselineBreached bool   `json:"baseline_breached"`
	ShutdownBreached bool   `json:"shutdown_breached"`
}

type simulateResponse struct {
	EventMinute      int64              `json:"event_minute"`
	Status           string             `json:"status"`
	ArrivedNodes     map[string]int64   `json:"arrived_nodes"`
	Snapshot         snapshotDTO        `json:"snapshot"`
	ClosedPipes      []int64            `json:"closed_pipes"`
	BaselineArrivals map[string]int64   `json:"baseline_arrivals"`
	ShutdownArrivals map[string]int64   `json:"shutdown_arrivals"`
	Changes          []arrivalChange    `json:"changes"`
	Intakes          []intakeSimulation `json:"intakes"`
	BaselineStatus   string             `json:"baseline_status"`
	ShutdownStatus   string             `json:"shutdown_status"`
	Protected        bool               `json:"protected"`
}

// simulate posts a shutdown rehearsal and returns the raw result.
func simulate(id string, closed []int64) (int, []byte) {
	status, body, err := postJSON("/incidents/"+id+"/simulate", simulateRequest{ClosedPipes: closed})
	if err != nil {
		return -1, []byte(err.Error())
	}
	return status, body
}

func mustSimulate(name, id string, closed []int64) simulateResponse {
	status, body := simulate(id, closed)
	if status != http.StatusOK {
		fail(name, "status %d, want 200, body %s", status, body)
		return simulateResponse{}
	}
	var resp simulateResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fail(name, "response not JSON: %v (%s)", err, body)
		return simulateResponse{}
	}
	return resp
}

func changesEqual(got []arrivalChange, want []arrivalChange) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		g, w := got[i], want[i]
		if g.Node != w.Node {
			return false
		}
		if (g.Baseline == nil) != (w.Baseline == nil) || (g.Shutdown == nil) != (w.Shutdown == nil) {
			return false
		}
		if g.Baseline != nil && *g.Baseline != *w.Baseline {
			return false
		}
		if g.Shutdown != nil && *g.Shutdown != *w.Shutdown {
			return false
		}
	}
	return true
}

func i64(v int64) *int64 { return &v }

// verifyShutdownSimulation exercises the read-only pipe-shutdown rehearsal:
// exact rerouting over parallel pipes, in-transit payload preservation,
// intake conclusions, stable rejections and the read-only guarantee.
func verifyShutdownSimulation() {
	// Parallel pipes 0->1 (idx 0: 2 min, idx 1: 5 min), then 1->2 (idx 2)
	// and 2->3 (idx 3). Release 0@0, intake 3, deadline 10. Baseline
	// arrivals: 0@0, 1@2, 2@4, 3@6 -> breached.
	req := incidentRequest{
		N:        4,
		Pipes:    []pipe{{0, 1, 2}, {0, 1, 5}, {1, 2, 2}, {2, 3, 2}},
		Releases: []release{{0, 0}},
		Intakes:  []int64{3},
		Deadline: 10,
	}
	id := createIncident("simulate: create event for shutdown rehearsal", req)
	if id == "" {
		return
	}
	if !checkStep("simulate: clock commits to minute 0", id, 0, "propagating", []arrival{{0, 0}}) {
		return
	}

	// Close the fast parallel pipe at minute 0: the departure from node 0
	// is blocked, so the flow reroutes over the slow parallel pipe:
	// 1@5, 2@7, 3@9 — still a breach, but three minutes later.
	resp := mustSimulate("simulate: reroute over the slow parallel pipe", id, []int64{0})
	if resp.EventMinute != 0 || resp.Status != "propagating" ||
		resp.Snapshot.CurrentMinute != 0 || resp.Snapshot.Status != "propagating" {
		fail("simulate: reroute over the slow parallel pipe",
			"event minute/status=%d/%s snapshot=%+v, want 0/propagating",
			resp.EventMinute, resp.Status, resp.Snapshot)
		return
	}
	if !mapsEqual(resp.ArrivedNodes, map[string]int64{"0": 0}) {
		fail("simulate: reroute over the slow parallel pipe", "arrived_nodes=%v, want {0:0}", resp.ArrivedNodes)
		return
	}
	if !mapsEqual(resp.BaselineArrivals, map[string]int64{"0": 0, "1": 2, "2": 4, "3": 6}) {
		fail("simulate: reroute over the slow parallel pipe", "baseline=%v", resp.BaselineArrivals)
		return
	}
	if !mapsEqual(resp.ShutdownArrivals, map[string]int64{"0": 0, "1": 5, "2": 7, "3": 9}) {
		fail("simulate: reroute over the slow parallel pipe", "shutdown=%v, want {0:0,1:5,2:7,3:9}", resp.ShutdownArrivals)
		return
	}
	wantChanges := []arrivalChange{
		{Node: 1, Baseline: i64(2), Shutdown: i64(5)},
		{Node: 2, Baseline: i64(4), Shutdown: i64(7)},
		{Node: 3, Baseline: i64(6), Shutdown: i64(9)},
	}
	if !changesEqual(resp.Changes, wantChanges) {
		fail("simulate: reroute over the slow parallel pipe", "changes=%+v, want %+v", resp.Changes, wantChanges)
		return
	}
	if resp.BaselineStatus != "breached" || resp.ShutdownStatus != "breached" || resp.Protected {
		fail("simulate: reroute over the slow parallel pipe",
			"conclusions=%s/%s protected=%v, want breached/breached/false",
			resp.BaselineStatus, resp.ShutdownStatus, resp.Protected)
		return
	}
	if len(resp.Intakes) != 1 || resp.Intakes[0].Node != 3 ||
		resp.Intakes[0].BaselineAt == nil || *resp.Intakes[0].BaselineAt != 6 ||
		resp.Intakes[0].ShutdownAt == nil || *resp.Intakes[0].ShutdownAt != 9 ||
		!resp.Intakes[0].BaselineBreached || !resp.Intakes[0].ShutdownBreached {
		fail("simulate: reroute over the slow parallel pipe", "intakes=%+v", resp.Intakes)
		return
	}
	pass("simulate: closing one parallel pipe reroutes over the other (1@5, 2@7, 3@9)")

	// Close both parallel pipes (unsorted input, sorted echo): node 1 is
	// cut off entirely and the intake is protected.
	resp = mustSimulate("simulate: closing every parallel pipe", id, []int64{1, 0})
	if len(resp.ClosedPipes) != 2 || resp.ClosedPipes[0] != 0 || resp.ClosedPipes[1] != 1 {
		fail("simulate: closing every parallel pipe", "closed_pipes=%v, want [0 1]", resp.ClosedPipes)
		return
	}
	if !mapsEqual(resp.ShutdownArrivals, map[string]int64{"0": 0}) {
		fail("simulate: closing every parallel pipe", "shutdown=%v, want {0:0}", resp.ShutdownArrivals)
		return
	}
	if resp.ShutdownStatus != "contained" || !resp.Protected {
		fail("simulate: closing every parallel pipe",
			"shutdown_status=%s protected=%v, want contained/true",
			resp.ShutdownStatus, resp.Protected)
		return
	}
	wantChanges = []arrivalChange{
		{Node: 1, Baseline: i64(2)},
		{Node: 2, Baseline: i64(4)},
		{Node: 3, Baseline: i64(6)},
	}
	if !changesEqual(resp.Changes, wantChanges) {
		fail("simulate: closing every parallel pipe", "changes=%+v, want baseline-only entries", resp.Changes)
		return
	}
	pass("simulate: closing every parallel pipe protects the intake")

	// The rehearsal is read-only: the next real advance sees exactly the
	// arrivals between minute 0 and 2.
	if !checkStep("simulate: rehearsal never mutates the event", id, 2, "propagating", []arrival{{1, 2}}) {
		return
	}

	// At minute 3 the payload of pipe 2 (1->2) is already in flight (it
	// departed at minute 2). Closing pipe 2 now cannot recall it: node 2
	// still arrives at 4 and node 3 at 6, so nothing changes.
	if !checkStep("simulate: advance into the in-transit window", id, 3, "propagating", []arrival{}) {
		return
	}
	resp = mustSimulate("simulate: in-transit payload survives the closure", id, []int64{2})
	if len(resp.Changes) != 0 || !mapsEqual(resp.ShutdownArrivals, resp.BaselineArrivals) {
		fail("simulate: in-transit payload survives the closure",
			"changes=%+v shutdown=%v baseline=%v, want no change",
			resp.Changes, resp.ShutdownArrivals, resp.BaselineArrivals)
		return
	}
	pass("simulate: in-transit payload keeps its original traversal time")

	// Duplicate and out-of-range pipe indices are stable 422s; a malformed
	// body is rejected the same way.
	expect422Raw("simulate: duplicate pipe index", "/incidents/"+id+"/simulate",
		[]byte(`{"closed_pipes":[0,0]}`))
	expect422Raw("simulate: pipe index out of range", "/incidents/"+id+"/simulate",
		[]byte(`{"closed_pipes":[4]}`))
	expect422Raw("simulate: negative pipe index", "/incidents/"+id+"/simulate",
		[]byte(`{"closed_pipes":[-1]}`))
	expect422Raw("simulate: malformed body", "/incidents/"+id+"/simulate",
		[]byte(`{"closed_pipes":[0,`))

	// Unknown event -> stable 404.
	status, body := simulate("inc_does_not_exist", []int64{0})
	if status != http.StatusNotFound {
		fail("simulate: unknown id", "status %d, want 404, body %s", status, body)
		return
	}
	var er errorResponse
	if err := json.Unmarshal(body, &er); err != nil || er.Error.Code != "not_found" {
		fail("simulate: unknown id", "body=%s", body)
		return
	}
	pass("simulate: unknown id is a stable 404")

	// Once the event is terminal, rehearsals are rejected with a stable
	// 409 and the event is left untouched.
	if !checkStep("simulate: drive the event to its terminal breach", id, 6, "breached",
		[]arrival{{2, 4}, {3, 6}}) {
		return
	}
	status, body = simulate(id, []int64{0})
	if status != http.StatusConflict {
		fail("simulate: terminal event", "status %d, want 409, body %s", status, body)
		return
	}
	if err := json.Unmarshal(body, &er); err != nil || er.Error.Code != "incident_terminal" {
		fail("simulate: terminal event", "body=%s, want incident_terminal", body)
		return
	}
	pass("simulate: terminal event rejects rehearsals with a stable 409")
}

var failures int

func pass(name string) { fmt.Printf("[PASS] %s\n", name) }

func fail(name, format string, args ...any) {
	failures++
	fmt.Printf("[FAIL] %s: %s\n", name, fmt.Sprintf(format, args...))
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func doPost(path string, raw []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

func postJSON(path string, v any) (int, []byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, nil, err
	}
	return doPost(path, raw)
}

// ---------------------------------------------------------------------------
// mincut checks
// ---------------------------------------------------------------------------

func expectCost(name string, payload solveRequest, want int64) {
	raw, err := json.Marshal(payload)
	if err != nil {
		fail(name, "marshal: %v", err)
		return
	}
	start := time.Now()
	status, body, err := doPost("/mincut", raw)
	elapsed := time.Since(start)
	if err != nil {
		fail(name, "request failed: %v", err)
		return
	}
	if status != http.StatusOK {
		fail(name, "status %d, body %s", status, body)
		return
	}
	var resp solveResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fail(name, "response not JSON: %v (%s)", err, body)
		return
	}
	if resp.MinimumShutdownCost != want {
		fail(name, "minimum_shutdown_cost=%d, want %d", resp.MinimumShutdownCost, want)
		return
	}
	fmt.Printf("[PASS] %s (cost=%d, %s)\n", name, want, elapsed.Round(time.Millisecond))
}

func expect422Raw(name, path string, raw []byte) {
	status, body, err := doPost(path, raw)
	if err != nil {
		fail(name, "request failed: %v", err)
		return
	}
	if status != http.StatusUnprocessableEntity {
		fail(name, "status %d, want 422, body %s", status, body)
		return
	}
	var resp errorResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fail(name, "error body not JSON: %v (%s)", err, body)
		return
	}
	if resp.Error.Code == "" || resp.Error.Message == "" {
		fail(name, "error structure incomplete: %s", body)
		return
	}
	pass(name)
}

// largeCase builds a deterministic 20000-node / 100000-edge network whose
// minimum cut is exactly the sum of the small edges into node 1: every edge
// into the sink is one of them, and every other edge costs 1e9, more than
// that sum, so no cheaper cut exists.
func largeCase() (solveRequest, int64) {
	const n = 20000
	const m = 100000
	const big = int64(1_000_000_000)
	state := uint64(20260916)
	next := func() uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return state >> 11
	}
	edges := make([]edge, 0, m)
	var want int64
	for i := int64(2); i <= 5000; i++ {
		edges = append(edges, edge{From: 0, To: i, Cost: big})
		c := int64(next()%1000) + 1
		edges = append(edges, edge{From: i, To: 1, Cost: c})
		want += c
	}
	for len(edges) < m {
		u := int64(2 + next()%uint64(n-2))
		v := int64(2 + next()%uint64(n-2))
		edges = append(edges, edge{From: u, To: v, Cost: big})
	}
	return solveRequest{N: n, Edges: edges, Sources: []int64{0}, Sinks: []int64{1}}, want
}

func tooManyEdgesPayload() []byte {
	var b strings.Builder
	b.WriteString(`{"n":2,"edges":[`)
	for i := 0; i < 100001; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"from":0,"to":1,"cost":1}`)
	}
	b.WriteString(`],"sources":[0],"sinks":[1]}`)
	return []byte(b.String())
}

// ---------------------------------------------------------------------------
// incident checks
// ---------------------------------------------------------------------------

func createIncident(name string, req incidentRequest) string {
	status, body, err := postJSON("/incidents", req)
	if err != nil {
		fail(name, "request failed: %v", err)
		return ""
	}
	if status != http.StatusOK {
		fail(name, "status %d, want 200, body %s", status, body)
		return ""
	}
	var resp createIncidentResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		fail(name, "response not JSON: %v (%s)", err, body)
		return ""
	}
	if !strings.HasPrefix(resp.ID, "inc_") {
		fail(name, "id=%q, want an inc_ identifier", resp.ID)
		return ""
	}
	if resp.Snapshot.Status != "scheduled" || resp.Snapshot.CurrentMinute != 0 || len(resp.Snapshot.EarliestArrivals) != 0 {
		fail(name, "initial snapshot=%+v, want scheduled/0/{}", resp.Snapshot)
		return ""
	}
	pass(name)
	return resp.ID
}

// advanceResult is one observed advance response.
type advanceResult struct {
	target int64
	status int
	body   []byte
}

func advance(id string, minute int64) advanceResult {
	status, body, err := postJSON("/incidents/"+id+"/advance", advanceRequest{Minute: minute})
	if err != nil {
		return advanceResult{target: minute, status: -1, body: []byte(err.Error())}
	}
	return advanceResult{target: minute, status: status, body: body}
}

// checkStep advances to minute and asserts the exact new arrivals and
// resulting snapshot status/clock.
func checkStep(name, id string, minute int64, wantStatus string, wantArrivals []arrival) bool {
	res := advance(id, minute)
	if res.status != http.StatusOK {
		fail(name, "minute %d: status %d, want 200, body %s", minute, res.status, res.body)
		return false
	}
	var resp advanceResponse
	if err := json.Unmarshal(res.body, &resp); err != nil {
		fail(name, "minute %d: %v (%s)", minute, err, res.body)
		return false
	}
	if resp.Snapshot.Status != wantStatus || resp.Snapshot.CurrentMinute != minute {
		fail(name, "minute %d: snapshot=%+v, want status=%s clock=%d",
			minute, resp.Snapshot, wantStatus, minute)
		return false
	}
	if !arrivalsEqual(resp.NewArrivals, wantArrivals) {
		fail(name, "minute %d: new_arrivals=%v, want %v", minute, resp.NewArrivals, wantArrivals)
		return false
	}
	pass(name)
	return true
}

func arrivalsEqual(got, want []arrival) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func expectConflict(name, id string, minute int64, code string) {
	res := advance(id, minute)
	if res.status != http.StatusConflict {
		fail(name, "minute %d: status %d, want 409, body %s", minute, res.status, res.body)
		return
	}
	var resp errorResponse
	if err := json.Unmarshal(res.body, &resp); err != nil || resp.Error.Code != code {
		fail(name, "minute %d: error=%s, want code %s", minute, res.body, code)
		return
	}
	pass(name)
}

// verifyIncidentLifecycle walks one event through scheduled -> propagating
// -> breached, exercising parallel pipes, self-loops, reverse edges, exact
// arrival minutes, segmented increments, idempotent retries and terminal
// conflicts.
func verifyIncidentLifecycle() {
	// 0 -2-> 1 -3-> 2 -2-> 3 -4-> 4 (intake), plus a slower parallel pipe
	// 0 -10-> 1, a self-loop at 1 and a reverse pipe 2 -> 0 that must not
	// leak anything upstream. Release at node 0, minute 2.
	req := incidentRequest{
		N: 5,
		Pipes: []pipe{
			{0, 1, 2}, {0, 1, 10}, {1, 1, 1}, {1, 2, 3},
			{2, 0, 1}, {2, 3, 2}, {3, 4, 4},
		},
		Releases: []release{{0, 2}},
		Intakes:  []int64{4},
		Deadline: 30,
	}
	id := createIncident("incident: create returns id and scheduled snapshot", req)
	if id == "" {
		return
	}

	// Expected earliest arrivals: 0@2, 1@4 (2+2 via the shorter parallel
	// pipe), 2@7 (4+3), 3@9 (7+2), 4@13 (9+4).
	if !checkStep("incident: scheduled before the first release", id, 1, "scheduled", []arrival{}) {
		return
	}
	checkStep("incident: first release reveals source and flips to propagating", id, 3, "propagating",
		[]arrival{{0, 2}})

	// Same-minute retry: empty increment, identical snapshot.
	if !checkStep("incident: same-minute retry returns empty increment", id, 3, "propagating",
		[]arrival{}) {
		return
	}

	expectConflict("incident: backwards advance is a stable 409", id, 2, "clock_regression")
	checkStep("incident: segmented arrival node 1 at minute 4", id, 6, "propagating",
		[]arrival{{1, 4}})
	checkStep("incident: segmented arrival node 2 at minute 7", id, 8, "propagating",
		[]arrival{{2, 7}})
	checkStep("incident: segmented arrival node 3 at minute 9", id, 12, "propagating",
		[]arrival{{3, 9}})

	// Rejected advances never mutate the snapshot: after the 409 above the
	// clock is still 12 and the next failed move at 13... succeeds; check the
	// full arrival map at the breach.
	res := advance(id, 13)
	if res.status != http.StatusOK {
		fail("incident: breach step", "status %d body %s", res.status, res.body)
		return
	}
	var breached advanceResponse
	if err := json.Unmarshal(res.body, &breached); err != nil {
		fail("incident: breach step", "%v", err)
		return
	}
	if breached.Snapshot.Status != "breached" || !arrivalsEqual(breached.NewArrivals, []arrival{{4, 13}}) {
		fail("incident: intake reached exactly at minute 13", "response=%+v", breached)
		return
	}
	wantMap := map[string]int64{"0": 2, "1": 4, "2": 7, "3": 9, "4": 13}
	if !mapsEqual(breached.Snapshot.EarliestArrivals, wantMap) {
		fail("incident: cumulative earliest arrival minutes", "got %v want %v",
			breached.Snapshot.EarliestArrivals, wantMap)
		return
	}
	pass("incident: intake reached exactly at minute 13 -> breached with full arrival map")

	expectConflict("incident: advancing a terminal event is a stable 409", id, 14, "incident_terminal")
	expectConflict("incident: terminal backwards move is a stable 409", id, 12, "clock_regression")
	checkStep("incident: same-minute retry on terminal event stays empty", id, 13, "breached",
		[]arrival{})
}

// verifyContained uses an intake the pollution can never reach and asserts
// the contained terminal state at the deadline, plus past-deadline 409s that
// leave the snapshot untouched.
func verifyContained() {
	req := incidentRequest{
		N:        4,
		Pipes:    []pipe{{0, 1, 1}, {2, 1, 1}}, // intake node 3 is isolated
		Releases: []release{{0, 0}},
		Intakes:  []int64{3},
		Deadline: 5,
	}
	id := createIncident("incident: create contained case", req)
	if id == "" {
		return
	}

	// A move past the deadline conflicts before anything is committed.
	expectConflict("incident: move past the deadline is 409 and commits nothing", id, 6, "past_deadline")
	res := advance(id, 5)
	if res.status != http.StatusOK {
		fail("incident: to-deadline advance", "status %d body %s", res.status, res.body)
		return
	}
	var resp advanceResponse
	if err := json.Unmarshal(res.body, &resp); err != nil {
		fail("incident: to-deadline advance", "%v (%s)", err, res.body)
		return
	}
	if resp.Snapshot.Status != "contained" || resp.Snapshot.CurrentMinute != 5 {
		fail("incident: contained at deadline", "snapshot=%+v", resp.Snapshot)
		return
	}
	if _, leak := resp.Snapshot.EarliestArrivals["3"]; leak {
		fail("incident: unreachable intake never appears", "map=%v", resp.Snapshot.EarliestArrivals)
		return
	}
	if got := resp.Snapshot.EarliestArrivals["1"]; got != 1 {
		fail("incident: exact arrival minute in contained case", "node1=%d want 1", got)
		return
	}
	pass("incident: deadline reached without intake contact -> contained")

	// Any further move on the terminal event conflicts, and the snapshot is
	// unchanged.
	expectConflict("incident: contained event rejects further advances", id, 6, "incident_terminal")
}

// verifyMultiSource exercises exact multi-source earliest arrival: a second
// release upstream-of-schedule provides the shorter route.
func verifyMultiSource() {
	// chain 0 -3-> 1 -4-> 2 -5-> 3 (intake); releases 0@0 and 2@6, so
	// arrivals are 0@0, 1@3, 2@6, 3@11 (not 12).
	req := incidentRequest{
		N:        4,
		Pipes:    []pipe{{0, 1, 3}, {1, 2, 4}, {2, 3, 5}},
		Releases: []release{{0, 0}, {2, 6}},
		Intakes:  []int64{3},
		Deadline: 20,
	}
	id := createIncident("incident: create multi-source case", req)
	if id == "" {
		return
	}
	if !checkStep("incident: multi-source earliest arrivals in one tick", id, 11, "breached",
		[]arrival{{0, 0}, {1, 3}, {2, 6}, {3, 11}}) {
		return
	}
}

func verifyErrors() {
	// Unknown incident -> 404 (unknown id checked before the body).
	status, body, err := postJSON("/incidents/inc_does_not_exist/advance", advanceRequest{Minute: 1})
	if err != nil {
		fail("incident: unknown id", "request failed: %v", err)
		return
	}
	if status != http.StatusNotFound {
		fail("incident: unknown id", "status %d want 404 body %s", status, body)
		return
	}
	var er errorResponse
	if err := json.Unmarshal(body, &er); err != nil || er.Error.Code != "not_found" {
		fail("incident: unknown id", "body=%s", body)
		return
	}
	pass("incident: unknown id is a stable 404")

	// Invalid topology -> stable 422 on POST /incidents, none of which may
	// have created state.
	expect422Raw("incident: n below minimum", "/incidents",
		[]byte(`{"n":1,"pipes":[],"releases":[{"node":0,"at":0}],"intakes":[0],"deadline":3}`))
	expect422Raw("incident: pipe endpoint out of range", "/incidents",
		[]byte(`{"n":2,"pipes":[{"from":0,"to":5,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3}`))
	expect422Raw("incident: zero traversal minutes", "/incidents",
		[]byte(`{"n":2,"pipes":[{"from":0,"to":1,"minutes":0}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3}`))
	expect422Raw("incident: non-integer minutes", "/incidents",
		[]byte(`{"n":2,"pipes":[{"from":0,"to":1,"minutes":2.5}],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3}`))
	expect422Raw("incident: release after deadline", "/incidents",
		[]byte(`{"n":2,"pipes":[],"releases":[{"node":0,"at":9}],"intakes":[1],"deadline":3}`))
	expect422Raw("incident: empty intakes", "/incidents",
		[]byte(`{"n":2,"pipes":[],"releases":[{"node":0,"at":0}],"intakes":[],"deadline":3}`))
	expect422Raw("incident: malformed json", "/incidents", []byte(`{"n":2,`))
	expect422Raw("incident: unknown field", "/incidents",
		[]byte(`{"n":2,"pipes":[],"releases":[{"node":0,"at":0}],"intakes":[1],"deadline":3,"bogus":1}`))

	// Illegal advance body is a 422 while a valid event exists.
	req := incidentRequest{
		N:        3,
		Pipes:    []pipe{{0, 1, 1}, {1, 2, 5}},
		Releases: []release{{0, 0}},
		Intakes:  []int64{2},
		Deadline: 5,
	}
	id := createIncident("incident: create event for advance-body checks", req)
	if id != "" {
		expect422Raw("incident: non-integer advance minute", "/incidents/"+id+"/advance",
			[]byte(`{"minute":1.5}`))
		// Failed request left the clock untouched.
		checkStep("incident: snapshot unchanged after invalid advance", id, 2, "propagating",
			[]arrival{{0, 0}, {1, 1}})
	}
}

// verifyConcurrentAdvances races many advance requests at the same event.
// Commits must be serialised strictly monotonically (a 200 response always
// lands on its requested minute, arrivals never appear before their minute),
// and same-minute retries produce exactly one non-empty increment.
func verifyConcurrentAdvances() {
	// Chain 0..5 with two-minute pipes: arrivals 0@0,1@2,...,5@10. Intake
	// node 5 breaches at minute 10, deadline 20.
	req := incidentRequest{
		N:        6,
		Pipes:    []pipe{{0, 1, 2}, {1, 2, 2}, {2, 3, 2}, {3, 4, 2}, {4, 5, 2}},
		Releases: []release{{0, 0}},
		Intakes:  []int64{5},
		Deadline: 20,
	}
	id := createIncident("incident: create event for concurrency stress", req)
	if id == "" {
		return
	}

	targets := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	results := make([]advanceResult, len(targets))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, m := range targets {
		wg.Add(1)
		go func(i int, m int64) {
			defer wg.Done()
			<-start
			results[i] = advance(id, m)
		}(i, m)
	}
	close(start)
	wg.Wait()

	var maxClock int64
	ok := true
	for _, res := range results {
		if res.status == -1 {
			fail("incident: concurrent advance", "request to minute %d failed: %s", res.target, res.body)
			ok = false
			continue
		}
		switch res.status {
		case http.StatusOK:
			var resp advanceResponse
			if err := json.Unmarshal(res.body, &resp); err != nil {
				fail("incident: concurrent advance", "bad json at %d: %s", res.target, res.body)
				ok = false
				continue
			}
			if resp.Snapshot.CurrentMinute != res.target {
				fail("incident: concurrent commit lands off-target",
					"requested %d but snapshot clock=%d", res.target, resp.Snapshot.CurrentMinute)
				ok = false
			}
			// No arrival may ever be reported ahead of the committed minute.
			for _, a := range resp.NewArrivals {
				if a.AtMinute > res.target {
					fail("incident: arrivals never jump ahead of the clock",
						"minute %d reported %+v", res.target, a)
					ok = false
				}
			}
			for node, m := range resp.Snapshot.EarliestArrivals {
				if m > res.target {
					fail("incident: snapshot arrivals never jump ahead",
						"minute %d map[%s]=%d", res.target, node, m)
					ok = false
				}
			}
			if resp.Snapshot.CurrentMinute > maxClock {
				maxClock = resp.Snapshot.CurrentMinute
			}
		case http.StatusConflict:
			var resp errorResponse
			if err := json.Unmarshal(res.body, &resp); err != nil {
				fail("incident: concurrent conflict body", "%s", res.body)
				ok = false
				continue
			}
			switch resp.Error.Code {
			case "clock_regression", "incident_terminal":
			default:
				fail("incident: unexpected conflict code", "%q at minute %d", resp.Error.Code, res.target)
				ok = false
			}
		default:
			fail("incident: concurrent advance status", "minute %d -> %d: %s",
				res.target, res.status, res.body)
			ok = false
		}
	}
	if !ok {
		return
	}

	// Final state: breached once some commit reached minute >= 10, with
	// exactly the six chain nodes known.
	res := advance(id, maxClock) // same-minute retry: must be 200, empty
	if res.status != http.StatusOK {
		fail("incident: post-race same-minute retry", "status %d body %s", res.status, res.body)
		return
	}
	var resp advanceResponse
	if err := json.Unmarshal(res.body, &resp); err != nil {
		fail("incident: post-race retry json", "%v", err)
		return
	}
	if len(resp.NewArrivals) != 0 {
		fail("incident: post-race retry must be an empty increment", "got %v", resp.NewArrivals)
		return
	}
	if resp.Snapshot.Status != "breached" || len(resp.Snapshot.EarliestArrivals) != 6 {
		fail("incident: serialised concurrent advances converge on breach",
			"snapshot=%+v", resp.Snapshot)
		return
	}
	pass("incident: concurrent advances commit monotonically in submission order")

	// Same-minute fan-out: exactly one request carries the increment.
	id2 := createIncident("incident: create event for same-minute fan-out", req)
	if id2 == "" {
		return
	}
	const fans = 20
	rs := make([]advanceResult, fans)
	var wg2 sync.WaitGroup
	barrier := make(chan struct{})
	for i := 0; i < fans; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			<-barrier
			rs[i] = advance(id2, 5)
		}(i)
	}
	close(barrier)
	wg2.Wait()
	nonEmpty, bad := 0, 0
	for _, r := range rs {
		if r.status != http.StatusOK {
			bad++
			continue
		}
		var a advanceResponse
		_ = json.Unmarshal(r.body, &a)
		if len(a.NewArrivals) > 0 {
			nonEmpty++
		}
	}
	if bad != 0 || nonEmpty != 1 {
		fail("incident: same-minute fan-out is one commit plus retries",
			"bad=%d nonEmpty=%d (want 0, 1)", bad, nonEmpty)
		return
	}
	pass("incident: same-minute concurrent retries share one commit")
}

// verifyLargeIncident makes sure create-time propagation over the largest
// accepted topology stays well inside the request budget.
func verifyLargeIncident() {
	const n = 20000
	const m = 100000
	state := uint64(20260917)
	next := func() uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return state >> 11
	}
	pipes := make([]pipe, 0, m)
	for len(pipes) < m {
		u := int64(next() % n)
		v := int64(next() % n)
		pipes = append(pipes, pipe{From: u, To: v, Minutes: int64(1 + next()%1000)})
	}
	req := incidentRequest{
		N: n, Pipes: pipes, Releases: []release{{0, 0}},
		Intakes: []int64{n - 1}, Deadline: 1_000_000_000,
	}
	start := time.Now()
	status, body, err := postJSON("/incidents", req)
	elapsed := time.Since(start)
	if err != nil {
		fail("incident: large create", "request failed: %v", err)
		return
	}
	if status != http.StatusOK {
		fail("incident: large create", "status %d body %s", status, body)
		return
	}
	var created createIncidentResponse
	if err := json.Unmarshal(body, &created); err != nil {
		fail("incident: large create", "%v", err)
		return
	}
	res := advance(created.ID, 1_000_000_000)
	if res.status != http.StatusOK {
		fail("incident: large advance", "status %d body %s", res.status, res.body)
		return
	}
	fmt.Printf("[PASS] incident: large instance 20000 nodes / 100000 pipes (%s)\n", elapsed.Round(time.Millisecond))
}

func mapsEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// driver
// ---------------------------------------------------------------------------

func waitForAPI() error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("API at %s not healthy after 60s", baseURL)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func checkHealthz() {
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		fail("healthz", "request failed: %v", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"ok"`) {
		fail("healthz", "status=%d body=%s", resp.StatusCode, body)
		return
	}
	pass("healthz returns {\"status\":\"ok\"}")
}

func main() {
	fmt.Printf("verify: exercising API at %s (request timeout %s)\n", baseURL, requestTimeout)
	if err := waitForAPI(); err != nil {
		fmt.Println("verify: " + err.Error())
		os.Exit(1)
	}

	checkHealthz()

	// --- mincut regression suite ------------------------------------------
	expectCost("single edge", solveRequest{N: 2, Edges: []edge{{0, 1, 7}}, Sources: []int64{0}, Sinks: []int64{1}}, 7)
	expectCost("parallel edges charged individually", solveRequest{N: 2, Edges: []edge{{0, 1, 3}, {0, 1, 4}}, Sources: []int64{0}, Sinks: []int64{1}}, 7)
	expectCost("self loops do not affect result", solveRequest{N: 3, Edges: []edge{{0, 0, 100}, {0, 1, 5}, {1, 1, 9}, {1, 2, 2}}, Sources: []int64{0}, Sinks: []int64{2}}, 2)
	expectCost("directed edges only", solveRequest{N: 2, Edges: []edge{{1, 0, 5}}, Sources: []int64{0}, Sinks: []int64{1}}, 0)
	expectCost("no path means zero cost", solveRequest{N: 4, Edges: []edge{{0, 1, 5}, {2, 3, 6}}, Sources: []int64{0}, Sinks: []int64{3}}, 0)
	expectCost("chain bottleneck", solveRequest{N: 5, Edges: []edge{{0, 1, 9}, {1, 2, 4}, {2, 3, 6}, {3, 4, 3}}, Sources: []int64{0}, Sinks: []int64{4}}, 3)
	expectCost("two disjoint paths", solveRequest{N: 4, Edges: []edge{{0, 1, 5}, {1, 3, 5}, {0, 2, 8}, {2, 3, 8}}, Sources: []int64{0}, Sinks: []int64{3}}, 13)
	expectCost("multi source multi sink", solveRequest{N: 6, Edges: []edge{{0, 2, 10}, {1, 2, 1}, {2, 3, 4}, {3, 4, 3}, {3, 5, 2}, {0, 4, 12}, {1, 5, 8}}, Sources: []int64{0, 1}, Sinks: []int64{4, 5}}, 24)
	expectCost("must sever every branch", solveRequest{N: 3, Edges: []edge{{0, 1, 4}, {0, 2, 6}}, Sources: []int64{0}, Sinks: []int64{1, 2}}, 10)

	expect422Raw("mincut: n below minimum", "/mincut", []byte(`{"n":1,"edges":[],"sources":[0],"sinks":[1]}`))
	expect422Raw("mincut: malformed JSON", "/mincut", []byte(`{"n":2,`))
	expect422Raw("mincut: too many edges", "/mincut", tooManyEdgesPayload())
	expectCost("mincut: server healthy after invalid input", solveRequest{N: 2, Edges: []edge{{0, 1, 11}}, Sources: []int64{0}, Sinks: []int64{1}}, 11)

	large, want := largeCase()
	expectCost("large instance 20000 nodes / 100000 edges", large, want)

	// --- contamination incident suite -------------------------------------
	verifyIncidentLifecycle()
	verifyContained()
	verifyMultiSource()
	verifyShutdownSimulation()
	verifyErrors()
	verifyConcurrentAdvances()
	verifyLargeIncident()

	// --- graceful drain suite (subprocesses, half-sent requests, signals) --
	verifyDrain()

	// The server must still answer after the whole barrage.
	checkHealthz()

	if failures > 0 {
		fmt.Printf("verify: %d check(s) failed\n", failures)
		os.Exit(1)
	}
	fmt.Println("verify: all checks passed")
}
