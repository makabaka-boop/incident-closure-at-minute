package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func doRequest(t *testing.T, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/mincut", strings.NewReader(body))
	rec := httptest.NewRecorder()
	NewMux().ServeHTTP(rec, req)
	var parsed map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("response is not JSON: %v (%q)", err, rec.Body.String())
	}
	return rec.Code, parsed
}

func TestSolveOK(t *testing.T) {
	status, body := doRequest(t, `{"n":2,"edges":[{"from":0,"to":1,"cost":3},{"from":0,"to":1,"cost":4}],"sources":[0],"sinks":[1]}`)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%v)", status, body)
	}
	got, ok := body["minimum_shutdown_cost"].(float64)
	if !ok || int64(got) != 7 {
		t.Fatalf("body = %v, want minimum_shutdown_cost=7", body)
	}
}

func TestSolveInvalid(t *testing.T) {
	cases := []struct{ name, body string }{
		{"n too small", `{"n":1,"edges":[],"sources":[0],"sinks":[1]}`},
		{"n too large", `{"n":20001,"edges":[],"sources":[0],"sinks":[1]}`},
		{"edge endpoint out of range", `{"n":2,"edges":[{"from":0,"to":2,"cost":1}],"sources":[0],"sinks":[1]}`},
		{"negative edge endpoint", `{"n":2,"edges":[{"from":-1,"to":1,"cost":1}],"sources":[0],"sinks":[1]}`},
		{"cost zero", `{"n":2,"edges":[{"from":0,"to":1,"cost":0}],"sources":[0],"sinks":[1]}`},
		{"cost too large", `{"n":2,"edges":[{"from":0,"to":1,"cost":1000000001}],"sources":[0],"sinks":[1]}`},
		{"non-integer cost", `{"n":2,"edges":[{"from":0,"to":1,"cost":1.5}],"sources":[0],"sinks":[1]}`},
		{"empty sources", `{"n":2,"edges":[],"sources":[],"sinks":[1]}`},
		{"missing sinks", `{"n":2,"edges":[],"sources":[0]}`},
		{"source sink overlap", `{"n":3,"edges":[],"sources":[0,1],"sinks":[1,2]}`},
		{"endpoint id out of range", `{"n":2,"edges":[],"sources":[0],"sinks":[5]}`},
		{"malformed json", `{"n":2,`},
		{"unknown field", `{"n":2,"edges":[],"sources":[0],"sinks":[1],"foo":1}`},
		{"trailing data", `{"n":2,"edges":[],"sources":[0],"sinks":[1]} {}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := doRequest(t, tc.body)
			if status != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422 (body=%v)", status, body)
			}
			errObj, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatalf("missing error object: %v", body)
			}
			if errObj["code"] == "" || errObj["message"] == "" {
				t.Fatalf("error object incomplete: %v", errObj)
			}
		})
	}
}

func TestTooManyEdges(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"n":2,"edges":[`)
	for i := 0; i < 100001; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"from":0,"to":1,"cost":1}`)
	}
	b.WriteString(`],"sources":[0],"sinks":[1]}`)
	status, _ := doRequest(t, b.String())
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", status)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/mincut", nil)
	rec := httptest.NewRecorder()
	NewMux().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestHealthz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	NewMux().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
