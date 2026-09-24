package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"raincut/internal/incident"
)

// PipeInput is one directed pipe with a traversal time for POST /incidents.
type PipeInput struct {
	From    int64 `json:"from"`
	To      int64 `json:"to"`
	Minutes int64 `json:"minutes"`
}

// ReleaseInput is one contamination release: node and release minute.
type ReleaseInput struct {
	Node int64 `json:"node"`
	At   int64 `json:"at"`
}

// CreateIncidentRequest is the payload accepted by POST /incidents.
type CreateIncidentRequest struct {
	N        int64          `json:"n"`
	Pipes    []PipeInput    `json:"pipes"`
	Releases []ReleaseInput `json:"releases"`
	Intakes  []int64        `json:"intakes"`
	Deadline int64          `json:"deadline"`
}

// AdvanceRequest is the payload accepted by POST /incidents/{id}/advance.
type AdvanceRequest struct {
	Minute int64 `json:"minute"`
}

// ArrivalDTO is a node reached for the first time at a given minute.
type ArrivalDTO struct {
	Node     int64 `json:"node"`
	AtMinute int64 `json:"at_minute"`
}

// SnapshotDTO is the committed clock state returned with every response.
type SnapshotDTO struct {
	CurrentMinute    int64            `json:"current_minute"`
	Status           string           `json:"status"`
	EarliestArrivals map[string]int64 `json:"earliest_arrivals"`
}

// CreateIncidentResponse is returned by POST /incidents: the identifier and
// the initial snapshot.
type CreateIncidentResponse struct {
	ID       string      `json:"id"`
	Snapshot SnapshotDTO `json:"snapshot"`
}

// AdvanceResponse is returned by POST /incidents/{id}/advance: the nodes
// reached for the first time since the previous committed minute and the new
// snapshot.
type AdvanceResponse struct {
	NewArrivals []ArrivalDTO `json:"new_arrivals"`
	Snapshot    SnapshotDTO  `json:"snapshot"`
}

// snapshotDTO converts a domain snapshot into the JSON shape, emitting
// earliest arrivals sorted by node so identical snapshots serialise
// identically (Go sorts map keys already, but the ordering is made explicit).
func snapshotDTO(s incident.Snapshot) SnapshotDTO {
	out := SnapshotDTO{
		CurrentMinute:    s.CurrentMinute,
		Status:           string(s.Status),
		EarliestArrivals: make(map[string]int64, len(s.EarliestArrivals)),
	}
	for node, minute := range s.EarliestArrivals {
		out.EarliestArrivals[fmt.Sprintf("%d", node)] = minute
	}
	return out
}

// decodeStrict reads exactly one JSON object into dst, rejecting unknown
// fields, trailing values and malformed bodies. Any failure is reported as a
// stable 422 with code invalid_json.
func decodeStrict(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid_json",
			"body must be a single JSON object with integer fields: "+err.Error())
		return false
	}
	if dec.More() {
		writeError(w, http.StatusUnprocessableEntity, "invalid_json",
			"body must contain exactly one JSON value")
		return false
	}
	return true
}

func (s *server) handleCreateIncident(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var req CreateIncidentRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	spec, ok := buildSpec(w, &req)
	if !ok {
		return
	}
	in, err := s.incidents.Create(spec)
	if err != nil {
		var ve *incident.ValidationError
		if errors.As(err, &ve) {
			writeError(w, http.StatusUnprocessableEntity, "invalid_input", ve.Msg)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create incident")
		return
	}
	writeJSON(w, http.StatusOK, CreateIncidentResponse{ID: in.ID(), Snapshot: snapshotDTO(in.Snapshot())})
}

func (s *server) handleAdvance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	id := r.PathValue("id")
	in, exists := s.incidents.Get(id)
	if !exists {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("incident %q does not exist", id))
		return
	}
	var req AdvanceRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	if req.Minute < 0 {
		writeError(w, http.StatusUnprocessableEntity, "invalid_input",
			fmt.Sprintf("minute must be >= 0, got %d", req.Minute))
		return
	}
	newArrivals, snap, err := in.Advance(req.Minute)
	if err != nil {
		var ce *incident.ConflictError
		detail := err.Error()
		if errors.As(err, &ce) {
			detail = ce.Detail
		}
		switch {
		case errors.Is(err, incident.ErrClockRegression):
			writeError(w, http.StatusConflict, "clock_regression", detail)
		case errors.Is(err, incident.ErrPastDeadline):
			writeError(w, http.StatusConflict, "past_deadline", detail)
		case errors.Is(err, incident.ErrTerminal):
			writeError(w, http.StatusConflict, "incident_terminal", detail)
		default:
			writeError(w, http.StatusInternalServerError, "internal_error", "advance failed")
		}
		return
	}
	dto := make([]ArrivalDTO, 0, len(newArrivals))
	for _, a := range newArrivals {
		dto = append(dto, ArrivalDTO{Node: int64(a.Node), AtMinute: a.AtMinute})
	}
	writeJSON(w, http.StatusOK, AdvanceResponse{NewArrivals: dto, Snapshot: snapshotDTO(snap)})
}

// buildSpec converts the request into a domain spec; it reports a stable 422
// for any field outside the accepted integer domain.
func buildSpec(w http.ResponseWriter, req *CreateIncidentRequest) (incident.Spec, bool) {
	if req.N < 2 || req.N > incident.MaxNodes {
		return incident.Spec{}, invalidSpec(w, fmt.Sprintf("n must satisfy 2 <= n <= %d, got %d", incident.MaxNodes, req.N))
	}
	if req.Deadline < 0 || req.Deadline > incident.MaxMinutes {
		return incident.Spec{}, invalidSpec(w, fmt.Sprintf("deadline must satisfy 0 <= deadline <= %d, got %d", incident.MaxMinutes, req.Deadline))
	}
	n := int(req.N)
	if len(req.Pipes) > incident.MaxPipes {
		return incident.Spec{}, invalidSpec(w, fmt.Sprintf("at most %d pipes allowed, got %d", incident.MaxPipes, len(req.Pipes)))
	}
	spec := incident.Spec{N: n, Deadline: req.Deadline}
	spec.Pipes = make([]incident.Pipe, 0, len(req.Pipes))
	for i, p := range req.Pipes {
		if p.From < 0 || p.From >= req.N {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("pipes[%d].from=%d is out of range [0,%d)", i, p.From, n))
		}
		if p.To < 0 || p.To >= req.N {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("pipes[%d].to=%d is out of range [0,%d)", i, p.To, n))
		}
		if p.Minutes < 1 || p.Minutes > incident.MaxMinutes {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("pipes[%d].minutes=%d must satisfy 1 <= minutes <= %d", i, p.Minutes, incident.MaxMinutes))
		}
		spec.Pipes = append(spec.Pipes, incident.Pipe{From: int(p.From), To: int(p.To), Minutes: p.Minutes})
	}
	if len(req.Releases) == 0 {
		return incident.Spec{}, invalidSpec(w, "releases must be a non-empty array of {node, at} objects")
	}
	spec.Releases = make([]incident.Release, 0, len(req.Releases))
	for i, r := range req.Releases {
		if r.Node < 0 || r.Node >= req.N {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("releases[%d].node=%d is out of range [0,%d)", i, r.Node, n))
		}
		if r.At < 0 || r.At > req.Deadline {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("releases[%d].at=%d must satisfy 0 <= at <= deadline %d", i, r.At, req.Deadline))
		}
		spec.Releases = append(spec.Releases, incident.Release{Node: int(r.Node), At: r.At})
	}
	if len(req.Intakes) == 0 {
		return incident.Spec{}, invalidSpec(w, "intakes must be a non-empty array of node ids")
	}
	spec.Intakes = make([]int, 0, len(req.Intakes))
	for i, id := range req.Intakes {
		if id < 0 || id >= req.N {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("intakes[%d]=%d is out of range [0,%d)", i, id, n))
		}
		spec.Intakes = append(spec.Intakes, int(id))
	}
	return spec, true
}

func invalidSpec(w http.ResponseWriter, msg string) bool {
	writeError(w, http.StatusUnprocessableEntity, "invalid_input", msg)
	return false
}
