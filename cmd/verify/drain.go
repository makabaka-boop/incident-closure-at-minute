// This file is the graceful-drain acceptance suite of cmd/verify. It
// reproduces the two-phase drain against real child processes: the API
// binary is started as a subprocess on its own API_PORT, requests are held
// half-sent on raw TCP connections, and real SIGTERM/SIGINT signals drive
// the barrier. It checks barrier attribution (a lease taken before the
// barrier completes even though its body arrives afterwards; post-barrier
// requests get a stable 503 and never enter a handler), the probe changes
// (/readyz flips to 503/draining, /healthz stays 200), the clean-exit and
// force-close paths, and that repeated signals cause neither a second
// shutdown nor a deadlock.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// API binary location / build
// ---------------------------------------------------------------------------

var (
	binaryOnce sync.Once
	binaryPath string
	binaryErr  error
)

// ensureAPIBinary resolves the API binary used for the drain subprocesses:
// API_BINARY wins (the compose verify image sets it), otherwise the binary
// is built once from the repository sources this verify was compiled from.
func ensureAPIBinary() (string, error) {
	binaryOnce.Do(func() {
		if v := os.Getenv("API_BINARY"); v != "" {
			binaryPath = v
			return
		}
		_, src, _, ok := runtime.Caller(0)
		if !ok {
			binaryErr = fmt.Errorf("cannot locate repository root")
			return
		}
		root := filepath.Dir(filepath.Dir(filepath.Dir(src))) // cmd/verify/drain.go -> root
		dir, err := os.MkdirTemp("", "raincut-drain-bin")
		if err != nil {
			binaryErr = err
			return
		}
		out := filepath.Join(dir, "api")
		var log bytes.Buffer
		cmd := exec.Command("go", "build", "-trimpath", "-o", out, "./cmd/api")
		cmd.Dir = root
		cmd.Stdout = &log
		cmd.Stderr = &log
		if err := cmd.Run(); err != nil {
			binaryErr = fmt.Errorf("go build ./cmd/api: %v: %s", err, log.String())
			return
		}
		binaryPath = out
	})
	return binaryPath, binaryErr
}

// ---------------------------------------------------------------------------
// subprocess harness
// ---------------------------------------------------------------------------

// syncBuffer lets the child's stdout/stderr be read while the child is still
// writing them.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// exitResult carries the child's exit code (-1 when it could not be
// determined, e.g. killed by a signal we sent outside the test plan).
type exitResult struct {
	code int
	err  error
}

// apiProcess is one running API child with its own port and drain timeout.
type apiProcess struct {
	cmd  *exec.Cmd
	base string
	addr string
	logs *syncBuffer
	wait chan exitResult
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// startAPIProcess launches the API binary with API_PORT and DRAIN_TIMEOUT
// and waits for /healthz to answer 200.
func startAPIProcess(name, drainTimeout string) *apiProcess {
	bin, err := ensureAPIBinary()
	if err != nil {
		fail(name, "locate api binary: %v", err)
		return nil
	}
	port, err := freePort()
	if err != nil {
		fail(name, "reserve port: %v", err)
		return nil
	}
	logs := &syncBuffer{}
	cmd := exec.Command(bin)
	cmd.Stdout = logs
	cmd.Stderr = logs
	// Appended last so it wins over any inherited API_PORT.
	cmd.Env = append(os.Environ(),
		"API_PORT="+strconv.Itoa(port),
		"DRAIN_TIMEOUT="+drainTimeout,
	)
	if err := cmd.Start(); err != nil {
		fail(name, "start api: %v", err)
		return nil
	}
	p := &apiProcess{
		cmd:  cmd,
		base: "http://127.0.0.1:" + strconv.Itoa(port),
		addr: "127.0.0.1:" + strconv.Itoa(port),
		logs: logs,
		wait: make(chan exitResult, 1),
	}
	go func() {
		err := cmd.Wait()
		res := exitResult{code: 0, err: err}
		if err != nil {
			res.code = -1
			if ee, ok := err.(*exec.ExitError); ok {
				res.code = ee.ExitCode()
			}
		}
		p.wait <- res
	}()

	deadline := time.Now().Add(15 * time.Second)
	for {
		if code, exited := p.pollExit(); exited {
			fail(name, "api exited during startup (code %d): %s", code, logs.String())
			return nil
		}
		resp, err := http.Get(p.base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p
			}
		}
		if time.Now().After(deadline) {
			p.cmd.Process.Kill()
			fail(name, "api did not become healthy: %s", logs.String())
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// pollExit reports whether the child has already exited.
func (p *apiProcess) pollExit() (int, bool) {
	select {
	case res := <-p.wait:
		p.wait <- res // keep it observable for later checks
		return res.code, true
	default:
		return 0, false
	}
}

// expectAlive fails the check if the child has already exited.
func (p *apiProcess) expectAlive(name string) {
	if code, exited := p.pollExit(); exited {
		fail(name, "process exited early (code %d): %s", code, p.logs.String())
		return
	}
	pass(name)
}

// expectExit waits up to d for the child to exit and checks the exit code
// with ok(code).
func (p *apiProcess) expectExit(name string, d time.Duration, ok func(int) bool, want string) {
	select {
	case res := <-p.wait:
		if !ok(res.code) {
			fail(name, "exit code %d, want %s: %s", res.code, want, p.logs.String())
			return
		}
		pass(name)
	case <-time.After(d):
		p.cmd.Process.Kill()
		fail(name, "process did not exit within %s (deadlock?): %s", d, p.logs.String())
	}
}

func (p *apiProcess) signal(name string, sig os.Signal) {
	if err := p.cmd.Process.Signal(sig); err != nil {
		fail(name, "send %s: %v", sig, err)
	}
}

// ---------------------------------------------------------------------------
// half-sent request
// ---------------------------------------------------------------------------

// halfRequest is a POST whose headers and first body half have been written
// while the rest of the body is withheld until finish is called.
type halfRequest struct {
	conn   net.Conn
	reader *bufio.Reader
	rest   []byte
}

func startHalfRequest(name, addr, path string, body []byte) *halfRequest {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fail(name, "dial: %v", err)
		return nil
	}
	head := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n",
		path, addr, len(body))
	if _, err := conn.Write([]byte(head)); err != nil {
		conn.Close()
		fail(name, "write headers: %v", err)
		return nil
	}
	half := len(body) / 2
	if _, err := conn.Write(body[:half]); err != nil {
		conn.Close()
		fail(name, "write first body half: %v", err)
		return nil
	}
	return &halfRequest{conn: conn, reader: bufio.NewReader(conn), rest: body[half:]}
}

// finish sends the withheld body and reads the full response.
func (h *halfRequest) finish() (int, []byte, error) {
	if _, err := h.conn.Write(h.rest); err != nil {
		return 0, nil, fmt.Errorf("write remaining body: %w", err)
	}
	_ = h.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	resp, err := http.ReadResponse(h.reader, &http.Request{Method: http.MethodPost})
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

func (h *halfRequest) close() { h.conn.Close() }

// ---------------------------------------------------------------------------
// probe / request helpers bound to a child base URL
// ---------------------------------------------------------------------------

func getStatusBody(base, path string) (int, []byte, error) {
	resp, err := http.Get(base + path)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, body, err
}

// expectProbe polls path until it reports the wanted status and body
// fragment, or the deadline expires.
func expectProbe(name, base, path string, wantStatus int, wantFragment string, within time.Duration) {
	deadline := time.Now().Add(within)
	var lastErr error
	var lastStatus int
	var lastBody []byte
	for {
		lastStatus, lastBody, lastErr = getStatusBody(base, path)
		if lastErr == nil && lastStatus == wantStatus && strings.Contains(string(lastBody), wantFragment) {
			pass(name)
			return
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				fail(name, "%s: %v", path, lastErr)
			} else {
				fail(name, "%s: status %d body %s, want %d containing %q",
					path, lastStatus, lastBody, wantStatus, wantFragment)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// expectDraining503 posts a valid business payload and requires the stable
// 503 draining error: the handler must never run.
func expectDraining503(name, base, path string, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		fail(name, "marshal: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(raw))
	if err != nil {
		fail(name, "build request: %v", err)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fail(name, "request failed: %v", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusServiceUnavailable {
		fail(name, "status %d, want 503, body %s", resp.StatusCode, body)
		return
	}
	var er errorResponse
	if err := json.Unmarshal(body, &er); err != nil || er.Error.Code != "draining" || er.Error.Message == "" {
		fail(name, "want stable draining error, got %s", body)
		return
	}
	pass(name)
}

// ---------------------------------------------------------------------------
// drain scenarios
// ---------------------------------------------------------------------------

// drainMincutPayload is a valid /mincut body with a known answer (chain
// bottleneck: min(9,4,6,3) = 3).
const drainMincutPayload = `{"n":5,"edges":[` +
	`{"from":0,"to":1,"cost":9},{"from":1,"to":2,"cost":4},` +
	`{"from":2,"to":3,"cost":6},{"from":3,"to":4,"cost":3}],` +
	`"sources":[0],"sinks":[4]}`

// verifyDrainIdle covers the zero-lease path: SIGINT on an idle process
// drains immediately, Shutdown runs once and the process exits 0.
func verifyDrainIdle() {
	p := startAPIProcess("drain: idle child start", "5s")
	if p == nil {
		return
	}
	expectProbe("drain: /readyz is 200 accepting before any signal",
		p.base, "/readyz", http.StatusOK, `"accepting"`, 5*time.Second)
	p.signal("drain: idle SIGINT", syscall.SIGINT)
	p.expectExit("drain: idle process exits 0 after SIGINT", 10*time.Second,
		func(code int) bool { return code == 0 }, "0")
}

// verifyDrainNormal is the main barrier-attribution scenario: a request that
// took its lease before SIGTERM finishes even though its body arrives after
// the barrier; post-barrier requests get a stable 503; probes flip; repeated
// signals are absorbed; the process exits 0 once the last lease is released.
func verifyDrainNormal() {
	p := startAPIProcess("drain: child start", "10s")
	if p == nil {
		return
	}
	expectProbe("drain: /readyz 200 accepting while accepting",
		p.base, "/readyz", http.StatusOK, `"accepting"`, 5*time.Second)

	// Half-send a valid /mincut request: headers plus the first half of the
	// body. The handler starts, takes a lease and blocks reading the rest.
	half := startHalfRequest("drain: half-sent request", p.addr, "/mincut", []byte(drainMincutPayload))
	if half == nil {
		p.cmd.Process.Kill()
		return
	}
	time.Sleep(300 * time.Millisecond) // let the handler start and take its lease

	p.signal("drain: SIGTERM", syscall.SIGTERM)
	// Repeated signals during the drain must be absorbed, not cause a
	// second shutdown or a deadlock.
	p.signal("drain: repeated SIGTERM", syscall.SIGTERM)
	p.signal("drain: repeated SIGINT", syscall.SIGINT)

	expectProbe("drain: /readyz flips to 503 draining",
		p.base, "/readyz", http.StatusServiceUnavailable, `"draining"`, 5*time.Second)
	expectProbe("drain: /healthz stays 200 during drain",
		p.base, "/healthz", http.StatusOK, `"ok"`, 5*time.Second)

	// The outstanding lease must hold the drain open: the process is still
	// alive well after the signals.
	time.Sleep(300 * time.Millisecond)
	p.expectAlive("drain: in-flight lease holds the process alive")

	// Post-barrier business requests never enter a handler and get the
	// stable 503 draining error.
	expectDraining503("drain: post-barrier /mincut is a stable 503", p.base, "/mincut",
		json.RawMessage(drainMincutPayload))
	expectDraining503("drain: post-barrier /incidents is a stable 503", p.base, "/incidents",
		map[string]any{
			"n": 2, "pipes": []any{}, "releases": []map[string]int64{{"node": 0, "at": 0}},
			"intakes": []int64{1}, "deadline": 3,
		})

	// The pre-barrier request now finishes its body and must run to
	// completion with the exact solver answer.
	status, body, err := half.finish()
	half.close()
	if err != nil {
		fail("drain: pre-barrier request completes", "finish: %v", err)
		return
	}
	var resp solveResponse
	if status != http.StatusOK || json.Unmarshal(body, &resp) != nil || resp.MinimumShutdownCost != 3 {
		fail("drain: pre-barrier request completes", "status %d body %s, want 200 cost=3", status, body)
		return
	}
	pass("drain: pre-barrier half-sent request completes with cost=3")

	// Last lease released: drain completes, Shutdown runs once, exit 0.
	p.expectExit("drain: process exits 0 once the last lease is released", 10*time.Second,
		func(code int) bool { return code == 0 }, "0")
}

// verifyDrainTimeout holds a lease past DRAIN_TIMEOUT: the process must
// force-close and exit non-zero instead of waiting forever.
func verifyDrainTimeout() {
	p := startAPIProcess("drain: timeout child start", "2s")
	if p == nil {
		return
	}
	half := startHalfRequest("drain: timeout half-sent request", p.addr, "/mincut", []byte(drainMincutPayload))
	if half == nil {
		p.cmd.Process.Kill()
		return
	}
	defer half.close()
	time.Sleep(300 * time.Millisecond) // handler started, lease held

	p.signal("drain: timeout SIGTERM", syscall.SIGTERM)
	p.signal("drain: timeout repeated SIGTERM", syscall.SIGTERM)

	expectProbe("drain: /readyz draining while the lease is stuck",
		p.base, "/readyz", http.StatusServiceUnavailable, `"draining"`, 1500*time.Millisecond)
	p.expectAlive("drain: still draining before the timeout")

	// The body is never completed: after DRAIN_TIMEOUT the process must
	// force-close and exit non-zero.
	p.expectExit("drain: stuck lease forces close with non-zero exit", 10*time.Second,
		func(code int) bool { return code != 0 }, "non-zero")
}

// verifyDrain runs the graceful-drain acceptance suite against real child
// processes.
func verifyDrain() {
	if _, err := ensureAPIBinary(); err != nil {
		fail("drain: api binary available", "%v", err)
		return
	}
	pass("drain: api binary available")
	verifyDrainIdle()
	verifyDrainNormal()
	verifyDrainTimeout()
}
