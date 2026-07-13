package activator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAPI models the Control Plane environment accurately: the target address
// ALWAYS accepts TCP (the mesh answers even for suspended workloads — verified
// on-platform 2026-07-13). What changes with suspend state is behavior after
// accept: suspended = silent dead pipe; awake = server banner, then echo.
type fakeAPI struct {
	t  *testing.T
	ln net.Listener

	awake        atomic.Bool
	wakeCalls    atomic.Int32
	suspendCalls atomic.Int32
	failWakes    bool // API accepts the wake but the backend never comes up
	suspended    chan struct{}
}

const banner = "BANNER\n"

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeAPI{t: t, ln: ln, suspended: make(chan struct{}, 8)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

func (f *fakeAPI) port() int { return f.ln.Addr().(*net.TCPAddr).Port }

func (f *fakeAPI) serve(c net.Conn) {
	defer c.Close()
	if !f.awake.Load() {
		// Mesh-with-no-backend: connection stays open, silent, sends nothing.
		io.Copy(io.Discard, c) //nolint:errcheck
		return
	}
	// Real server-first backend: banner, then echo until client half-closes.
	if _, err := c.Write([]byte(banner)); err != nil {
		return
	}
	io.Copy(c, c) //nolint:errcheck
}

func (f *fakeAPI) SetSuspend(_ context.Context, suspend bool) error {
	if suspend {
		f.suspendCalls.Add(1)
		f.awake.Store(false)
		select {
		case f.suspended <- struct{}{}:
		default:
		}
		return nil
	}
	f.wakeCalls.Add(1)
	if !f.failWakes {
		f.awake.Store(true)
	}
	return nil
}

func testOptions() Options {
	return Options{
		TargetHost:       "127.0.0.1",
		IdleHold:         150 * time.Millisecond,
		MaxHold:          2 * time.Second,
		WakePollInterval: 10 * time.Millisecond,
		WakeTimeout:      2 * time.Second,
		DialTimeout:      200 * time.Millisecond,
		ProbeWindow:      100 * time.Millisecond,
		SuspendTimeout:   time.Second,
		Logger:           slog.New(slog.DiscardHandler),
	}
}

// runClient pushes a payload through the proxy path, expecting the server
// banner first (server-speaks-first protocol), then an echo of the payload.
func runClient(t *testing.T, a *Activator, port int, payload string) error {
	t.Helper()
	clientSide, proxySide := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.HandleConn(context.Background(), proxySide, port)
	}()
	defer func() {
		clientSide.Close()
		<-done
	}()

	clientSide.SetDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
	got := make([]byte, len(banner))
	if _, err := io.ReadFull(clientSide, got); err != nil {
		return fmt.Errorf("read banner: %w", err)
	}
	if string(got) != banner {
		return fmt.Errorf("banner mismatch: got %q", got)
	}
	if _, err := clientSide.Write([]byte(payload)); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(clientSide, buf); err != nil {
		return fmt.Errorf("read echo: %w", err)
	}
	if string(buf) != payload {
		return fmt.Errorf("echo mismatch: got %q want %q", buf, payload)
	}
	return nil
}

// The headline regression test for the mesh finding: the target address
// accepts TCP while "suspended", and the proxy must NOT be fooled — it must
// wake the target (exactly once for N concurrent connections) and only then
// splice.
func TestSilentMeshAcceptTriggersWakeAndSingleflight(t *testing.T) {
	fake := newFakeAPI(t)
	a := New(fake, testOptions())
	defer a.Close()

	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			errs <- runClient(t, a, fake.port(), fmt.Sprintf("hello-%d", i))
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
	}

	if got := fake.wakeCalls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 wake call for %d concurrent connections, got %d", n, got)
	}
}

func TestIdleSuspendAfterConnectionsClose(t *testing.T) {
	fake := newFakeAPI(t)
	a := New(fake, testOptions())
	defer a.Close()

	if err := runClient(t, a, fake.port(), "ping"); err != nil {
		t.Fatal(err)
	}

	select {
	case <-fake.suspended:
	case <-time.After(3 * time.Second):
		t.Fatal("expected an idle suspend after the connection closed")
	}
}

func TestNewConnectionCancelsIdleTimer(t *testing.T) {
	fake := newFakeAPI(t)
	opts := testOptions()
	opts.IdleHold = 300 * time.Millisecond
	a := New(fake, opts)
	defer a.Close()

	if err := runClient(t, a, fake.port(), "first"); err != nil {
		t.Fatal(err)
	}
	// Reconnect well inside the idle window, then hold the connection open
	// past where the original timer would have fired.
	clientSide, proxySide := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); a.HandleConn(context.Background(), proxySide, fake.port()) }()
	time.Sleep(500 * time.Millisecond)

	if got := fake.suspendCalls.Load(); got != 0 {
		t.Fatalf("target was suspended while a connection was active (suspend calls: %d)", got)
	}
	clientSide.Close()
	<-done

	select {
	case <-fake.suspended:
	case <-time.After(3 * time.Second):
		t.Fatal("expected idle suspend after the second connection closed")
	}
}

func TestMaxHoldDropsConnectionWhenTargetNeverReady(t *testing.T) {
	fake := newFakeAPI(t)
	fake.failWakes = true
	opts := testOptions()
	opts.MaxHold = 300 * time.Millisecond
	opts.WakeTimeout = 10 * time.Second // wake would keep polling; MaxHold must cut the client loose
	a := New(fake, opts)
	defer a.Close()

	clientSide, proxySide := net.Pipe()
	done := make(chan struct{})
	start := time.Now()
	go func() { defer close(done); a.HandleConn(context.Background(), proxySide, fake.port()) }()

	clientSide.SetDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
	buf := make([]byte, 1)
	if _, err := clientSide.Read(buf); err == nil {
		t.Fatal("expected the held connection to be closed")
	}
	<-done
	if held := time.Since(start); held > 2*time.Second {
		t.Fatalf("connection held too long: %s (MaxHold was 300ms)", held)
	}
}

func TestFastPathSkipsWakeWhenTargetSpeaks(t *testing.T) {
	fake := newFakeAPI(t)
	fake.awake.Store(true) // target genuinely up: banner flows
	a := New(fake, testOptions())
	defer a.Close()

	if err := runClient(t, a, fake.port(), "already-up"); err != nil {
		t.Fatal(err)
	}
	if got := fake.wakeCalls.Load(); got != 0 {
		t.Fatalf("expected no wake calls when target is already up, got %d", got)
	}
}

func TestStatsReflectDemand(t *testing.T) {
	fake := newFakeAPI(t)
	fake.awake.Store(true)
	a := New(fake, testOptions())
	defer a.Close()

	clientSide, proxySide := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); a.HandleConn(context.Background(), proxySide, fake.port()) }()

	deadline := time.Now().Add(2 * time.Second)
	for a.Stats().ActiveConnections != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("stats never showed the active connection: %+v", a.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	clientSide.Close()
	<-done
	if got := a.Stats().ActiveConnections; got != 0 {
		t.Fatalf("expected 0 active connections after close, got %d", got)
	}
}
