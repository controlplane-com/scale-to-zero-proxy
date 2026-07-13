package activator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAPI simulates the Control Plane API: SetSuspend(false) starts a real
// TCP echo listener on a pre-reserved port; SetSuspend(true) stops it.
type fakeAPI struct {
	t    *testing.T
	port int

	mu           sync.Mutex
	ln           net.Listener
	wakeCalls    atomic.Int32
	suspendCalls atomic.Int32
	failWakes    bool // SetSuspend(false) succeeds but backend never listens
	suspended    chan struct{}
}

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	// Reserve a port, then free it so the "suspended" state is a real
	// connection-refused until wake starts the listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return &fakeAPI{t: t, port: port, suspended: make(chan struct{}, 8)}
}

func (f *fakeAPI) SetSuspend(_ context.Context, suspend bool) error {
	if suspend {
		f.suspendCalls.Add(1)
		f.stop()
		select {
		case f.suspended <- struct{}{}:
		default:
		}
		return nil
	}
	f.wakeCalls.Add(1)
	if f.failWakes {
		return nil // API accepts, but the backend never comes up
	}
	return f.start()
}

func (f *fakeAPI) start() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ln != nil {
		return nil
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", f.port))
	if err != nil {
		return err
	}
	f.ln = ln
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				io.Copy(c, c) //nolint:errcheck // echo until client half-closes
				c.Close()     // a real server closes when done; the splice relies on EOF
			}(c)
		}
	}()
	return nil
}

func (f *fakeAPI) stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ln != nil {
		f.ln.Close()
		f.ln = nil
	}
}

func testOptions(port int) Options {
	return Options{
		TargetHost:       "127.0.0.1",
		IdleHold:         150 * time.Millisecond,
		MaxHold:          2 * time.Second,
		WakePollInterval: 10 * time.Millisecond,
		WakeTimeout:      2 * time.Second,
		DialTimeout:      200 * time.Millisecond,
		SuspendTimeout:   time.Second,
		Logger:           slog.New(slog.DiscardHandler),
	}
}

// runClient pushes a payload through the proxy path and asserts the echo.
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
	if _, err := clientSide.Write([]byte(payload)); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(clientSide, buf); err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if string(buf) != payload {
		return fmt.Errorf("echo mismatch: got %q want %q", buf, payload)
	}
	return nil
}

func TestWakeSpliceAndSingleflight(t *testing.T) {
	fake := newFakeAPI(t)
	a := New(fake, testOptions(fake.port))
	defer a.Close()

	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			errs <- runClient(t, a, fake.port, fmt.Sprintf("hello-%d", i))
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
	a := New(fake, testOptions(fake.port))
	defer a.Close()

	if err := runClient(t, a, fake.port, "ping"); err != nil {
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
	opts := testOptions(fake.port)
	opts.IdleHold = 300 * time.Millisecond
	a := New(fake, opts)
	defer a.Close()

	if err := runClient(t, a, fake.port, "first"); err != nil {
		t.Fatal(err)
	}
	// Reconnect well inside the idle window, then hold the connection open
	// past where the original timer would have fired.
	clientSide, proxySide := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); a.HandleConn(context.Background(), proxySide, fake.port) }()
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
	opts := testOptions(fake.port)
	opts.MaxHold = 300 * time.Millisecond
	opts.WakeTimeout = 10 * time.Second // wake would keep polling; MaxHold must cut the client loose
	a := New(fake, opts)
	defer a.Close()

	clientSide, proxySide := net.Pipe()
	done := make(chan struct{})
	start := time.Now()
	go func() { defer close(done); a.HandleConn(context.Background(), proxySide, fake.port) }()

	// The proxy should close our connection once MaxHold expires.
	clientSide.SetDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
	buf := make([]byte, 1)
	_, err := clientSide.Read(buf)
	if err == nil {
		t.Fatal("expected the held connection to be closed")
	}
	<-done
	if held := time.Since(start); held > 2*time.Second {
		t.Fatalf("connection held too long: %s (MaxHold was 300ms)", held)
	}
}

func TestFastPathSkipsWake(t *testing.T) {
	fake := newFakeAPI(t)
	if err := fake.start(); err != nil { // target already running
		t.Fatal(err)
	}
	a := New(fake, testOptions(fake.port))
	defer a.Close()

	if err := runClient(t, a, fake.port, "already-up"); err != nil {
		t.Fatal(err)
	}
	if got := fake.wakeCalls.Load(); got != 0 {
		t.Fatalf("expected no wake calls when target is already up, got %d", got)
	}
}

func TestStatsReflectDemand(t *testing.T) {
	fake := newFakeAPI(t)
	if err := fake.start(); err != nil {
		t.Fatal(err)
	}
	a := New(fake, testOptions(fake.port))
	defer a.Close()

	clientSide, proxySide := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); a.HandleConn(context.Background(), proxySide, fake.port) }()

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
