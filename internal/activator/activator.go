// Package activator implements the connection-holding state machine:
// hold → wake → splice → idle-down. See docs/DESIGN.md.
package activator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"
)

// API is the minimal Control Plane surface the activator needs.
type API interface {
	SetSuspend(ctx context.Context, suspend bool) error
}

type Options struct {
	// TargetHost is the DNS name (or IP) of the target workload's internal endpoint.
	TargetHost string
	// IdleHold is how long the target stays awake after the last connection closes.
	IdleHold time.Duration
	// MaxHold bounds how long a single connection may wait for the target to
	// become ready before being dropped (dead clients must not pin wakes).
	MaxHold time.Duration
	// WakePollInterval is the cadence of readiness dials during a wake.
	WakePollInterval time.Duration
	// WakeTimeout bounds a single wake operation (API call + readiness polling).
	WakeTimeout time.Duration
	// DialTimeout bounds individual TCP dials to the target.
	DialTimeout time.Duration
	// SuspendTimeout bounds the suspend API call issued by the idle timer.
	SuspendTimeout time.Duration

	Logger *slog.Logger
}

func (o *Options) fillDefaults() {
	if o.DialTimeout <= 0 {
		o.DialTimeout = 1 * time.Second
	}
	if o.WakePollInterval <= 0 {
		o.WakePollInterval = 500 * time.Millisecond
	}
	if o.WakeTimeout <= 0 {
		o.WakeTimeout = 120 * time.Second
	}
	if o.MaxHold <= 0 {
		o.MaxHold = 90 * time.Second
	}
	if o.IdleHold <= 0 {
		o.IdleHold = 5 * time.Minute
	}
	if o.SuspendTimeout <= 0 {
		o.SuspendTimeout = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

type wakeOp struct {
	done chan struct{}
	err  error
}

type Activator struct {
	api API
	opt Options

	mu        sync.Mutex
	demand    int // held + spliced connections
	idleTimer *time.Timer
	wake      *wakeOp
	closed    bool
}

// New creates an Activator and starts the initial idle countdown, so a target
// that receives no traffic after the proxy boots gets suspended.
func New(api API, opt Options) *Activator {
	opt.fillDefaults()
	a := &Activator{api: api, opt: opt}
	a.mu.Lock()
	a.scheduleIdleLocked()
	a.mu.Unlock()
	return a
}

// Stats is a point-in-time snapshot for the health endpoint.
type Stats struct {
	ActiveConnections int  `json:"activeConnections"`
	WakeInProgress    bool `json:"wakeInProgress"`
}

func (a *Activator) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Stats{ActiveConnections: a.demand, WakeInProgress: a.wake != nil}
}

// Close stops the idle timer. In-flight connections are not interrupted.
func (a *Activator) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	if a.idleTimer != nil {
		a.idleTimer.Stop()
		a.idleTimer = nil
	}
}

// HandleConn owns one client connection end to end: registers demand, ensures
// the target is awake and its port accepts, splices, and releases demand.
// It always closes client.
func (a *Activator) HandleConn(ctx context.Context, client net.Conn, targetPort int) {
	log := a.opt.Logger.With("remote", remoteAddr(client), "targetPort", targetPort)
	a.connArrived()
	defer a.connDone()
	defer client.Close()

	holdCtx, cancel := context.WithTimeout(ctx, a.opt.MaxHold)
	defer cancel()

	start := time.Now()
	backend, err := a.connectBackend(holdCtx, targetPort)
	if err != nil {
		log.Warn("dropping connection: target not ready in time", "heldFor", time.Since(start).Round(time.Millisecond).String(), "error", err.Error())
		return
	}
	defer backend.Close()

	waited := time.Since(start)
	if waited > a.opt.DialTimeout {
		log.Info("connection spliced after wake", "heldFor", waited.Round(time.Millisecond).String())
	}
	splice(client, backend)
}

// connectBackend returns an established connection to the target port,
// waking the target if necessary.
func (a *Activator) connectBackend(ctx context.Context, targetPort int) (net.Conn, error) {
	addr := net.JoinHostPort(a.opt.TargetHost, strconv.Itoa(targetPort))

	// Fast path: target already up.
	if conn, err := a.dial(ctx, addr); err == nil {
		return conn, nil
	}

	// Slow path: trigger (or join) a wake, then dial for real.
	if err := a.ensureAwake(ctx, addr); err != nil {
		return nil, err
	}
	return a.dial(ctx, addr)
}

func (a *Activator) dial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: a.opt.DialTimeout}
	return d.DialContext(ctx, "tcp", addr)
}

// ensureAwake joins the in-flight wake operation, starting one if none exists
// (singleflight: N concurrent connections cause exactly one wake).
func (a *Activator) ensureAwake(ctx context.Context, addr string) error {
	a.mu.Lock()
	w := a.wake
	if w == nil {
		w = &wakeOp{done: make(chan struct{})}
		a.wake = w
		go a.runWake(w, addr)
	}
	a.mu.Unlock()

	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		return fmt.Errorf("gave up waiting for wake: %w", ctx.Err())
	}
}

func (a *Activator) runWake(w *wakeOp, addr string) {
	log := a.opt.Logger
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), a.opt.WakeTimeout)
	defer cancel()

	log.Info("waking target", "addr", addr)
	err := a.api.SetSuspend(ctx, false)
	if err == nil {
		err = a.waitReady(ctx, addr)
	}

	if err != nil {
		log.Error("wake failed", "error", err.Error(), "after", time.Since(start).Round(time.Millisecond).String())
	} else {
		log.Info("target ready", "wakeTook", time.Since(start).Round(time.Millisecond).String())
	}

	w.err = err
	a.mu.Lock()
	a.wake = nil
	a.mu.Unlock()
	close(w.done)
}

// waitReady polls the target port until a TCP dial succeeds. A successful dial
// is the readiness signal — the application itself is listening, not just the
// platform reporting the workload healthy.
func (a *Activator) waitReady(ctx context.Context, addr string) error {
	ticker := time.NewTicker(a.opt.WakePollInterval)
	defer ticker.Stop()
	for {
		conn, err := a.dial(ctx, addr)
		if err == nil {
			conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("target %s never became ready: %w", addr, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (a *Activator) connArrived() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.demand++
	if a.idleTimer != nil {
		a.idleTimer.Stop()
		a.idleTimer = nil
	}
}

func (a *Activator) connDone() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.demand--
	if a.demand == 0 && !a.closed {
		a.scheduleIdleLocked()
	}
}

func (a *Activator) scheduleIdleLocked() {
	if a.idleTimer != nil {
		a.idleTimer.Stop()
	}
	a.idleTimer = time.AfterFunc(a.opt.IdleHold, a.idleFired)
}

// idleFired suspends the target if the proxy is still idle when the hold
// window expires. On failure it reschedules, so a transient API error only
// delays the suspend.
func (a *Activator) idleFired() {
	a.mu.Lock()
	if a.demand != 0 || a.wake != nil || a.closed {
		a.mu.Unlock()
		return
	}
	a.idleTimer = nil
	a.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), a.opt.SuspendTimeout)
	defer cancel()
	if err := a.api.SetSuspend(ctx, true); err != nil {
		a.opt.Logger.Warn("idle suspend failed, will retry after another idle-hold", "error", err.Error())
		a.mu.Lock()
		if a.demand == 0 && !a.closed {
			a.scheduleIdleLocked()
		}
		a.mu.Unlock()
		return
	}
	a.opt.Logger.Info("target suspended after idle-hold", "idleHold", a.opt.IdleHold.String())
}

// splice copies bytes in both directions until both sides are done,
// propagating half-closes so protocols that shut down one direction first
// (and the final FINs) behave naturally.
func splice(client, backend net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(backend, client) //nolint:errcheck // best-effort byte pump
		closeWrite(backend)
	}()
	go func() {
		defer wg.Done()
		io.Copy(client, backend) //nolint:errcheck
		closeWrite(client)
	}()
	wg.Wait()
}

// closeWrite half-closes when possible so the peer sees EOF while the other
// direction keeps flowing; falls back to a full close otherwise.
func closeWrite(c net.Conn) {
	type closeWriter interface{ CloseWrite() error }
	if cw, ok := c.(closeWriter); ok {
		cw.CloseWrite() //nolint:errcheck
		return
	}
	c.Close()
}

func remoteAddr(c net.Conn) string {
	if c.RemoteAddr() != nil {
		return c.RemoteAddr().String()
	}
	return "unknown"
}
