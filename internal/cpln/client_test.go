package cpln

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetSuspendSendsDocumentedRequestShape(t *testing.T) {
	var gotAuth, gotPath, gotBody, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{Endpoint: srv.URL, Org: "o", GVC: "g", Workload: "w", Token: "w.raw-token"}
	if err := c.SetSuspend(context.Background(), true); err != nil {
		t.Fatal(err)
	}

	if gotMethod != http.MethodPatch {
		t.Errorf("method = %s", gotMethod)
	}
	if gotPath != "/org/o/gvc/g/workload/w" {
		t.Errorf("path = %s", gotPath)
	}
	// Raw token — the platform rejects "Bearer <workload-token>" as anonymous.
	if gotAuth != "w.raw-token" {
		t.Errorf("Authorization = %q, want the raw token with no Bearer prefix", gotAuth)
	}
	if gotBody != `{"spec":{"defaultOptions":{"suspend":true}}}` {
		t.Errorf("body = %s", gotBody)
	}
}

func TestSetSuspendRetriesOn5xxButNotOn4xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{Endpoint: srv.URL, Org: "o", GVC: "g", Workload: "w", Token: "t",
		Backoff: time.Millisecond, Logger: slog.New(slog.DiscardHandler)}
	if err := c.SetSuspend(context.Background(), false); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}

	// 4xx must fail immediately: permission errors don't heal by retrying.
	var denyCalls atomic.Int32
	deny := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		denyCalls.Add(1)
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"message":"not granted"}`)) //nolint:errcheck
	}))
	defer deny.Close()

	c2 := &Client{Endpoint: deny.URL, Org: "o", GVC: "g", Workload: "w", Token: "t", Backoff: time.Millisecond}
	if err := c2.SetSuspend(context.Background(), false); err == nil {
		t.Fatal("expected error on 403")
	}
	if denyCalls.Load() != 1 {
		t.Errorf("403 was retried %d times; must not retry 4xx", denyCalls.Load())
	}
}
