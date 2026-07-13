// Package cpln is a minimal Control Plane API client for the one operation
// the proxy needs: flipping a workload's suspend flag.
//
// Requests go to $CPLN_ENDPOINT (the attested path — workload tokens arrive
// as anonymous anywhere else) with a raw Authorization header (no "Bearer").
// Validated on-platform 2026-07-13; see docs/DESIGN.md.
package cpln

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	HTTP     *http.Client
	Endpoint string // e.g. http://api.cpln.io (from CPLN_ENDPOINT)
	Org      string
	GVC      string // target workload's GVC
	Workload string // target workload name
	Token    string // raw CPLN_TOKEN
	Logger   *slog.Logger

	// Retries for transient failures (network errors, 5xx). 4xx never retries.
	Attempts int           // default 3
	Backoff  time.Duration // base backoff between attempts, default 500ms
}

// SetSuspend PATCHes the target workload's suspend flag. The body is a deep
// merge, so nothing else about the workload is touched.
func (c *Client) SetSuspend(ctx context.Context, suspend bool) error {
	url := fmt.Sprintf("%s/org/%s/gvc/%s/workload/%s", c.Endpoint, c.Org, c.GVC, c.Workload)
	body := fmt.Sprintf(`{"spec":{"defaultOptions":{"suspend":%t}}}`, suspend)

	attempts := c.Attempts
	if attempts <= 0 {
		attempts = 3
	}
	backoff := c.Backoff
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	httpc := c.HTTP
	if httpc == nil {
		httpc = &http.Client{Timeout: 15 * time.Second}
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff * time.Duration(attempt-1)):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, strings.NewReader(body))
		if err != nil {
			return err
		}
		// Raw token, not "Bearer <token>" — the documented in-workload form.
		req.Header.Set("Authorization", c.Token)
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpc.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("cpln api: %w", err)
			c.logRetry(attempt, attempts, lastErr)
			continue
		}
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusOK:
			return nil
		case resp.StatusCode >= 400 && resp.StatusCode < 500:
			// Permission/validation problems don't heal by retrying.
			return fmt.Errorf("cpln api: PATCH %s returned %d: %s", url, resp.StatusCode, strings.TrimSpace(string(snippet)))
		default:
			lastErr = fmt.Errorf("cpln api: PATCH %s returned %d: %s", url, resp.StatusCode, strings.TrimSpace(string(snippet)))
			c.logRetry(attempt, attempts, lastErr)
		}
	}
	return lastErr
}

func (c *Client) logRetry(attempt, attempts int, err error) {
	if c.Logger != nil && attempt < attempts {
		c.Logger.Warn("cpln api call failed, retrying", "attempt", attempt, "error", err.Error())
	}
}
