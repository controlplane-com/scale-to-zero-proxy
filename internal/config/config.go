// Package config loads the proxy's configuration from environment variables.
//
// Platform-injected variables (CPLN_*) come from Control Plane; everything
// else is set by the consuming template. See docs/DESIGN.md for the contract.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PortMapping maps a listener port on the proxy to a port on the target workload.
type PortMapping struct {
	Front  int
	Target int
}

type Config struct {
	// Target workload coordinates.
	TargetWorkload string // TARGET_WORKLOAD (required)
	TargetGVC      string // TARGET_GVC (defaults to CPLN_GVC)
	TargetHost     string // TARGET_HOST (defaults to {workload}.{gvc}.cpln.local)

	// Control Plane API access (platform-injected).
	Endpoint string // CPLN_ENDPOINT — the attested API path; workload tokens are ONLY honored here
	Org      string // CPLN_ORG
	Token    string // CPLN_TOKEN

	Mappings []PortMapping // PORT_MAPPINGS (required), e.g. "2022:2022" or "2022:2022,9000:9000"

	IdleHold         time.Duration // IDLE_HOLD, default 5m
	MaxHold          time.Duration // MAX_HOLD, default 90s
	WakePollInterval time.Duration // WAKE_POLL_INTERVAL, default 500ms
	WakeTimeout      time.Duration // WAKE_TIMEOUT, default 120s
	ProbeWindow      time.Duration // PROBE_WINDOW, default 2s — max wait for the server's first bytes

	HealthPort int // HEALTH_PORT, default 8081
}

// Load reads configuration via getenv (usually os.Getenv; injectable for tests).
func Load(getenv func(string) string) (Config, error) {
	var c Config

	c.TargetWorkload = getenv("TARGET_WORKLOAD")
	if c.TargetWorkload == "" {
		return c, fmt.Errorf("TARGET_WORKLOAD is required")
	}

	ownGVC := getenv("CPLN_GVC")
	c.TargetGVC = firstNonEmpty(getenv("TARGET_GVC"), ownGVC)
	if c.TargetGVC == "" {
		return c, fmt.Errorf("TARGET_GVC is required when CPLN_GVC is not injected")
	}

	c.TargetHost = firstNonEmpty(getenv("TARGET_HOST"),
		fmt.Sprintf("%s.%s.cpln.local", c.TargetWorkload, c.TargetGVC))

	c.Endpoint = strings.TrimRight(getenv("CPLN_ENDPOINT"), "/")
	c.Org = getenv("CPLN_ORG")
	c.Token = getenv("CPLN_TOKEN")
	for name, v := range map[string]string{"CPLN_ENDPOINT": c.Endpoint, "CPLN_ORG": c.Org, "CPLN_TOKEN": c.Token} {
		if v == "" {
			return c, fmt.Errorf("%s is required (platform-injected; is the workload running on Control Plane?)", name)
		}
	}

	mappings, err := parseMappings(getenv("PORT_MAPPINGS"))
	if err != nil {
		return c, err
	}
	c.Mappings = mappings

	if c.IdleHold, err = durationOr(getenv("IDLE_HOLD"), 5*time.Minute); err != nil {
		return c, fmt.Errorf("IDLE_HOLD: %w", err)
	}
	if c.MaxHold, err = durationOr(getenv("MAX_HOLD"), 90*time.Second); err != nil {
		return c, fmt.Errorf("MAX_HOLD: %w", err)
	}
	if c.WakePollInterval, err = durationOr(getenv("WAKE_POLL_INTERVAL"), 500*time.Millisecond); err != nil {
		return c, fmt.Errorf("WAKE_POLL_INTERVAL: %w", err)
	}
	if c.WakeTimeout, err = durationOr(getenv("WAKE_TIMEOUT"), 120*time.Second); err != nil {
		return c, fmt.Errorf("WAKE_TIMEOUT: %w", err)
	}
	if c.ProbeWindow, err = durationOr(getenv("PROBE_WINDOW"), 2*time.Second); err != nil {
		return c, fmt.Errorf("PROBE_WINDOW: %w", err)
	}

	if c.HealthPort, err = intOr(getenv("HEALTH_PORT"), 8081); err != nil {
		return c, fmt.Errorf("HEALTH_PORT: %w", err)
	}

	for _, m := range c.Mappings {
		if m.Front == c.HealthPort {
			return c, fmt.Errorf("HEALTH_PORT %d collides with a PORT_MAPPINGS listener port", c.HealthPort)
		}
	}

	return c, nil
}

func parseMappings(raw string) ([]PortMapping, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("PORT_MAPPINGS is required (e.g. \"2022:2022\")")
	}
	var out []PortMapping
	seen := map[int]bool{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		front, target, ok := strings.Cut(part, ":")
		if !ok {
			// A bare port maps to itself.
			target = front
		}
		f, err := parsePort(front)
		if err != nil {
			return nil, fmt.Errorf("PORT_MAPPINGS %q: %w", part, err)
		}
		t, err := parsePort(target)
		if err != nil {
			return nil, fmt.Errorf("PORT_MAPPINGS %q: %w", part, err)
		}
		if seen[f] {
			return nil, fmt.Errorf("PORT_MAPPINGS: duplicate listener port %d", f)
		}
		seen[f] = true
		out = append(out, PortMapping{Front: f, Target: t})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("PORT_MAPPINGS is required (e.g. \"2022:2022\")")
	}
	return out, nil
}

func parsePort(s string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return n, nil
}

func durationOr(raw string, def time.Duration) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("must be positive, got %s", d)
	}
	return d, nil
}

func intOr(raw string, def int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return def, nil
	}
	return strconv.Atoi(strings.TrimSpace(raw))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
