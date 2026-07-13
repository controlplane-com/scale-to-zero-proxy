package config

import (
	"strings"
	"testing"
	"time"
)

func env(overrides map[string]string) func(string) string {
	base := map[string]string{
		"TARGET_WORKLOAD": "sftpgo",
		"PORT_MAPPINGS":   "2022:2022",
		"CPLN_ENDPOINT":   "http://api.cpln.io",
		"CPLN_ORG":        "my-org",
		"CPLN_GVC":        "my-gvc",
		"CPLN_TOKEN":      "w.token",
	}
	for k, v := range overrides {
		base[k] = v
	}
	return func(k string) string { return base[k] }
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.TargetGVC != "my-gvc" {
		t.Errorf("TargetGVC = %q, want own gvc", c.TargetGVC)
	}
	if c.TargetHost != "sftpgo.my-gvc.cpln.local" {
		t.Errorf("TargetHost = %q", c.TargetHost)
	}
	if c.IdleHold != 5*time.Minute || c.MaxHold != 90*time.Second {
		t.Errorf("unexpected duration defaults: %s %s", c.IdleHold, c.MaxHold)
	}
	if len(c.Mappings) != 1 || c.Mappings[0] != (PortMapping{Front: 2022, Target: 2022}) {
		t.Errorf("mappings = %+v", c.Mappings)
	}
	if c.HealthPort != 8081 {
		t.Errorf("HealthPort = %d", c.HealthPort)
	}
}

func TestLoadMultipleAndBareMappings(t *testing.T) {
	c, err := Load(env(map[string]string{"PORT_MAPPINGS": "2022:2023, 9000"}))
	if err != nil {
		t.Fatal(err)
	}
	want := []PortMapping{{2022, 2023}, {9000, 9000}}
	for i, m := range want {
		if c.Mappings[i] != m {
			t.Errorf("mapping[%d] = %+v, want %+v", i, c.Mappings[i], m)
		}
	}
}

func TestLoadErrors(t *testing.T) {
	cases := []struct {
		name      string
		overrides map[string]string
		wantIn    string
	}{
		{"missing workload", map[string]string{"TARGET_WORKLOAD": ""}, "TARGET_WORKLOAD"},
		{"missing mappings", map[string]string{"PORT_MAPPINGS": ""}, "PORT_MAPPINGS"},
		{"bad port", map[string]string{"PORT_MAPPINGS": "abc:2022"}, "invalid port"},
		{"duplicate front", map[string]string{"PORT_MAPPINGS": "2022:1,2022:2"}, "duplicate"},
		{"missing token", map[string]string{"CPLN_TOKEN": ""}, "CPLN_TOKEN"},
		{"bad duration", map[string]string{"IDLE_HOLD": "soon"}, "IDLE_HOLD"},
		{"negative duration", map[string]string{"MAX_HOLD": "-5s"}, "MAX_HOLD"},
		{"health collision", map[string]string{"HEALTH_PORT": "2022"}, "collides"},
		{"no gvc anywhere", map[string]string{"CPLN_GVC": "", "TARGET_GVC": ""}, "TARGET_GVC"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(env(tc.overrides))
			if err == nil || !strings.Contains(err.Error(), tc.wantIn) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantIn)
			}
		})
	}
}

func TestTargetHostOverride(t *testing.T) {
	c, err := Load(env(map[string]string{"TARGET_HOST": "custom.internal", "TARGET_GVC": "other-gvc"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.TargetHost != "custom.internal" || c.TargetGVC != "other-gvc" {
		t.Errorf("got host=%q gvc=%q", c.TargetHost, c.TargetGVC)
	}
}
