package domain

import (
	"encoding/json"
	"magpie/internal/api/dto"
	"reflect"
	"testing"
)

func checkerDefaults() *dto.CheckerSettings {
	value := (&Workspace{Timeout: 7500, Retries: 2, SOCKS5Protocol: true}).DefaultCheckerSettings()
	return value
}

func TestCheckerRuleModesPriorityAndInheritance(t *testing.T) {
	settings := checkerDefaults()
	timeout, retries := uint16(1500), uint8(0)
	settings.Rules = []dto.TagCheckerRule{
		{TagID: 3, Mode: "add", Protocols: []string{"http"}, Retries: &retries},
		{TagID: 2, Mode: "remove", Protocols: []string{"http"}},
		{TagID: 1, Mode: "add", Protocols: []string{"http"}, Timeout: &timeout},
	}
	cases := []struct {
		name        string
		tags        map[uint64]bool
		http, socks bool
		timeout     uint16
		retries     uint8
	}{
		{"default", nil, false, true, 7500, 2},
		{"add", map[uint64]bool{1: true}, true, true, 1500, 2},
		{"remove wins", map[uint64]bool{1: true, 2: true}, false, true, 1500, 2},
		{"reenable inherits shared settings", map[uint64]bool{1: true, 2: true, 3: true}, true, true, 1500, 0},
		{"inherit previous fields", map[uint64]bool{1: true, 3: true}, true, true, 1500, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan := ResolveCheckerSettings(settings, c.tags)
			if plan[0].Enabled != c.http || plan[3].Enabled != c.socks || plan[0].Timeout != c.timeout || plan[0].Retries != c.retries || plan[3].Timeout != c.timeout || plan[3].Retries != c.retries {
				t.Fatalf("plan = %+v", plan)
			}
		})
	}
	before := settings.Defaults
	settings.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "replace", Protocols: []string{"http"}, Timeout: &timeout}}
	plan := ResolveCheckerSettings(settings, map[uint64]bool{1: true})
	if !plan[0].Enabled || plan[3].Enabled || plan[0].Timeout != 1500 {
		t.Fatal(plan)
	}
	settings.Rules[0].Protocols = []string{}
	plan = ResolveCheckerSettings(settings, map[uint64]bool{1: true})
	for _, entry := range plan {
		if entry.Enabled {
			t.Fatal("empty replace must skip checking")
		}
	}
	if !reflect.DeepEqual(before, settings.Defaults) {
		t.Fatal("resolver modified Default")
	}
}

func TestCheckerRuleAddUpdatesEnabledProtocol(t *testing.T) {
	settings := checkerDefaults()
	timeout := uint16(2500)
	settings.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "add", Protocols: []string{"socks5"}, Timeout: &timeout}}
	plan := ResolveCheckerSettings(settings, map[uint64]bool{1: true})
	if plan[3].Timeout != 2500 || plan[3].Retries != 2 || plan[3].Transport != "tcp" {
		t.Fatal(plan)
	}
	settings.Defaults = dto.CheckerProfileSettings{Protocols: []string{"socks5"}, Timeout: 5000, Retries: 4, Transport: "tcp"}
	plan = ResolveCheckerSettings(settings, map[uint64]bool{1: true})
	if plan[3].Retries != 4 || plan[3].Timeout != 2500 {
		t.Fatal(plan)
	}
}

func TestValidateCheckerSettings(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*dto.CheckerSettings)
	}{
		{"unknown default protocol", func(s *dto.CheckerSettings) { s.Defaults.Protocols = []string{"udp"} }},
		{"duplicate default protocol", func(s *dto.CheckerSettings) { s.Defaults.Protocols = []string{"http", "http"} }},
		{"zero timeout", func(s *dto.CheckerSettings) { s.Defaults.Timeout = 0 }},
		{"SOCKS QUIC", func(s *dto.CheckerSettings) { s.Defaults.Transport = "quic" }},

		{"invalid mode", func(s *dto.CheckerSettings) { s.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "multiply"}} }},
		{"duplicate tag", func(s *dto.CheckerSettings) {
			s.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "add"}, {TagID: 1, Mode: "remove"}}
		}},
		{"unknown protocol", func(s *dto.CheckerSettings) {
			s.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "add", Protocols: []string{"udp"}}}
		}},
		{"remove with fields", func(s *dto.CheckerSettings) {
			s.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "remove", Protocols: []string{"http"}, Retries: new(uint8(0))}}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := checkerDefaults()
			c.mutate(s)
			if ValidateCheckerSettings(s) == nil {
				t.Fatal("accepted invalid settings")
			}
		})
	}
	s := checkerDefaults()
	s.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "add", Protocols: []string{"http"}, Retries: new(uint8(0))}}
	if err := ValidateCheckerSettings(s); err != nil {
		t.Fatal(err)
	}
}

func TestSharedTransportAndInheritedBudget(t *testing.T) {
	s := checkerDefaults()
	s.Defaults.Protocols = []string{"http", "https"}
	quic, timeout := "quic", uint16(1000)
	s.Rules = []dto.TagCheckerRule{{TagID: 1, Mode: "add", Protocols: []string{}, Transport: &quic, Timeout: &timeout}}
	if err := ValidateCheckerSettings(s); err != nil {
		t.Fatal(err)
	}
	plan := ResolveCheckerSettings(s, map[uint64]bool{1: true})
	for _, i := range []int{0, 1} {
		if !plan[i].Enabled || plan[i].Transport != "quic" || plan[i].Timeout != 1000 || plan[i].Retries != 2 {
			t.Fatal(plan)
		}
	}
}
func TestValidationChecksTagCombinationsRatherThanOnlyIndividualRules(t *testing.T) {
	s := checkerDefaults()
	s.Defaults.Protocols = []string{"http"}
	quic := "quic"
	s.Rules = []dto.TagCheckerRule{
		{TagID: 2, Mode: "add", Protocols: []string{}, Transport: &quic},
		{TagID: 1, Mode: "add", Protocols: []string{"socks5"}},
	}
	if ValidateCheckerSettings(s) == nil {
		t.Fatal("two individually valid tags can select SOCKS with QUIC")
	}
	s.Rules[0].Mode = "replace"
	s.Rules[0].Protocols = []string{"http"}
	if err := ValidateCheckerSettings(s); err != nil {
		t.Fatal("replace removes SOCKS before changing transport", err)
	}
}

func TestEarlierMixedTransportsConvertToSupportedSharedSettings(t *testing.T) {
	var settings dto.CheckerSettings
	data := []byte(`{"defaults":{"http":{"enabled":true,"transport":"quic","timeout":2000,"retries":1},"socks5":{"enabled":true,"transport":"tcp","timeout":7500,"retries":2}},"rules":[]}`)
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatal(err)
	}
	workspace := Workspace{CheckerConfig: &settings}
	compatible := workspace.DefaultCheckerSettings()
	if err := ValidateCheckerSettings(compatible); err != nil {
		t.Fatal(err)
	}
	plan := ResolveCheckerSettings(compatible, nil)
	if plan[0].Transport != "tcp" || plan[3].Transport != "tcp" || plan[0].Timeout != 2000 || plan[3].Timeout != 2000 {
		t.Fatal(plan)
	}
	if settings.Defaults.Transport != "quic" {
		t.Fatal("conversion mutated the stored draft")
	}
}
