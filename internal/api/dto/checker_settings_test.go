package dto

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCheckerProfilesRoundTrip(t *testing.T) {
	settings := CheckerSettings{Defaults: CheckerProfileSettings{Protocols: []string{"http", "socks5"}, Transport: "tcp", Timeout: 1500, Retries: 2}, Rules: []TagCheckerRule{{TagID: 9, Mode: "add", Protocols: []string{}, Retries: new(uint8(0))}}}
	data, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	var decoded CheckerSettings
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(settings, decoded) {
		t.Fatal(decoded)
	}
}
func TestEarlierProtocolDraftReadsAsSharedProfiles(t *testing.T) {
	data := []byte(`{"defaults":{"http":{"enabled":false,"transport":"tcp","timeout":7000,"retries":2},"https":{"enabled":true,"transport":"tcp","timeout":3000,"retries":0},"socks5":{"enabled":true,"transport":"tcp","timeout":7500,"retries":2}},"rules":[{"tag_id":9,"mode":"add","protocols":{"https":{"timeout":1200},"http":{"timeout":1000,"retries":0}}}]}`)
	var settings CheckerSettings
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatal(err)
	}
	if settings.Defaults.Timeout != 3000 || settings.Defaults.Retries != 0 || !reflect.DeepEqual(settings.Defaults.Protocols, []string{"https", "socks5"}) {
		t.Fatal(settings)
	}
	rule := settings.Rules[0]
	if rule.Timeout == nil || *rule.Timeout != 1000 || rule.Retries == nil || *rule.Retries != 0 || rule.Transport != nil || !reflect.DeepEqual(rule.Protocols, []string{"http", "https"}) {
		t.Fatal(rule)
	}
}
