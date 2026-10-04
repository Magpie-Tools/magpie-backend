package graphql

import (
	"testing"
)

func graphQLCheckerInput() map[string]any {

	return map[string]any{"defaults": map[string]any{"protocols": []any{"http"}, "transport": "tcp", "timeout": 1000, "retries": 0}, "rules": []any{map[string]any{"tagId": "7", "mode": "add", "protocols": []any{"socks5"}, "retries": 0}}}

}
func TestCheckerSettingsGraphQLPreservesInheritanceAndZero(t *testing.T) {
	value, err := parseCheckerSettings(graphQLCheckerInput())
	if err != nil {
		t.Fatal(err)
	}
	fields := value.Rules[0]
	if fields.Timeout != nil || fields.Transport != nil || fields.Retries == nil || *fields.Retries != 0 {
		t.Fatal(fields)
	}
	output := checkerSettingsOutput(value)
	rule := output["rules"].([]map[string]any)[0]
	protocol := rule
	if protocol["retries"] != 0 || protocol["timeout"] != nil {
		t.Fatal(protocol)
	}
}
func TestCheckerSettingsGraphQLRejectsRangeBeforeNarrowing(t *testing.T) {
	for _, field := range []string{"timeout", "retries"} {
		for _, invalid := range []int{-1, 65536} {
			t.Run(field, func(t *testing.T) {
				raw := graphQLCheckerInput()
				raw["defaults"].(map[string]any)[field] = invalid
				if _, err := parseCheckerSettings(raw); err == nil {
					t.Fatal("out-of-range integer accepted")
				}
			})
		}
	}
}
