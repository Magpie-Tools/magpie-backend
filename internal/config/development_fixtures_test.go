package config

import "testing"

func TestDevelopmentFixturesEnabled(t *testing.T) {
	previous := InProductionMode
	t.Cleanup(func() { InProductionMode = previous })
	for _, key := range []string{envAppEnvironment, envRuntimeEnvironment, envGoEnvironment, envMagpieEnvironment} {
		t.Setenv(key, "")
	}
	for _, test := range []struct {
		name       string
		value      string
		production bool
		appEnv     string
		want       bool
		wantError  bool
	}{
		{name: "default keeps jobs running"},
		{name: "explicitly disabled", value: "false"},
		{name: "enabled for local development", value: "true", want: true},
		{name: "production flag rejected", value: "true", production: true, wantError: true},
		{name: "production environment rejected", value: "true", appEnv: "production", wantError: true},
		{name: "normal production allowed", value: "false", production: true},
		{name: "invalid value rejected", value: "perhaps", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			InProductionMode = test.production
			t.Setenv("DEVELOPMENT_FIXTURES", test.value)
			t.Setenv(envAppEnvironment, test.appEnv)
			got, err := DevelopmentFixturesEnabled()
			if got != test.want || (err != nil) != test.wantError {
				t.Fatalf("enabled=%t error=%v", got, err)
			}
		})
	}
}
