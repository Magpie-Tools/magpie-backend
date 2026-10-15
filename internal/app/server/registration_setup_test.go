package server

import (
	"testing"

	"magpie/internal/config"
)

func TestLocalSetupRegistrationDefaultsDoNotEnableProductionBootstrap(t *testing.T) {
	previous := config.InProductionMode
	t.Cleanup(func() { config.SetProductionMode(previous) })
	t.Setenv(envAllowInsecureRegistrationDefaults, "true")
	t.Setenv(envDisablePublicRegistration, "")
	t.Setenv(envEnablePublicFirstAdminBootstrap, "")
	config.SetProductionMode(false)
	policy := resolveUserRegistrationPolicy()
	if policy.DisablePublicRegistration || policy.DisablePublicFirstAdminBootstrap {
		t.Fatal("local Compose setup cannot create its first account")
	}
	config.SetProductionMode(true)
	policy = resolveUserRegistrationPolicy()
	if !policy.DisablePublicRegistration || !policy.DisablePublicFirstAdminBootstrap {
		t.Fatal("local setup switch enabled registration or bootstrap in production")
	}
	config.SetProductionMode(false)
	t.Setenv(envEnablePublicFirstAdminBootstrap, "false")
	if !resolveUserRegistrationPolicy().DisablePublicFirstAdminBootstrap {
		t.Fatal("local setup ignored an explicit bootstrap restriction")
	}
}
