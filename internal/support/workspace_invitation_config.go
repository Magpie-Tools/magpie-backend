package support

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	envWorkspaceInvitationTTLHours = "WORKSPACE_INVITATION_TTL_HOURS"

	defaultWorkspaceInvitationTTL = 7 * 24 * time.Hour
	minWorkspaceInvitationTTL     = time.Hour
	maxWorkspaceInvitationTTL     = 30 * 24 * time.Hour
)

type WorkspaceInvitationConfig struct {
	PublicAppURL string
	TTL          time.Duration
}

func ReadWorkspaceInvitationConfig() (WorkspaceInvitationConfig, error) {
	cfg := WorkspaceInvitationConfig{
		PublicAppURL: strings.TrimSpace(GetEnv(envPublicAppURL, "")),
		TTL:          defaultWorkspaceInvitationTTL,
	}

	if raw := strings.TrimSpace(GetEnv(envWorkspaceInvitationTTLHours, "")); raw != "" {
		hours, err := strconv.Atoi(raw)
		if err != nil {
			return WorkspaceInvitationConfig{}, fmt.Errorf("invalid %s value %q: must be an integer number of hours", envWorkspaceInvitationTTLHours, raw)
		}
		cfg.TTL = time.Duration(hours) * time.Hour
	}
	if cfg.TTL < minWorkspaceInvitationTTL || cfg.TTL > maxWorkspaceInvitationTTL {
		return WorkspaceInvitationConfig{}, fmt.Errorf(
			"%s out of range: got %d hours, expected %d-%d",
			envWorkspaceInvitationTTLHours,
			int(cfg.TTL/time.Hour),
			int(minWorkspaceInvitationTTL/time.Hour),
			int(maxWorkspaceInvitationTTL/time.Hour),
		)
	}
	if cfg.PublicAppURL != "" {
		if _, err := normalizePublicURL(cfg.PublicAppURL); err != nil {
			return WorkspaceInvitationConfig{}, fmt.Errorf("invalid %s value %q: %w", envPublicAppURL, cfg.PublicAppURL, err)
		}
	}
	return cfg, nil
}

func RequireWorkspaceInvitationConfigValid() error {
	cfg, err := ReadWorkspaceInvitationConfig()
	if err != nil {
		return err
	}
	emailCfg, err := ReadEmailConfig()
	if err != nil {
		return err
	}
	if emailCfg.IsConfigured() && cfg.PublicAppURL == "" {
		return fmt.Errorf("workspace invitation email configuration incomplete: set %s", envPublicAppURL)
	}
	return nil
}

func BuildWorkspaceInvitationsURL(cfg WorkspaceInvitationConfig) (string, error) {
	if cfg.PublicAppURL == "" {
		return "", fmt.Errorf("workspace invitation link cannot be built: set %s", envPublicAppURL)
	}
	base, err := normalizePublicURL(cfg.PublicAppURL)
	if err != nil {
		return "", err
	}
	cleanPath := strings.TrimSuffix(base.Path, "/")
	if cleanPath == "" {
		base.Path = "/invitations"
	} else {
		base.Path = path.Clean(cleanPath + "/invitations")
		if !strings.HasPrefix(base.Path, "/") {
			base.Path = "/" + base.Path
		}
	}
	return base.String(), nil
}
