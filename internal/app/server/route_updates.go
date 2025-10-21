package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/charmbracelet/log"
)

type githubUpdateConfig struct {
	Enabled bool
	Owner   string
	Repo    string
	Branch  string
	Token   string
}

type githubCommit struct {
	SHA     string `json:"sha"`
	HTMLURL string `json:"html_url"`
	Commit  struct {
		Message string `json:"message"`
		Author  struct {
			Date string `json:"date"`
		} `json:"author"`
		Committer struct {
			Date string `json:"date"`
		} `json:"committer"`
	} `json:"commit"`
}

type updateResponse struct {
	SHA             string `json:"sha"`
	ShortSHA        string `json:"short_sha"`
	HTMLURL         string `json:"html_url,omitempty"`
	Message         string `json:"message,omitempty"`
	CommittedAt     string `json:"committed_at,omitempty"`
	CurrentSHA      string `json:"current_sha,omitempty"`
	CurrentShortSHA string `json:"current_short_sha,omitempty"`
}

func loadGitHubUpdateConfig() githubUpdateConfig {
	enabled := parseBool(os.Getenv("GITHUB_UPDATES_ENABLED"))
	owner := strings.TrimSpace(os.Getenv("GITHUB_UPDATES_OWNER"))
	repo := strings.TrimSpace(os.Getenv("GITHUB_UPDATES_REPO"))
	branch := strings.TrimSpace(os.Getenv("GITHUB_UPDATES_BRANCH"))
	if branch == "" {
		branch = "master"
	}
	token := strings.TrimSpace(os.Getenv("GITHUB_UPDATES_TOKEN"))

	if !enabled || owner == "" || repo == "" {
		enabled = false
	}

	return githubUpdateConfig{
		Enabled: enabled,
		Owner:   owner,
		Repo:    repo,
		Branch:  branch,
		Token:   token,
	}
}

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func normalizeCommitValue(raw string) string {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return ""
	}
	for _, r := range trimmed {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return ""
		}
	}
	return trimmed
}

func shortCommit(value string) string {
	if len(value) < 7 {
		return ""
	}
	if len(value) > 7 {
		return value[:7]
	}
	return value
}

func detectLocalCommit(ctx context.Context) (string, string) {
	if raw := normalizeCommitValue(os.Getenv("GITHUB_UPDATES_LOCAL_SHA")); raw != "" {
		if short := shortCommit(raw); short != "" {
			return raw, short
		}
	}

	repoPath := strings.TrimSpace(os.Getenv("GITHUB_UPDATES_REPO_PATH"))
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	if repoPath != "" {
		cmd.Dir = repoPath
	}

	output, err := cmd.Output()
	if err != nil {
		log.Debug("git rev-parse failed", "error", err)
		return "", ""
	}

	full := normalizeCommitValue(string(output))
	if full == "" {
		return "", ""
	}

	return full, shortCommit(full)
}

func getLatestUpdate(w http.ResponseWriter, r *http.Request) {
	cfg := loadGitHubUpdateConfig()
	if !cfg.Enabled {
		writeError(w, "update checks disabled", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	target := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", cfg.Owner, cfg.Repo, cfg.Branch)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		writeError(w, "failed to create request", http.StatusInternalServerError)
		return
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "magpie-update-checker")
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Warn("github request failed", "error", err)
		writeError(w, "failed to contact update source", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		log.Warn("github rejected credentials", "status", resp.StatusCode)
		writeError(w, "github authentication failed", http.StatusBadGateway)
		return
	}

	if resp.StatusCode >= 400 {
		log.Warn("github update lookup failed", "status", resp.StatusCode)
		writeError(w, "github returned error", http.StatusBadGateway)
		return
	}

	var payload githubCommit
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		log.Warn("failed to decode github response", "error", err)
		writeError(w, "invalid response from github", http.StatusBadGateway)
		return
	}

	fullSHA := normalizeCommitValue(payload.SHA)
	if fullSHA == "" {
		writeError(w, "github response missing sha", http.StatusBadGateway)
		return
	}

	committedAt := payload.Commit.Author.Date
	if committedAt == "" {
		committedAt = payload.Commit.Committer.Date
	}

	message := strings.TrimSpace(payload.Commit.Message)
	if idx := strings.IndexByte(message, '\n'); idx >= 0 {
		message = message[:idx]
	}

	shortSHA := shortCommit(fullSHA)
	if shortSHA == "" {
		writeError(w, "github response missing valid short sha", http.StatusBadGateway)
		return
	}

	localSHA, localShort := detectLocalCommit(ctx)

	writeJSON(w, http.StatusOK, updateResponse{
		SHA:             fullSHA,
		ShortSHA:        shortSHA,
		HTMLURL:         payload.HTMLURL,
		Message:         message,
		CommittedAt:     committedAt,
		CurrentSHA:      localSHA,
		CurrentShortSHA: localShort,
	})
}
