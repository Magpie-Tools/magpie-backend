package releases

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/log"
)

const (
	githubReleasesURL = "https://api.github.com/repos/Kuucheen/magpie/releases"
	cacheTTL          = 15 * time.Minute
)

// Release represents a GitHub release entry returned to the frontend.
type Release struct {
	ID          int64     `json:"id"`
	TagName     string    `json:"tagName"`
	Title       string    `json:"title"`
	Body        string    `json:"body"`
	HTMLURL     string    `json:"htmlUrl"`
	PublishedAt time.Time `json:"publishedAt"`
	Prerelease  bool      `json:"prerelease"`
}

type releaseCache struct {
	mu      sync.Mutex
	etag    string
	fetched time.Time
	entries []Release
}

var cache releaseCache

// Get returns the latest GitHub releases, reusing a short-lived in-memory cache
// to avoid hammering the GitHub API (and its rate limits).
func Get(ctx context.Context) ([]Release, error) {
	if entries, ok := cache.loadFresh(); ok {
		return entries, nil
	}
	return cache.refresh(ctx)
}

func (c *releaseCache) loadFresh() ([]Release, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) == 0 {
		return nil, false
	}
	if time.Since(c.fetched) >= cacheTTL {
		return nil, false
	}
	return cloneReleases(c.entries), true
}

func (c *releaseCache) refresh(ctx context.Context) ([]Release, error) {
	c.mu.Lock()
	prevEntries := c.entries
	prevETag := c.etag
	c.mu.Unlock()

	entries, etag, notModified, err := fetchFromGitHub(ctx, prevETag)
	if err != nil {
		if len(prevEntries) > 0 {
			log.Warn("GitHub releases fetch failed; serving cached copy", "error", err)
			return cloneReleases(prevEntries), nil
		}
		return nil, err
	}

	if notModified {
		c.mu.Lock()
		c.fetched = time.Now()
		c.mu.Unlock()
		return cloneReleases(prevEntries), nil
	}

	now := time.Now()

	c.mu.Lock()
	c.entries = entries
	c.fetched = now
	if etag != "" {
		c.etag = etag
	}
	c.mu.Unlock()

	return cloneReleases(entries), nil
}

func fetchFromGitHub(ctx context.Context, etag string) ([]Release, string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubReleasesURL, nil)
	if err != nil {
		return nil, "", false, fmt.Errorf("build releases request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "magpie-backend")

	if token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	client := http.Client{
		Timeout: 12 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", false, fmt.Errorf("github releases request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		return nil, resp.Header.Get("ETag"), true, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, "", false, fmt.Errorf("github releases %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var payload []githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, "", false, fmt.Errorf("decode github releases: %w", err)
	}

	releases := make([]Release, 0, len(payload))
	for _, r := range payload {
		if r.Draft {
			continue
		}

		published := firstNonZero(r.PublishedAt, r.CreatedAt)

		releases = append(releases, Release{
			ID:          r.ID,
			TagName:     strings.TrimSpace(r.TagName),
			Title:       strings.TrimSpace(r.Name),
			Body:        r.Body,
			HTMLURL:     r.HTMLURL,
			PublishedAt: published,
			Prerelease:  r.Prerelease,
		})
	}

	return releases, resp.Header.Get("ETag"), false, nil
}

type githubRelease struct {
	ID          int64      `json:"id"`
	TagName     string     `json:"tag_name"`
	Name        string     `json:"name"`
	Body        string     `json:"body"`
	HTMLURL     string     `json:"html_url"`
	PublishedAt *time.Time `json:"published_at"`
	CreatedAt   *time.Time `json:"created_at"`
	Draft       bool       `json:"draft"`
	Prerelease  bool       `json:"prerelease"`
}

func firstNonZero(times ...*time.Time) time.Time {
	for _, t := range times {
		if t != nil && !t.IsZero() {
			return *t
		}
	}
	return time.Time{}
}

func cloneReleases(in []Release) []Release {
	if len(in) == 0 {
		return nil
	}
	out := make([]Release, len(in))
	copy(out, in)
	return out
}
