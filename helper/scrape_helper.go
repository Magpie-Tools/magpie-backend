package helper

import (
	"html"
	"net/url"
	"regexp"
	"strings"
)

func IsValidURL(raw string) bool {
	u, err := url.ParseRequestURI(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https")
}

func GetProxiesOfHTML(rawHTML string) []string {
	replacements := []struct{ old, new string }{
		// entity replacements
		{"&colon;", ":"},
		{"&nbsp;", " "},
		{"</td><td>", ":"},
		{"<td>", ":"},
		{"</td>", ":"},
		{"<th>", ":"},
		{"</th>", ":"},
		{"<tr>", ":"},
		{"</tr>", ":"},
		{" ", " "},
	}
	normalized := rawHTML
	for _, r := range replacements {
		normalized = strings.ReplaceAll(normalized, r.old, r.new)
	}

	normalized = regexp.MustCompile(`:{2,}`).ReplaceAllString(normalized, ":")

	decoded := html.UnescapeString(normalized)

	tagRe := regexp.MustCompile(`<[^>]+>`)
	text := tagRe.ReplaceAllString(decoded, "")

	proxyRe := regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}:\d{1,5}\b`)

	return proxyRe.FindAllString(text, -1)
}

func ParseTextToSources(text string) []string {
	lines := strings.Split(text, "\n")
	var sources []string

	for _, line := range lines {
		// Trim whitespace and skip empty lines
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Only add valid URLs
		if IsValidURL(line) {
			sources = append(sources, line)
		}
	}

	return sources
}
