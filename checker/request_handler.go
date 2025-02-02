package checker

import (
	"io"
	"magpie/helper"
	"magpie/models"
	"magpie/settings"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// ProxyCheckRequest makes a request to the provided siteUrl with the provided proxy
func ProxyCheckRequest(proxyToCheck models.Proxy, judge *models.Judge, protocol string) (string, error) {
	transport, err := helper.CreateTransport(proxyToCheck, judge, protocol)
	if err != nil {
		return "Failed to create transport", err
	}
	defer transport.CloseIdleConnections() // Release resources immediately

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(settings.GetConfig().Checker.Timeout) * time.Millisecond,
	}

	req, err := http.NewRequest("GET", judge.GetFullString(), nil)
	if err != nil {
		return "Error creating request", err
	}
	req.Header.Set("Connection", "close")

	resp, err := client.Do(req)
	if err != nil {
		return "Request failed", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "Error reading body", err
	}

	html := string(body)
	if !CheckForValidResponse(html, judge.GetRegex()) {
		return "Invalid response", nil
	}

	return html, nil
}

func CheckForValidResponse(html string, regex string) bool {
	if strings.EqualFold(regex, "default") {
		html = strings.ReplaceAll(html, "_", "-")
		html = strings.ToUpper(html)

		for _, header := range settings.GetConfig().Checker.StandardHeader {
			if !strings.Contains(html, header) {

				return false
			}
		}

		return true
	}

	re, err := regexp.Compile(regex)
	if err != nil {
		return false
	}

	return re.MatchString(html)
}

func DefaultRequest(siteName string) (string, error) {
	response, err := http.Get(siteName)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
