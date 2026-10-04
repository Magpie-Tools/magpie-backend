package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	gql "github.com/graphql-go/graphql"

	"magpie/internal/config"
	"magpie/internal/database"
	"magpie/internal/domain"
	gqlschema "magpie/internal/graphql"
	"magpie/internal/jobs/checker/judges"
)

func setupSettingsAPITest(t *testing.T) (uint, uint) {
	t.Helper()
	setupUserRegistrationTestDB(t)
	t.Setenv("JWT_SECRET", "settings-api-test-secret")
	t.Setenv("AUTH_REVOCATION_FAIL_OPEN", "true")
	user, _ := createWorkspaceMiddlewareTestUser(t, "settings@example.test")
	workspace, err := database.CreateWorkspace(user.ID, "Shared settings")
	if err != nil {
		t.Fatal(err)
	}
	if user.ID == workspace.ID {
		t.Fatal("test requires distinct account and workspace IDs")
	}
	t.Cleanup(func() { judges.SetUserJudges(workspace.ID, nil) })
	return user.ID, workspace.ID
}

// Both entry points use the real request decoding, permission checks, and
// persistence path; GraphQL also validates the input against its schema.
func submitSettings(t *testing.T, api string, userID, workspaceID uint, input map[string]any) error {
	t.Helper()
	if api == "GraphQL" {
		schema, err := gqlschema.NewSchema()
		if err != nil {
			t.Fatal(err)
		}
		ctx := gqlschema.WithWorkspaceAccess(gqlschema.WithUserID(context.Background(), userID), workspaceID, domain.WorkspaceRoleOwner)
		result := gql.Do(gql.Params{
			Schema: schema, Context: ctx,
			RequestString:  `mutation Update($input: UpdateUserSettingsInput!) { updateUserSettings(input: $input) { timeout retries failureAction judges { url regex } scrapingSources } }`,
			VariableValues: map[string]interface{}{"input": input},
		})
		if len(result.Errors) > 0 {
			return fmt.Errorf("%v", result.Errors)
		}
		return nil
	}
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request := workspaceMiddlewareTestRequest(t, userID, strconv.FormatUint(uint64(workspaceID), 10))
	request.Method = http.MethodPost
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	withWorkspaceOperator(http.HandlerFunc(saveUserSettings)).ServeHTTP(recorder, request)
	if contentType := recorder.Result().Header.Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("settings response Content-Type = %q", contentType)
	}
	if recorder.Code != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", recorder.Code, recorder.Body.String())
	}
	return nil
}

func TestSettingsAPIsPersistAndPreserveFailureAction(t *testing.T) {
	for _, api := range []string{"REST", "GraphQL"} {
		t.Run(api, func(t *testing.T) {
			userID, workspaceID := setupSettingsAPITest(t)
			if actual := database.GetWorkspaceByID(workspaceID).FailureAction; actual != domain.FailureActionPause {
				t.Fatalf("default action = %q", actual)
			}
			name := "failureAction"
			if api == "REST" {
				name = "failure_action"
			}
			for _, action := range []string{domain.FailureActionDelete, domain.FailureActionPause} {
				if err := submitSettings(t, api, userID, workspaceID, map[string]any{name: action}); err != nil {
					t.Fatal(err)
				}
				if actual := database.GetWorkspaceByID(workspaceID).FailureAction; actual != action {
					t.Fatalf("action = %q, want %q", actual, action)
				}
				if err := submitSettings(t, api, userID, workspaceID, map[string]any{"timeout": 1234}); err != nil {
					t.Fatal(err)
				}
				if actual := database.GetWorkspaceByID(workspaceID).FailureAction; actual != action {
					t.Fatalf("omitted action reset %q to %q", action, actual)
				}
			}
			for _, invalid := range []string{"archive", "DELETE", "remove"} {
				before := database.GetWorkspaceByID(workspaceID)
				if err := submitSettings(t, api, userID, workspaceID, map[string]any{name: invalid, "timeout": 4321}); err == nil {
					t.Fatalf("accepted invalid action %q", invalid)
				}
				after := database.GetWorkspaceByID(workspaceID)
				if after.FailureAction != before.FailureAction || after.Timeout != before.Timeout {
					t.Fatal("invalid action partially saved settings")
				}
			}
		})
	}
}

func TestSettingsAPIsRefreshWorkspaceJudgeCache(t *testing.T) {
	for _, api := range []string{"REST", "GraphQL"} {
		t.Run(api, func(t *testing.T) {
			userID, workspaceID := setupSettingsAPITest(t)
			judges.AddUserJudge(workspaceID, &domain.Judge{ID: 999, FullString: "https://old.example.test"}, "old")
			input := map[string]any{"judges": []any{map[string]any{"url": "https://new.example.test", "regex": "new"}}}
			if err := submitSettings(t, api, userID, workspaceID, input); err != nil {
				t.Fatal(err)
			}
			stored := database.GetWorkspaceJudges(workspaceID)
			cached, regex := judges.GetNextJudge(workspaceID, "https")
			if len(stored) != 1 || stored[0].Url != "https://new.example.test" || cached == nil || cached.FullString != stored[0].Url || regex != "new" {
				t.Fatalf("judge update not applied: database=%+v cache=%+v regex=%q", stored, cached, regex)
			}
			if err := submitSettings(t, api, userID, workspaceID, map[string]any{"judges": []any{}}); err != nil {
				t.Fatal(err)
			}
			if cached, _ := judges.GetNextJudge(workspaceID, "https"); cached != nil {
				t.Fatal("cleared judges remain in checker cache")
			}
		})
	}
}

func TestSettingsAPIsRejectInvalidNumbersWithoutSaving(t *testing.T) {
	for _, api := range []string{"REST", "GraphQL"} {
		t.Run(api, func(t *testing.T) {
			userID, workspaceID := setupSettingsAPITest(t)
			for _, field := range []struct {
				name string
				max  int
			}{{"timeout", 65535}, {"retries", 255}, {"autoRemoveFailureThreshold", 255}} {
				for _, value := range []int{-1, field.max + 1} {
					t.Run(fmt.Sprintf("%s=%d", field.name, value), func(t *testing.T) {
						name := field.name
						if api == "REST" && name == "autoRemoveFailureThreshold" {
							name = "auto_remove_failure_threshold"
						}
						before := database.GetWorkspaceByID(workspaceID)
						if err := submitSettings(t, api, userID, workspaceID, map[string]any{name: value}); err == nil {
							t.Fatal("invalid number accepted")
						}
						after := database.GetWorkspaceByID(workspaceID)
						if before.Timeout != after.Timeout || before.Retries != after.Retries || before.AutoRemoveFailureThreshold != after.AutoRemoveFailureThreshold {
							t.Fatal("rejected input changed persisted settings")
						}
					})
				}
			}
			thresholdName := "autoRemoveFailureThreshold"
			if api == "REST" {
				thresholdName = "auto_remove_failure_threshold"
			}
			for _, values := range [][3]int{{0, 0, 0}, {65535, 255, 255}} {
				if err := submitSettings(t, api, userID, workspaceID, map[string]any{"timeout": values[0], "retries": values[1], thresholdName: values[2]}); err != nil {
					t.Fatal(err)
				}
				actual := database.GetWorkspaceByID(workspaceID)
				if int(actual.Timeout) != values[0] || int(actual.Retries) != values[1] || int(actual.AutoRemoveFailureThreshold) != values[2] {
					t.Fatal("valid boundary did not persist")
				}
			}
		})
	}
}

func TestGraphQLRejectsUnsupportedScrapingSourcesInput(t *testing.T) {
	userID, workspaceID := setupSettingsAPITest(t)
	before := database.GetWorkspaceByID(workspaceID)
	err := submitSettings(t, "GraphQL", userID, workspaceID, map[string]any{"timeout": 123, "scrapingSources": []any{"https://source.example.test"}})
	if err == nil || !strings.Contains(err.Error(), "scrapingSources") {
		t.Fatalf("expected unsupported-field error, got %v", err)
	}
	if after := database.GetWorkspaceByID(workspaceID); after.Timeout != before.Timeout {
		t.Fatal("invalid mutation partially saved")
	}
}

func TestSettingsAPIsRejectBlockedJudges(t *testing.T) {
	withTempServerWorkingDir(t)
	original := config.GetConfig()
	t.Cleanup(func() {
		if err := config.SetConfig(original); err != nil {
			t.Error(err)
		}
	})
	updated := original
	updated.WebsiteBlacklist = []string{"blocked.example.test"}
	if err := config.SetConfig(updated); err != nil {
		t.Fatal(err)
	}
	for _, api := range []string{"REST", "GraphQL"} {
		t.Run(api, func(t *testing.T) {
			userID, workspaceID := setupSettingsAPITest(t)
			before := database.GetWorkspaceByID(workspaceID)
			err := submitSettings(t, api, userID, workspaceID, map[string]any{"timeout": 123, "judges": []any{map[string]any{"url": "https://blocked.example.test/judge", "regex": "blocked"}}})
			if err == nil || !strings.Contains(err.Error(), "blocked websites") {
				t.Fatalf("expected blocked-judge error, got %v", err)
			}
			if after := database.GetWorkspaceByID(workspaceID); after.Timeout != before.Timeout {
				t.Fatal("blocked judge input partially saved")
			}
		})
	}
}

func TestSettingsAPIsPersistCheckerProfilesAndPreserveOmittedFields(t *testing.T) {
	for _, api := range []string{"REST", "GraphQL"} {
		t.Run(api, func(t *testing.T) {
			userID, workspaceID := setupSettingsAPITest(t)
			if err := database.DB.AutoMigrate(&domain.ProxyTag{}, &domain.ProxyTagAssignment{}); err != nil {
				t.Fatal(err)
			}
			tag, err := database.CreateProxyTag(workspaceID, "HTTP extra", "#22C55E")
			if err != nil {
				t.Fatal(err)
			}
			defaults := map[string]any{"protocols": []any{"socks5"}, "transport": "tcp", "timeout": 3000, "retries": 0}
			rule := map[string]any{"mode": "add", "protocols": []any{"http"}, "timeout": 1000, "retries": 0}
			var input map[string]any
			if api == "REST" {
				rule["tag_id"] = tag.ID
				input = map[string]any{"checker_settings": map[string]any{"defaults": defaults, "rules": []any{rule}}}
			} else {
				rule["tagId"] = fmt.Sprint(tag.ID)
				input = map[string]any{"checkerSettings": map[string]any{"defaults": defaults, "rules": []any{rule}}}
			}

			if err := submitSettings(t, api, userID, workspaceID, input); err != nil {
				t.Fatal(err)
			}
			before := database.GetWorkspaceByID(workspaceID)
			if before.CheckerConfig == nil || len(before.CheckerConfig.Rules) != 1 || before.CheckerConfig.Rules[0].Retries == nil {
				t.Fatal(before.CheckerConfig)
			}
			// Partial legacy settings saves keep all profile selections and overrides.
			if err := submitSettings(t, api, userID, workspaceID, map[string]any{}); err != nil {
				t.Fatal(err)
			}
			after := database.GetWorkspaceByID(workspaceID)
			encodedBefore, _ := json.Marshal(before.CheckerConfig)
			encodedAfter, _ := json.Marshal(after.CheckerConfig)
			if !bytes.Equal(encodedBefore, encodedAfter) {
				t.Fatal("omitted profiles changed", string(encodedAfter))
			}
		})
	}
}
