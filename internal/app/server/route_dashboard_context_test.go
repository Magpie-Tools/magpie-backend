package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"magpie/internal/database"
)

func TestDashboardHandlerPassesCanceledRequestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx = context.WithValue(ctx, workspaceContextKey{}, database.WorkspaceAccess{WorkspaceID: 1})
	request := httptest.NewRequest(http.MethodGet, "/api/getDashboardInfo", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	getDashboardInfo(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("canceled request returned successful empty dashboard: status=%d body=%s", response.Code, response.Body.String())
	}
}
