package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/charmbracelet/log"

	"magpie/internal/auth"
)

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, msg string, status int) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func enableCORS(next http.Handler) http.Handler {
	cors := resolveCORSConfig()
	allowedMethods := "GET, POST, OPTIONS, PUT, PATCH, DELETE"
	allowedHeaders := "Content-Type, Authorization, X-Request-ID, X-Observability-Token, X-Workspace-ID"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			if !cors.isAllowed(origin) && !isSameHostOrigin(origin, r) {
				log.Warn("Blocked CORS origin", "origin", origin, "request_host", r.Host)
				writeError(w, "CORS origin is not allowed", http.StatusForbidden)
				return
			}

			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", allowedMethods)
			w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
			w.Header().Set("Access-Control-Max-Age", "600")
			w.Header().Add("Vary", "Origin")
			w.Header().Add("Vary", "Access-Control-Request-Method")
			w.Header().Add("Vary", "Access-Control-Request-Headers")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func OpenRoutes(ctx context.Context, port int) error {
	if ctx == nil {
		ctx = context.Background()
	}

	router := http.NewServeMux()
	router.Handle("GET /healthz", withObservabilityProtection(http.HandlerFunc(healthz)))
	router.Handle("GET /readyz", withObservabilityProtection(http.HandlerFunc(readyz)))
	router.Handle("GET /metrics", withObservabilityProtection(metricsHandler()))

	gqlHandler, err := getGraphQLHandler()
	if err != nil {
		return fmt.Errorf("failed to initialize graphql handler: %w", err)
	}

	apiMux := http.NewServeMux()
	apiMux.Handle("/graphql", applyRequestBodyLimit(auth.RequireAuth(withWorkspaceViewer(withGraphQLGuard(gqlHandler))), resolveJSONMaxBodyBytes()))
	apiMux.Handle("POST /register", withRegisterRateLimit(http.HandlerFunc(registerUser)))
	apiMux.Handle("POST /login", withLoginRateLimit(http.HandlerFunc(loginUser)))
	apiMux.Handle("POST /forgotPassword", withForgotPasswordRateLimit(http.HandlerFunc(forgotPassword)))
	apiMux.Handle("POST /resetPassword", withResetPasswordRateLimit(http.HandlerFunc(resetPassword)))
	apiMux.Handle("POST /logout", auth.RequireAuth(http.HandlerFunc(logoutUser)))
	apiMux.Handle("POST /refreshToken", auth.RequireAuth(http.HandlerFunc(refreshToken)))
	apiMux.Handle("GET /checkLogin", auth.RequireAuth(http.HandlerFunc(checkLogin)))
	apiMux.Handle("POST /changePassword", auth.RequireAuth(http.HandlerFunc(changePassword)))
	apiMux.Handle("POST /deleteAccount", auth.RequireAuth(http.HandlerFunc(deleteAccount)))
	apiMux.Handle("POST /saveSettings", auth.IsAdmin(http.HandlerFunc(saveSettings)))
	apiMux.Handle("POST /global/proxies/requeue", auth.IsAdmin(http.HandlerFunc(requeueAllProxies)))
	apiMux.Handle("POST /global/proxies/{id}/requeue", auth.IsAdmin(http.HandlerFunc(requeueProxy)))
	apiMux.Handle("POST /global/scrapeSources/requeue", auth.IsAdmin(http.HandlerFunc(requeueAllScrapeSources)))
	apiMux.Handle("POST /global/scrapeSources/{id}/requeue", auth.IsAdmin(http.HandlerFunc(requeueScrapeSource)))
	apiMux.Handle("GET /releases", http.HandlerFunc(getReleases))
	apiMux.Handle("GET /workspaces", auth.RequireAuth(http.HandlerFunc(listWorkspaces)))
	apiMux.Handle("POST /workspaces", auth.RequireAuth(http.HandlerFunc(createWorkspace)))
	apiMux.Handle("GET /workspaces/{id}", auth.RequireAuth(http.HandlerFunc(getWorkspace)))
	apiMux.Handle("PATCH /workspaces/{id}", auth.RequireAuth(http.HandlerFunc(updateWorkspace)))
	apiMux.Handle("POST /workspaces/{id}/select", auth.RequireAuth(http.HandlerFunc(selectWorkspace)))
	apiMux.Handle("GET /workspaces/{id}/members", auth.RequireAuth(http.HandlerFunc(listWorkspaceMembers)))
	apiMux.Handle("PATCH /workspaces/{id}/members/{userId}", auth.RequireAuth(http.HandlerFunc(updateWorkspaceMember)))
	apiMux.Handle("DELETE /workspaces/{id}/members/{userId}", auth.RequireAuth(http.HandlerFunc(removeWorkspaceMember)))
	apiMux.Handle("GET /workspaces/{id}/invitations", auth.RequireAuth(http.HandlerFunc(listWorkspaceInvitations)))
	apiMux.Handle("POST /workspaces/{id}/invitations", auth.RequireAuth(http.HandlerFunc(createWorkspaceInvitation)))
	apiMux.Handle("PATCH /workspaces/{id}/invitations/{invitationId}", auth.RequireAuth(http.HandlerFunc(updateWorkspaceInvitation)))
	apiMux.Handle("DELETE /workspaces/{id}/invitations/{invitationId}", auth.RequireAuth(http.HandlerFunc(revokeWorkspaceInvitation)))
	apiMux.Handle("GET /invitations", auth.RequireAuth(http.HandlerFunc(listUserWorkspaceInvitations)))
	apiMux.Handle("POST /invitations/{invitationId}/accept", auth.RequireAuth(http.HandlerFunc(acceptWorkspaceInvitation)))
	apiMux.Handle("POST /invitations/{invitationId}/decline", auth.RequireAuth(http.HandlerFunc(declineWorkspaceInvitation)))

	apiMux.Handle("GET /getDashboardInfo", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getDashboardInfo))))

	apiMux.Handle("GET /getProxyCount", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getProxyCount))))
	apiMux.Handle("GET /getProxyPage/{page}", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getProxyPage))))
	apiMux.Handle("GET /proxyFilters", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getProxyFilters))))
	apiMux.Handle("GET /proxies/{id}/statistics", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getProxyStatistics))))
	apiMux.Handle("GET /proxies/{id}/statistics/{statisticId}", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getProxyStatisticResponseBody))))
	apiMux.Handle("GET /proxies/{id}", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getProxyDetail))))
	apiMux.Handle("PUT /proxies/{id}/lifecycle", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(updateManagedProxyLifecycle))))
	apiMux.Handle("PUT /proxies/{id}/tags", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(replaceProxyTags))))
	apiMux.Handle("POST /addProxies", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(addProxies))))
	apiMux.Handle("DELETE /proxies", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(deleteProxies))))
	apiMux.Handle("GET /proxyTags", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(listProxyTags))))
	apiMux.Handle("POST /proxyTags", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(createProxyTag))))
	apiMux.Handle("PUT /proxyTags/{id}", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(updateProxyTag))))
	apiMux.Handle("DELETE /proxyTags/{id}", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(deleteProxyTag))))

	apiMux.Handle("GET /rotatingProxies", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(listRotatingProxies))))
	apiMux.Handle("GET /rotatingProxies/instances", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(listRotatingProxyInstances))))
	apiMux.Handle("POST /rotatingProxies", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(createRotatingProxy))))
	apiMux.Handle("DELETE /rotatingProxies/{id}", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(deleteRotatingProxy))))
	apiMux.Handle("POST /rotatingProxies/{id}/next", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getNextRotatingProxy))))

	apiMux.Handle("GET /getScrapingSourcesCount", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getScrapeSourcesCount))))
	apiMux.Handle("GET /getScrapingSourcesPage/{page}", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getScrapeSourcePage))))
	apiMux.Handle("POST /scrapingSources", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(saveScrapingSources))))
	apiMux.Handle("POST /scrapingSources/export", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(exportScrapeSources))))
	apiMux.Handle("DELETE /scrapingSources", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(deleteScrapingSources))))
	apiMux.Handle("GET /scrapingSources/check", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(checkScrapeSourceRobots))))
	apiMux.Handle("GET /scrapingSources/respectRobots", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getRobotsRespectSetting))))
	apiMux.Handle("GET /scrapingSources/{id}/proxies", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getScrapeSourceProxies))))
	apiMux.Handle("PATCH /scrapingSources/{id}", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(updateScrapeSourceSettings))))
	apiMux.Handle("GET /scrapingSources/{id}", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getScrapeSourceDetail))))

	apiMux.Handle("GET /user/settings", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getUserSettings))))
	apiMux.Handle("POST /user/settings", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(saveUserSettings))))
	apiMux.Handle("GET /workspace/settings", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(getUserSettings))))
	apiMux.Handle("POST /workspace/settings", auth.RequireAuth(withWorkspaceOperator(http.HandlerFunc(saveUserSettings))))
	apiMux.Handle("GET /user/role", auth.RequireAuth(http.HandlerFunc(getUserRole)))
	apiMux.Handle("POST /user/export", auth.RequireAuth(withWorkspaceViewer(http.HandlerFunc(exportProxies))))
	apiMux.Handle("GET /global/settings", auth.IsAdmin(http.HandlerFunc(getGlobalSettings)))

	router.Handle("/api", http.StripPrefix("/api", apiMux))
	router.Handle("/api/", http.StripPrefix("/api", apiMux))

	log.Debug("Routes opened")
	timeouts := resolveServerTimeouts()

	handler := withRequestID(withAccessLog(withPanicRecovery(withSecurityHeaders(enableCORS(router)))))

	server := http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadTimeout:       timeouts.readTimeout,
		ReadHeaderTimeout: timeouts.readHeaderTimeout,
		WriteTimeout:      timeouts.writeTimeout,
		IdleTimeout:       timeouts.idleTimeout,
	}

	log.Infof("Starting magpie backend on port :%d", port)
	serverErrCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- fmt.Errorf("api server failed: %w", err)
			return
		}
		serverErrCh <- nil
	}()

	select {
	case err := <-serverErrCh:
		return err
	case <-ctx.Done():
		log.Info("Shutting down API server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), resolveServerShutdownTimeout())
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("api server shutdown failed: %w", err)
		}
		return <-serverErrCh
	}
}
