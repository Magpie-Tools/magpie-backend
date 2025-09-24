package server

import (
	"encoding/json"
	"fmt"
	"github.com/charmbracelet/log"
	"magpie/internal/auth"
	"net/http"
	"os"
	"path/filepath"
)

const distDir = "./static/frontend/browser"

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, msg string, status int) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow any origin
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight request
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Pass the request to the next handler
		next.ServeHTTP(w, r)
	})
}

func ServeFrontend(port int) error {
	if abs, err := filepath.Abs(distDir); err == nil {
		log.Debugf("➡️  Serving static from: %s", abs)
	} else {
		log.Warnf("couldn’t resolve %q: %v", distDir, err)
	}

	mux := http.NewServeMux()
	fs := http.FileServer(http.Dir(distDir))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fp := filepath.Join(distDir, filepath.Clean(r.URL.Path))
		if info, err := os.Stat(fp); err == nil && !info.IsDir() {
			fs.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(distDir, "index.csr.html"))
	})

	addr := fmt.Sprintf(":%d", port)
	log.Infof("Starting frontend static server on port %s", addr)
	return http.ListenAndServe(addr, mux)
}

func OpenRoutes(port int, serveStatic bool) error {

	router := http.NewServeMux()
	router.HandleFunc("POST /register", registerUser)
	router.HandleFunc("POST /login", loginUser)
	router.Handle("GET /checkLogin", auth.RequireAuth(http.HandlerFunc(checkLogin)))
	router.Handle("POST /changePassword", auth.RequireAuth(http.HandlerFunc(changePassword)))
	router.Handle("POST /saveSettings", auth.IsAdmin(http.HandlerFunc(saveSettings)))
	router.Handle("GET /getDashboardInfo", auth.RequireAuth(http.HandlerFunc(getDashboardInfo)))

	router.Handle("GET /getProxyCount", auth.RequireAuth(http.HandlerFunc(getProxyCount)))
	router.Handle("GET /getProxyPage/{page}", auth.RequireAuth(http.HandlerFunc(getProxyPage)))
	router.Handle("POST /addProxies", auth.RequireAuth(http.HandlerFunc(addProxies)))
	router.Handle("DELETE /proxies", auth.RequireAuth(http.HandlerFunc(deleteProxies)))

	router.Handle("GET /getScrapingSourcesCount", auth.RequireAuth(http.HandlerFunc(getScrapeSourcesCount)))
	router.Handle("GET /getScrapingSourcesPage/{page}", auth.RequireAuth(http.HandlerFunc(getScrapeSourcePage)))
	router.Handle("POST /scrapingSources", auth.RequireAuth(http.HandlerFunc(saveScrapingSources)))
	router.Handle("DELETE /scrapingSources", auth.RequireAuth(http.HandlerFunc(deleteScrapingSources)))

	router.Handle("GET /user/settings", auth.RequireAuth(http.HandlerFunc(getUserSettings)))
	router.Handle("POST /user/settings", auth.RequireAuth(http.HandlerFunc(saveUserSettings)))
	router.Handle("GET /user/role", auth.RequireAuth(http.HandlerFunc(getUserRole)))
	router.Handle("POST /user/export", auth.RequireAuth(http.HandlerFunc(exportProxies)))

	router.Handle("GET /global/settings", auth.IsAdmin(http.HandlerFunc(getGlobalSettings)))

	// ---------------
	// FRONTEND
	// ---------------
	if serveStatic {
		fs := http.FileServer(http.Dir(distDir))

		router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				http.NotFound(w, r)
			}
			path := filepath.Join(distDir, filepath.Clean(r.URL.Path))
			if info, err := os.Stat(path); err == nil && !info.IsDir() {
				fs.ServeHTTP(w, r)
				return
			}
			http.ServeFile(w, r, filepath.Join(distDir, "index.csr.html"))
		})

		log.Debugf("Frontend assets served from %s on the same port", distDir)
	}

	log.Debug("Routes opened")

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: enableCORS(router),
	}

	log.Infof("Starting magpie backend on port :%d", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("api server failed: %w", err)
	}
	return nil
}
