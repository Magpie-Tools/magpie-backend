package routing

import (
	"fmt"
	"github.com/charmbracelet/log"
	"magpie/authorization"
	"net/http"
	"os"
	"path/filepath"
)

const distDir = "../frontend/dist/frontend/browser"

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

func ServeFrontend(port int) {
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
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Frontend server failed: %v", err)
	}
}

func OpenRoutes(port int, serveStatic bool) {

	router := http.NewServeMux()
	router.HandleFunc("POST /register", registerUser)
	router.HandleFunc("POST /login", loginUser)
	router.Handle("POST /saveSettings", authorization.IsAdmin(http.HandlerFunc(saveSettings)))
	router.Handle("GET /getDashboardInfo", authorization.RequireAuth(http.HandlerFunc(getDashboardInfo)))

	router.Handle("GET /getProxyCount", authorization.RequireAuth(http.HandlerFunc(getProxyCount)))
	router.Handle("GET /getProxyPage/{page}", authorization.RequireAuth(http.HandlerFunc(getProxyPage)))
	router.Handle("POST /addProxies", authorization.RequireAuth(http.HandlerFunc(addProxies)))
	router.Handle("DELETE /proxies", authorization.RequireAuth(http.HandlerFunc(deleteProxies)))

	router.Handle("GET /getScrapingSourcesCount", authorization.RequireAuth(http.HandlerFunc(getScrapeSourcesCount)))
	router.Handle("GET /getScrapingSourcesPage/{page}", authorization.RequireAuth(http.HandlerFunc(getScrapeSourcePage)))
	router.Handle("POST /scrapingSources", authorization.RequireAuth(http.HandlerFunc(saveScrapingSources)))
	router.Handle("DELETE /scrapingSources", authorization.RequireAuth(http.HandlerFunc(deleteScrapingSources)))

	router.Handle("GET /user/settings", authorization.RequireAuth(http.HandlerFunc(getUserSettings)))
	router.Handle("POST /user/settings", authorization.RequireAuth(http.HandlerFunc(saveUserSettings)))
	router.Handle("GET /user/role", authorization.RequireAuth(http.HandlerFunc(getUserRole)))
	router.Handle("POST /user/export", authorization.RequireAuth(http.HandlerFunc(exportProxies)))

	router.Handle("GET /global/settings", authorization.IsAdmin(http.HandlerFunc(getGlobalSettings)))

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

		log.Debugf("➡️  Frontend assets served from %s on same port", distDir)
	}

	log.Debug("Routes opened")

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: enableCORS(router),
	}

	log.Infof("Starting mapgie backend on port :%d\n", port)
	err := server.ListenAndServe()
	if err != nil {
		log.Fatalf("%s\nUse \"go run magpie -port=[PORT]\" to run with a custom port", err)
		return
	}
}
