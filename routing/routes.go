package routing

import (
	"fmt"
	"github.com/charmbracelet/log"
	"magpie/authorization"
	"net/http"
)

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

func OpenRoutes(port int) {

	router := http.NewServeMux()
	router.HandleFunc("POST /register", registerUser)
	router.HandleFunc("POST /login", loginUser)
	router.Handle("POST /addProxies", authorization.RequireAuth(http.HandlerFunc(addProxies)))
	router.Handle("POST /saveSettings", authorization.RequireRole("admin")(http.HandlerFunc(SaveSettings)))
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
