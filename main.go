package main

import (
	"log"
	"net/http"
)

func main() {
	router := http.NewServeMux()
	router.HandleFunc("GET /item/{id}", func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("id")
		writer.Write([]byte("received request for item: " + id))
	})

	server := http.Server{
		Addr:    ":8080",
		Handler: router,
	}
	log.Println("Starting server on port :8080")
	err := server.ListenAndServe()
	if err != nil {
		return
	}
}
