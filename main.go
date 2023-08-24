package main

import (
	"inmemorydb/core"
	"inmemorydb/server"
	"log"
	"net/http"
)

func main() {
	db := core.StartInMemoryDb()

	serverHandler := server.NewHandler(db)

	// Register routes
	http.HandleFunc("/", serverHandler.Status)
	http.HandleFunc("/command", serverHandler.Command)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
