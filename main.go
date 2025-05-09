package main

import (
	"little-glue-studio/handlers"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/generate", handlers.GenerateScript)

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
