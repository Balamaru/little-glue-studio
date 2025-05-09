package handlers

import (
	"encoding/json"
	"html/template"
	"little-glue-studio/models"
	"net/http"
	"os"
	"path/filepath"
)

func GenerateScript(w http.ResponseWriter, r *http.Request) {
	var etl models.ETLRequest
	if err := json.NewDecoder(r.Body).Decode(&etl); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	tmpl, err := template.ParseFiles("templates/base_spark.tmpl")
	if err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}

	outputPath := filepath.Join("output", "job1234.py")
	file, err := os.Create(outputPath)
	if err != nil {
		http.Error(w, "file create error", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	if err := tmpl.Execute(file, etl); err != nil {
		http.Error(w, "template execution error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Script generated at " + outputPath))
}
