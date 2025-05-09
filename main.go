package main

import (
	"encoding/json"
	"little-glue-studio/handlers"
	"log"
	"net/http"
	"os"
)

// Simple middleware for logging requests
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

// Health check endpoint
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]string{
		"status":  "ok",
		"version": "1.0.0",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Get generated scripts list
func listScriptsHandler(w http.ResponseWriter, r *http.Request) {
	// Read output directory
	files, err := os.ReadDir("output")
	if err != nil {
		http.Error(w, "Failed to read output directory", http.StatusInternalServerError)
		return
	}

	var scripts []string
	for _, file := range files {
		if !file.IsDir() {
			scripts = append(scripts, file.Name())
		}
	}

	response := map[string]interface{}{
		"scripts": scripts,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// createExampleFiles creates example JSON files for reference
func createExampleFiles() error {
	// Ensure examples directory exists
	if err := os.MkdirAll("examples", 0755); err != nil {
		return err
	}

	// Create complete example
	completeExample := `{
  "source": {
    "type": "postgresql",
    "host": "db.example.com",
    "port": 5432,
    "database": "sales_db",
    "table": "public.orders",
    "user": "analyzer",
    "password": "secure_password"
  },
  "transforms": [
    {
      "type": "filter",
      "condition": "order_date >= '2023-01-01' AND order_status != 'CANCELLED'"
    },
    {
      "type": "select",
      "columns": ["order_id", "customer_id", "order_date", "total_amount", "payment_method"]
    },
    {
      "type": "rename_column",
      "rename_columns": {
        "order_id": "id",
        "customer_id": "cust_id",
        "order_date": "date"
      }
    },
    {
      "type": "add_column",
      "name": "order_month",
      "expression": "date_format(date, 'yyyy-MM')"
    },
    {
      "type": "join",
      "right_source": {
        "type": "postgresql",
        "host": "db.example.com",
        "port": 5432,
        "database": "sales_db",
        "table": "public.customers",
        "user": "analyzer",
        "password": "secure_password"
      },
      "join_type": "left",
      "left_columns": ["cust_id"],
      "right_columns": ["customer_id"]
    },
    {
      "type": "select",
      "columns": ["id", "cust_id", "date", "total_amount", "payment_method", "order_month", "customer_name", "customer_segment"]
    },
    {
      "type": "groupby_agg",
      "groupby_cols": ["order_month", "customer_segment", "payment_method"],
      "aggregations": {
        "total_amount": "sum",
        "id": "count"
      }
    }
  ],
  "target": {
    "type": "s3",
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "bucket": "analytics-reports",
    "path": "monthly_sales/segment_report/"
  }
}`

	// Create S3 example
	s3Example := `{
  "source": {
    "type": "s3",
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "bucket": "raw-data",
    "path": "sales/2023/"
  },
  "transforms": [
    {
      "type": "filter",
      "condition": "sales_amount > 100"
    },
    {
      "type": "select",
      "columns": ["transaction_id", "product_id", "sales_amount", "transaction_date"]
    }
  ],
  "target": {
    "type": "postgresql",
    "host": "warehouse.example.com",
    "port": 5432,
    "database": "analytics",
    "table": "filtered_sales",
    "user": "etl_user",
    "password": "secure_password"
  }
}`

	// Create S3 compatible example
	s3CompatibleExample := `{
  "source": {
    "type": "s3_compatible",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "raw-data",
    "path": "logs/app/",
    "endpoint_url": "http://minio.local:9000"
  },
  "transforms": [
    {
      "type": "filter",
      "condition": "status_code >= 400"
    },
    {
      "type": "add_column",
      "name": "is_error",
      "expression": "status_code >= 500"
    },
    {
      "type": "drop_column",
      "columns": ["request_body", "response_body"]
    }
  ],
  "target": {
    "type": "s3_compatible",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "processed-data",
    "path": "error-logs/",
    "endpoint_url": "http://minio.local:9000"
  }
}`

	// Write example files
	if err := os.WriteFile("examples/complete_etl.json", []byte(completeExample), 0644); err != nil {
		return err
	}

	if err := os.WriteFile("examples/s3_source.json", []byte(s3Example), 0644); err != nil {
		return err
	}

	if err := os.WriteFile("examples/s3_compatible_source.json", []byte(s3CompatibleExample), 0644); err != nil {
		return err
	}

	return nil
}

func main() {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll("output", 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Create example files
	if err := createExampleFiles(); err != nil {
		log.Printf("Warning: Failed to create example files: %v", err)
	}

	// Create router
	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("/generate", handlers.GenerateScript)
	mux.HandleFunc("/health", healthCheckHandler)
	mux.HandleFunc("/scripts", listScriptsHandler)

	// Add middleware
	handler := loggingMiddleware(mux)

	// Determine port
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Start server
	log.Printf("Little Glue Studio server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, handler))
}
