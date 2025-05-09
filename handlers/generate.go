package handlers

import (
	"encoding/json"
	"fmt"
	"html/template"
	"little-glue-studio/models"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// createSparkTemplate creates the PySpark template file
func createSparkTemplate() error {
	templateContent := `from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Generated ETL Job").getOrCreate()

# Define source data loading
{{if eq .SourceType "s3"}}
# Load data from Amazon S3
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "{{.S3Source.AccessKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "{{.S3Source.SecretKey}}")
df = spark.read.format("parquet").load("s3a://{{.S3Source.Bucket}}/{{.S3Source.Path}}")
{{else if eq .SourceType "s3_compatible"}}
# Load data from S3-compatible storage
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "{{.S3Source.AccessKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "{{.S3Source.SecretKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "{{.S3Source.EndpointURL}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
df = spark.read.format("parquet").load("s3a://{{.S3Source.Bucket}}/{{.S3Source.Path}}")
{{else if eq .SourceType "postgresql"}}
# Load data from PostgreSQL
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://{{.PgSource.Host}}:{{.PgSource.Port}}/{{.PgSource.Database}}") \
    .option("dbtable", "{{.PgSource.Table}}") \
    .option("user", "{{.PgSource.User}}") \
    .option("password", "{{.PgSource.Password}}") \
    .load()
{{end}}

# Apply transforms
{{range $index, $transform := .Transforms}}
# Transform {{$index}}: {{$transform.Type}}
{{if eq $transform.Type "filter"}}
df = df.filter("{{$transform.Condition}}")
{{else if eq $transform.Type "select"}}
df = df.select({{range $i, $col := $transform.Columns}}{{if $i}}, {{end}}"{{$col}}"{{end}})
{{else if eq $transform.Type "rename_column"}}
{{range $old, $new := $transform.RenameColumns}}
df = df.withColumnRenamed("{{$old}}", "{{$new}}")
{{end}}
{{else if eq $transform.Type "drop_column"}}
df = df.drop({{range $i, $col := $transform.Columns}}{{if $i}}, {{end}}"{{$col}}"{{end}})
{{else if eq $transform.Type "add_column"}}
df = df.withColumn("{{$transform.Name}}", F.expr("{{$transform.Expression}}"))
{{else if eq $transform.Type "join"}}
# Load right dataframe for join
{{if eq $transform.RightSourceType "s3"}}
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "{{$transform.RightS3Source.AccessKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "{{$transform.RightS3Source.SecretKey}}")
right_df = spark.read.format("parquet").load("s3a://{{$transform.RightS3Source.Bucket}}/{{$transform.RightS3Source.Path}}")
{{else if eq $transform.RightSourceType "s3_compatible"}}
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "{{$transform.RightS3Source.AccessKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "{{$transform.RightS3Source.SecretKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "{{$transform.RightS3Source.EndpointURL}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
right_df = spark.read.format("parquet").load("s3a://{{$transform.RightS3Source.Bucket}}/{{$transform.RightS3Source.Path}}")
{{else if eq $transform.RightSourceType "postgresql"}}
right_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://{{$transform.RightPgSource.Host}}:{{$transform.RightPgSource.Port}}/{{$transform.RightPgSource.Database}}") \
    .option("dbtable", "{{$transform.RightPgSource.Table}}") \
    .option("user", "{{$transform.RightPgSource.User}}") \
    .option("password", "{{$transform.RightPgSource.Password}}") \
    .load()
{{end}}

# Create join conditions
join_condition = {{range $i, $lcol := $transform.LeftColumns}}{{$rcol := index $transform.RightColumns $i}}{{if $i}} & {{end}}(df["{{$lcol}}"] == right_df["{{$rcol}}"]){{end}}

# Apply join
df = df.join(right_df, join_condition, "{{$transform.JoinType}}")
{{else if eq $transform.Type "groupby_agg"}}
# Group by and aggregate
df = df.groupBy({{range $i, $col := $transform.GroupByCols}}{{if $i}}, {{end}}"{{$col}}"{{end}}) \
    .agg(
        {{range $i, $col := $transform.AggregationColumns}}
        {{if $i}},
        {{end}}F.{{index $transform.AggregationFunctions $i}}("{{$col}}").alias("{{$col}}_{{index $transform.AggregationFunctions $i}}")
        {{end}}
    )
{{end}}

{{end}}

# Write to target destination
{{if eq .Target.Type "s3"}}
# Write to Amazon S3
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "{{.Target.AccessKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "{{.Target.SecretKey}}")
df.write.mode("overwrite").parquet("s3a://{{.Target.Bucket}}/{{.Target.Path}}")
{{else if eq .Target.Type "s3_compatible"}}
# Write to S3-compatible storage
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "{{.Target.AccessKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "{{.Target.SecretKey}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "{{.Target.EndpointURL}}")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
df.write.mode("overwrite").parquet("s3a://{{.Target.Bucket}}/{{.Target.Path}}")
{{else if eq .Target.Type "postgresql"}}
# Write to PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://{{.Target.Host}}:{{.Target.Port}}/{{.Target.Database}}") \
    .option("dbtable", "{{.Target.Table}}") \
    .option("user", "{{.Target.User}}") \
    .option("password", "{{.Target.Password}}") \
    .mode("overwrite") \
    .save()
{{end}}

print("ETL job completed successfully!")
`

	return os.WriteFile("templates/spark_template.tmpl", []byte(templateContent), 0644)
}

func GenerateScript(w http.ResponseWriter, r *http.Request) {
	var etlRequest models.ETLRequest
	if err := json.NewDecoder(r.Body).Decode(&etlRequest); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse source type and data
	sourceType := etlRequest.GetSourceType()
	source, err := etlRequest.ParseSource()
	if err != nil {
		http.Error(w, "Error parsing source: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Prepare template data
	data := map[string]interface{}{
		"SourceType": sourceType,
		"Transforms": []map[string]interface{}{},
		"Target":     etlRequest.Target,
	}

	// Add source specific data
	switch sourceType {
	case "s3", "s3_compatible":
		data["S3Source"] = source
	case "postgresql":
		data["PgSource"] = source
	}

	// Process transforms with additional handling for specific transform types
	for i, transform := range etlRequest.Transforms {
		transformData := map[string]interface{}{
			"Type":      transform.Type,
			"Condition": transform.Condition,
			"Columns":   transform.Columns,
		}

		// Process transform-specific data
		switch transform.Type {
		case "rename_column":
			transformData["RenameColumns"] = transform.RenameColumns
		case "drop_column":
			transformData["Columns"] = transform.Columns
		case "add_column":
			transformData["Name"] = transform.Name
			transformData["Expression"] = transform.Expression
		case "join":
			// Parse right source
			rightSource, err := models.ParseRightSource(transform.RightSource)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error parsing right source for join %d: %s", i, err.Error()), http.StatusBadRequest)
				return
			}

			// Determine right source type
			var rightSourceType string
			switch rs := rightSource.(type) {
			case models.S3Source:
				rightSourceType = rs.Type
				transformData["RightS3Source"] = rs
			case models.PostgreSQLSource:
				rightSourceType = "postgresql"
				transformData["RightPgSource"] = rs
			}

			transformData["RightSourceType"] = rightSourceType
			transformData["JoinType"] = transform.JoinType
			transformData["LeftColumns"] = transform.LeftColumns
			transformData["RightColumns"] = transform.RightColumns
		case "groupby_agg":
			transformData["GroupByCols"] = transform.GroupByCols

			// Extract columns and aggregation functions for template
			aggColumns := []string{}
			aggFunctions := []string{}
			for col, aggFunc := range transform.Aggregations {
				aggColumns = append(aggColumns, col)
				aggFunctions = append(aggFunctions, aggFunc)
			}
			transformData["AggregationColumns"] = aggColumns
			transformData["AggregationFunctions"] = aggFunctions
		}

		// Add transform to the list
		data["Transforms"] = append(data["Transforms"].([]map[string]interface{}), transformData)
	}

	// Ensure templates directory exists
	if err := os.MkdirAll("templates", 0755); err != nil {
		http.Error(w, "Failed to create templates directory: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if template file exists
	templatePath := "templates/spark_template.tmpl"
	if _, err := os.Stat(templatePath); os.IsNotExist(err) {
		// Create the template file with content
		if err := createSparkTemplate(); err != nil {
			http.Error(w, "Failed to create template file: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Load the template
	tmpl, err := template.ParseFiles(templatePath)
	if err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll("output", 0755); err != nil {
		http.Error(w, "Failed to create output directory: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Generate a unique filename
	timestamp := time.Now().Format("20060102_150405")
	outputPath := filepath.Join("output", fmt.Sprintf("etl_job_%s.py", timestamp))

	// Create output file
	file, err := os.Create(outputPath)
	if err != nil {
		http.Error(w, "File create error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Execute template
	if err := tmpl.Execute(file, data); err != nil {
		http.Error(w, "Template execution error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return success response
	response := map[string]string{
		"status":      "success",
		"message":     "PySpark script generated successfully",
		"script_path": outputPath,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
