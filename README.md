# Little Glue Studio

A simplified service similar to AWS Glue Studio for generating PySpark ETL scripts based on JSON configurations.

## Features

### Supported Data Sources

1. **Amazon S3**
   - Access Key
   - Secret Key
   - Bucket
   - S3 Path

2. **S3 Compatible Storage** (MinIO, etc.)
   - Access Key
   - Secret Key
   - Bucket
   - S3 Path
   - Endpoint URL

3. **PostgreSQL**
   - Host
   - Port
   - Database Name
   - Table Name
   - User
   - Password

### Supported Transformations

1. **Filter**
   - Filter data based on SQL-like conditions

2. **Select**
   - Select specific columns from the dataset

3. **Rename Column**
   - Rename columns with a mapping of old names to new names

4. **Drop Column**
   - Remove specified columns from the dataset

5. **Add Column**
   - Add a new column based on a SQL expression

6. **Join**
   - Join with another data source (S3 or PostgreSQL)
   - Supports various join types (inner, left, right, full)

7. **Group By with Aggregations**
   - Group data by specified columns
   - Apply aggregation functions (sum, count, avg, etc.)

### Supported Targets

Same as data sources:
- Amazon S3
- S3 Compatible Storage
- PostgreSQL

## Getting Started

### Prerequisites

- Go 1.22 or later

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/Balamaru/little-glue-studio.git
   cd little-glue-studio
   ```

2. Build the application:
   ```
   go build
   ```

### Running the Application

```
./little-glue-studio
```

The server will start on port 8080 by default. You can specify a different port using the `PORT` environment variable.

## API Endpoints

### Generate PySpark Script

```
POST /generate
```

Request body: JSON configuration for ETL job (see examples)

Response:
```json
{
  "status": "success",
  "message": "PySpark script generated successfully",
  "script_path": "output/etl_job_20250509_123456.py"
}
```

### List Generated Scripts

```
GET /scripts
```

Response:
```json
{
  "scripts": [
    "etl_job_20250509_123456.py",
    "etl_job_20250508_234567.py"
  ]
}
```

### Health Check

```
GET /health
```

Response:
```json
{
  "status": "ok",
  "version": "1.0.0"
}
```

## Example Configuration

Check the `examples` directory for sample ETL job configurations:

- `complete_etl.json`: A comprehensive example using PostgreSQL source, multiple transformations, and S3 target
- `s3_source.json`: Example using Amazon S3 as source and PostgreSQL as target
- `s3_compatible_source.json`: Example using S3-compatible storage (like MinIO)

## Project Structure

```
little-glue-studio/
├── handlers/        # HTTP request handlers
├── models/          # Data models
├── templates/       # PySpark script templates
├── examples/        # Example ETL configurations
├── output/          # Generated PySpark scripts
├── main.go          # Application entry point
└── README.md        # This file
```