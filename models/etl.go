package models

import (
	"encoding/json"
	"fmt"
)

// Source types
type S3Source struct {
	Type        string `json:"type"` // "s3"
	AccessKey   string `json:"access_key"`
	SecretKey   string `json:"secret_key"`
	Bucket      string `json:"bucket"`
	Path        string `json:"path"`
	EndpointURL string `json:"endpoint_url,omitempty"` // Only for S3 compatible
}

type PostgreSQLSource struct {
	Type     string `json:"type"` // "postgresql"
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Table    string `json:"table"`
	User     string `json:"user"`
	Password string `json:"password"`
}

// Transform types
type FilterTransform struct {
	Type      string `json:"type"` // "filter"
	Condition string `json:"condition"`
}

type SelectTransform struct {
	Type    string   `json:"type"` // "select"
	Columns []string `json:"columns"`
}

type RenameColumnTransform struct {
	Type    string            `json:"type"`    // "rename_column"
	Columns map[string]string `json:"columns"` // old_name -> new_name
}

type DropColumnTransform struct {
	Type    string   `json:"type"` // "drop_column"
	Columns []string `json:"columns"`
}

type AddColumnTransform struct {
	Type       string `json:"type"` // "add_column"
	Name       string `json:"name"`
	Expression string `json:"expression"` // SQL expression
}

type JoinTransform struct {
	Type         string   `json:"type"`         // "join"
	RightSource  any      `json:"right_source"` // Can be S3Source or PostgreSQLSource
	JoinType     string   `json:"join_type"`    // "inner", "left", "right", "full"
	LeftColumns  []string `json:"left_columns"`
	RightColumns []string `json:"right_columns"`
}

type GroupByAggTransform struct {
	Type         string            `json:"type"` // "groupby_agg"
	GroupByCols  []string          `json:"groupby_cols"`
	Aggregations map[string]string `json:"aggregations"` // col -> agg_func
}

// Transform is a union type of all transform types
type Transform struct {
	Type          string            `json:"type"`
	Condition     string            `json:"condition,omitempty"`
	Columns       []string          `json:"columns,omitempty"`
	RenameColumns map[string]string `json:"rename_columns,omitempty"`
	Name          string            `json:"name,omitempty"`
	Expression    string            `json:"expression,omitempty"`
	RightSource   json.RawMessage   `json:"right_source,omitempty"`
	JoinType      string            `json:"join_type,omitempty"`
	LeftColumns   []string          `json:"left_columns,omitempty"`
	RightColumns  []string          `json:"right_columns,omitempty"`
	GroupByCols   []string          `json:"groupby_cols,omitempty"`
	Aggregations  map[string]string `json:"aggregations,omitempty"`
}

// Target types are the same as source types
type Target struct {
	Type string `json:"type"` // "s3", "s3_compatible", or "postgresql"
	// S3 fields
	AccessKey   string `json:"access_key,omitempty"`
	SecretKey   string `json:"secret_key,omitempty"`
	Bucket      string `json:"bucket,omitempty"`
	Path        string `json:"path,omitempty"`
	EndpointURL string `json:"endpoint_url,omitempty"`
	// PostgreSQL fields
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Database string `json:"database,omitempty"`
	Table    string `json:"table,omitempty"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
}

// Main ETL Request structure
type ETLRequest struct {
	Source     json.RawMessage `json:"source"` // Can be S3Source or PostgreSQLSource
	Transforms []Transform     `json:"transforms"`
	Target     Target          `json:"target"`
}

// Helper methods to determine the type of source
func (e *ETLRequest) GetSourceType() string {
	var sourceMap map[string]interface{}
	json.Unmarshal(e.Source, &sourceMap)
	return sourceMap["type"].(string)
}

// Parse the raw source into appropriate struct
func (e *ETLRequest) ParseSource() (interface{}, error) {
	sourceType := e.GetSourceType()

	switch sourceType {
	case "s3", "s3_compatible":
		var s3Source S3Source
		err := json.Unmarshal(e.Source, &s3Source)
		return s3Source, err
	case "postgresql":
		var pgSource PostgreSQLSource
		err := json.Unmarshal(e.Source, &pgSource)
		return pgSource, err
	default:
		return nil, fmt.Errorf("unsupported source type: %s", sourceType)
	}
}

// Parse right source for join transforms
func ParseRightSource(rawSource json.RawMessage) (interface{}, error) {
	var sourceMap map[string]interface{}
	if err := json.Unmarshal(rawSource, &sourceMap); err != nil {
		return nil, err
	}

	sourceType := sourceMap["type"].(string)

	switch sourceType {
	case "s3", "s3_compatible":
		var s3Source S3Source
		err := json.Unmarshal(rawSource, &s3Source)
		return s3Source, err
	case "postgresql":
		var pgSource PostgreSQLSource
		err := json.Unmarshal(rawSource, &pgSource)
		return pgSource, err
	default:
		return nil, fmt.Errorf("unsupported source type: %s", sourceType)
	}
}
