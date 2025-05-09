package models

type Source struct {
	Type     string `json:"type"`
	Table    string `json:"table"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
}

type Transform struct {
	Type      string   `json:"type"`
	Condition string   `json:"condition,omitempty"`
	Columns   []string `json:"columns,omitempty"`
}

type Target struct {
	Type string `json:"type"`
	Path string `json:"path"`
}

type ETLRequest struct {
	Source     Source      `json:"source"`
	Transforms []Transform `json:"transforms"`
	Target     Target      `json:"target"`
}
