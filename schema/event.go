package schema

type Action string

const (
	Set    = "set"
	Update = "update"
	Delete = "delete"
)

type StateChange struct {
	Collection string                    `json:"collection"`
	Deletes    []string                  `json:"deletes,omitempty"`
	Sets       []Document                `json:"sets,omitempty"`
	Updates    map[string]map[string]any `json:"updates,omitempty"`
}
