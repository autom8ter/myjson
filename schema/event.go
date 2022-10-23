package schema

type Action string

const (
	Set    = "set"
	Update = "update"
	Delete = "delete"
)

type Event struct {
	Collection string      `json:"collection"`
	Action     Action      `json:"action,omitempty"`
	Documents  []*Document `json:"documents,omitempty"`
}
