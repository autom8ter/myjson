package model

// Index is a database index used to optimize queries against a collection
type Index struct {
	// Name is the indexes unique name in the collection
	Name string `json:"name" validate:"required"`
	// Fields to index - order matters
	Fields []string `json:"fields" validate:"required"`
	// Unique indicates that it's a unique index which will enforce uniqueness
	Unique bool `json:"unique"`
	// Unique indicates that it's a primary index
	Primary bool `json:"primary"`
	// IsBuilding indicates that the index is currently building
	IsBuilding bool `json:"isBuilding"`
}
