package model

import (
	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/internal/util"
)

// Index is a database index used to optimize queries against a collection
type Index struct {
	// Name is the indexes unique name in the collection
	Name string `json:"name" validate:"required,min=1"`
	// Fields to index - order matters
	Fields []string `json:"fields" validate:"required,min=1"`
	// Unique indicates that it's a unique index which will enforce uniqueness
	Unique bool `json:"unique"`
	// Unique indicates that it's a primary index
	Primary bool `json:"primary"`
	// IsBuilding indicates that the index is currently building
	IsBuilding bool `json:"isBuilding"`
}

// Validate validates the index
func (i Index) Validate() error {
	return errors.Wrap(util.ValidateStruct(&i), errors.Validation, "")
}
