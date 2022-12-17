package model_test

import (
	"testing"

	"github.com/autom8ter/gokvkit/model"
	"github.com/stretchr/testify/assert"
)

func TestCommand(t *testing.T) {
	c := model.Command{}
	assert.NotNil(t, c.Validate())
	c.Collection = "task"
	assert.NotNil(t, c.Validate())
}
