package gokvkit

import (
	"os"
	"testing"

	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/stretchr/testify/assert"
)

func TestOpenAPI(t *testing.T) {
	t.Run("crm api", func(t *testing.T) {
		f, _ := os.Create("tmp/openapi.yaml")
		defer f.Close()
		u, err := newCollectionSchema([]byte(userSchema))
		assert.Nil(t, err)
		tsk, err := newCollectionSchema([]byte(taskSchema))
		assert.Nil(t, err)
		bits, err := getOpenAPISpec(safe.NewMap(map[string]*collectionSchema{
			"user": u,
			"task": tsk,
		}), &openAPIParams{
			title:       "CRM API",
			version:     "1.0.0",
			description: "an example CRM api",
		})
		f.Write(bits)
	})
}
