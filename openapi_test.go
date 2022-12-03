package gokvkit

import (
	"fmt"
	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpenAPI(t *testing.T) {
	t.Run("", func(t *testing.T) {
		u, err := newCollectionSchema([]byte(userSchema))
		assert.Nil(t, err)
		tsk, err := newCollectionSchema([]byte(taskSchema))
		assert.Nil(t, err)
		bits, err := getOpenAPISpec(safe.NewMap(map[string]*collectionSchema{
			"user": u,
			"task": tsk,
		}))
		fmt.Println(string(bits))
	})
}
