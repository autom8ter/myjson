package myjson

import (
	"sync"
	"testing"

	"github.com/autom8ter/dagger"
	"github.com/stretchr/testify/assert"
)

func TestDag(t *testing.T) {
	t.Run("dag - topological sort", func(t *testing.T) {
		dag := &collectionDag{
			dagger:  dagger.NewGraph(),
			schemas: map[string]CollectionSchema{},
			mu:      sync.RWMutex{},
		}
		u, _ := newCollectionSchema([]byte(userSchema))
		dag.AddSchema(u)
		a, _ := newCollectionSchema([]byte(accountSchema))
		dag.AddSchema(a)
		tsk, _ := newCollectionSchema([]byte(taskSchema))
		dag.AddSchema(tsk)

		sorted, err := dag.TopologicalSort()
		assert.NoError(t, err)
		assert.Equal(t, "task", sorted[0].Collection())
		assert.Equal(t, "user", sorted[1].Collection())
		assert.Equal(t, "account", sorted[2].Collection())
	})
	t.Run("dag - topological sort reverse", func(t *testing.T) {
		dag := &collectionDag{
			dagger:  dagger.NewGraph(),
			schemas: map[string]CollectionSchema{},
			mu:      sync.RWMutex{},
		}
		u, _ := newCollectionSchema([]byte(userSchema))
		dag.AddSchema(u)
		a, _ := newCollectionSchema([]byte(accountSchema))
		dag.AddSchema(a)
		tsk, _ := newCollectionSchema([]byte(taskSchema))
		dag.AddSchema(tsk)
		sorted, err := dag.ReverseTopologicalSort()
		assert.NoError(t, err)
		assert.Equal(t, "account", sorted[0].Collection())
		assert.Equal(t, "user", sorted[1].Collection())
		assert.Equal(t, "task", sorted[2].Collection())
	})
}
