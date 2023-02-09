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
			dagger: dagger.NewGraph(),
			mu:     sync.RWMutex{},
		}
		u, _ := newCollectionSchema([]byte(userSchema))
		a, _ := newCollectionSchema([]byte(accountSchema))
		tsk, _ := newCollectionSchema([]byte(taskSchema))
		if err := dag.SetSchemas([]CollectionSchema{u, a, tsk}); err != nil {
			t.Fatal(err)
		}
		sorted, err := dag.TopologicalSort()
		assert.NoError(t, err)
		assert.Equal(t, "task", sorted[0].Collection())
		assert.Equal(t, "user", sorted[1].Collection())
		assert.Equal(t, "account", sorted[2].Collection())
	})
	t.Run("dag - topological sort reverse", func(t *testing.T) {
		dag := &collectionDag{
			dagger: dagger.NewGraph(),
			mu:     sync.RWMutex{},
		}
		u, _ := newCollectionSchema([]byte(userSchema))
		a, _ := newCollectionSchema([]byte(accountSchema))
		tsk, _ := newCollectionSchema([]byte(taskSchema))
		if err := dag.SetSchemas([]CollectionSchema{u, a, tsk}); err != nil {
			t.Fatal(err)
		}
		sorted, err := dag.ReverseTopologicalSort()
		assert.NoError(t, err)
		assert.Equal(t, "account", sorted[0].Collection())
		assert.Equal(t, "user", sorted[1].Collection())
		assert.Equal(t, "task", sorted[2].Collection())
	})
	t.Run("dag - check edges", func(t *testing.T) {
		dag := &collectionDag{
			dagger: dagger.NewGraph(),
			mu:     sync.RWMutex{},
		}
		u, _ := newCollectionSchema([]byte(userSchema))
		a, _ := newCollectionSchema([]byte(accountSchema))
		tsk, _ := newCollectionSchema([]byte(taskSchema))
		if err := dag.SetSchemas([]CollectionSchema{u, a, tsk}); err != nil {
			t.Fatal(err)
		}
		{
			count := 0
			dag.dagger.RangeEdgesTo("foreignkey", dagger.Path{
				XID:   "task",
				XType: "collection",
			}, func(e dagger.Edge) bool {
				count++
				return true
			})
			assert.Equal(t, 0, count)
		}
		{
			count := 0
			dag.dagger.RangeEdgesTo("foreignkey", dagger.Path{
				XID:   "user",
				XType: "collection",
			}, func(e dagger.Edge) bool {
				count++
				return true
			})
			assert.Equal(t, 1, count)
		}
		{
			count := 0
			dag.dagger.RangeEdgesTo("foreignkey", dagger.Path{
				XID:   "account",
				XType: "collection",
			}, func(e dagger.Edge) bool {
				count++
				return true
			})
			assert.Equal(t, 1, count)
		}
	})
}
