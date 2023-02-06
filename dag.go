package myjson

import (
	"sync"

	"github.com/autom8ter/dagger"
	"github.com/autom8ter/myjson/errors"
)

type collectionDag struct {
	dagger  *dagger.Graph
	schemas map[string]CollectionSchema
	mu      sync.RWMutex
}

func (c *collectionDag) AddSchema(schema CollectionSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[schema.Collection()] = schema
	nodePath := dagger.Path{
		XID:   schema.Collection(),
		XType: "collection",
	}
	c.dagger.SetNode(nodePath, map[string]interface{}{})
	for _, f := range schema.Properties() {
		if f.ForeignKey != nil {
			fkeypath := dagger.Path{
				XID:   f.ForeignKey.Collection,
				XType: "collection",
			}
			if !c.dagger.HasNode(fkeypath) {
				c.dagger.SetNode(dagger.Path{
					XID:   f.ForeignKey.Collection,
					XType: "collection",
				}, map[string]interface{}{})
			}

			if _, err := c.dagger.SetEdge(nodePath, fkeypath, dagger.Node{
				Path: dagger.Path{
					XID:   f.Name,
					XType: "foreignkey",
				},
				Attributes: map[string]interface{}{},
			},
			); err != nil {
				panic(err)
			}
		}
	}
}

func (c *collectionDag) RemoveSchema(schema string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.schemas, schema)
	c.dagger.DelNode(dagger.Path{
		XID:   schema,
		XType: "collection",
	})
}

func (c *collectionDag) TopologicalSort() ([]CollectionSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var schemas []CollectionSchema
	var err error
	c.dagger.TopologicalSort("collection", "foreignkey", func(node dagger.Node) bool {
		if c.schemas[node.Path.XID] == nil {
			err = errors.New(errors.Validation, "schema not found for node %s", node.Path.XID)
			return false
		}
		schemas = append(schemas, c.schemas[node.Path.XID])
		return true
	})
	return schemas, err
}

func (c *collectionDag) ReverseTopologicalSort() ([]CollectionSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var schemas []CollectionSchema
	var err error
	c.dagger.ReverseTopologicalSort("collection", "foreignkey", func(node dagger.Node) bool {
		if c.schemas[node.Path.XID] == nil {
			err = errors.New(errors.Validation, "schema not found for node %s", node.Path.XID)
			return false
		}
		schemas = append(schemas, c.schemas[node.Path.XID])
		return true
	})
	return schemas, err
}
