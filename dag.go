package myjson

import (
	"sync"

	"github.com/autom8ter/dagger"
	"github.com/autom8ter/myjson/errors"
)

type collectionDag struct {
	dagger *dagger.Graph
	mu     sync.RWMutex
}

func newCollectionDag() *collectionDag {
	return &collectionDag{dagger: dagger.NewGraph()}
}

func (c *collectionDag) SetSchemas(schemas []CollectionSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var newDag = newCollectionDag()
	for _, schema := range schemas {
		nodePath := dagger.Path{
			XID:   schema.Collection(),
			XType: "collection",
		}
		newDag.dagger.SetNode(nodePath, map[string]interface{}{
			"schema": schema,
		})
	}
	for _, schema := range schemas {
		for _, f := range schema.Properties() {
			if f.ForeignKey != nil {
				fkeypath := dagger.Path{
					XID:   f.ForeignKey.Collection,
					XType: "collection",
				}
				if !newDag.dagger.HasNode(fkeypath) {
					return errors.New(errors.Validation, "foreign key collection not found: %s", f.ForeignKey.Collection)
				}

				if _, err := newDag.dagger.SetEdge(dagger.Path{
					XID:   schema.Collection(),
					XType: "collection",
				}, fkeypath, dagger.Node{
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
	_, err := newDag.TopologicalSort()
	if err != nil {
		return err
	}
	_, err = newDag.ReverseTopologicalSort()
	if err != nil {
		return err
	}
	c.dagger = newDag.dagger
	return nil
}

func (c *collectionDag) TopologicalSort() ([]CollectionSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var schemas []CollectionSchema
	var err error
	c.dagger.TopologicalSort("collection", "foreignkey", func(node dagger.Node) bool {
		var collection, ok = c.dagger.GetNode(node.Path)
		if !ok {
			err = errors.New(errors.Validation, "schema not found: %s", node.Path.XID)
			return false
		}
		schemas = append(schemas, collection.Attributes["schema"].(CollectionSchema))
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
		var collection, ok = c.dagger.GetNode(node.Path)
		if !ok {
			err = errors.New(errors.Validation, "schema not found: %s", node.Path.XID)
			return false
		}
		schemas = append(schemas, collection.Attributes["schema"].(CollectionSchema))
		return true
	})
	return schemas, err
}
