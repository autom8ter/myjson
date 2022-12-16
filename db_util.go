package gokvkit

import (
	"context"

	"github.com/autom8ter/gokvkit/model"
)

func (d *DB) getReadyIndexes(ctx context.Context, collection string) map[string]model.Index {
	var indexes = map[string]model.Index{}
	for _, i := range d.collections.Get(collection).indexing {
		if i.IsBuilding {
			continue
		}
		indexes[i.Name] = i
	}
	return indexes
}
