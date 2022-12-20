package gokvkit

import (
	"context"
)

func (d *DB) getReadyIndexes(ctx context.Context, collection string) map[string]Index {
	var indexes = map[string]Index{}
	for _, i := range d.collections.Get(collection).Indexing() {
		if i.IsBuilding {
			continue
		}
		indexes[i.Name] = i
	}
	return indexes
}
