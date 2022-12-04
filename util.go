package gokvkit

import (
	"github.com/autom8ter/gokvkit/model"
	"reflect"
)

type indexDiff struct {
	toRemove []model.Index
	toAdd    []model.Index
	toUpdate []model.Index
}

func getIndexDiff(after, before map[string]model.Index) (indexDiff, error) {
	var (
		toRemove []model.Index
		toAdd    []model.Index
		toUpdate []model.Index
	)
	for _, index := range after {
		if _, ok := before[index.Name]; !ok {
			toAdd = append(toAdd, index)
		}
	}

	for _, current := range before {
		if _, ok := after[current.Name]; !ok {
			toRemove = append(toRemove, current)
		} else {
			if !reflect.DeepEqual(current.Fields, current.Fields) {
				toUpdate = append(toUpdate, current)
			}
		}
	}
	return indexDiff{
		toRemove: toRemove,
		toAdd:    toAdd,
		toUpdate: toUpdate,
	}, nil
}
