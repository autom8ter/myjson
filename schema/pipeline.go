package schema

import (
	"context"
	"github.com/reactivex/rxgo/v2"
)

func pipeFullScan(ctx context.Context, input chan rxgo.Item, where []Where, order OrderBy) chan rxgo.Item {
	var documents []*Document
	for doc := range rxgo.FromEventSource(input, rxgo.WithContext(ctx), rxgo.WithObservationStrategy(rxgo.Eager)).
		Filter(func(i interface{}) bool {
			pass, err := i.(*Document).Where(where)
			if err != nil {
				return false
			}
			return pass
		}).Observe() {
		documents = append(documents, doc.V.(*Document))
	}
	documents = SortOrder(order, documents)
	var sorted = make(chan rxgo.Item)
	go func() {
		for _, doc := range documents {
			sorted <- rxgo.Of(doc)
		}
		close(sorted)
	}()
	return sorted
}
