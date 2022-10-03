package wolverine

import (
	"context"
	"sync"

	"github.com/autom8ter/machine/v4"
)

func (d *db) ChangeStream(ctx context.Context, collections []string, fn ChangeStreamHandler) error {
	wg := sync.WaitGroup{}
	for _, collection := range collections {
		wg.Add(1)
		collection := collection
		go func(collection string) {
			defer wg.Done()
			d.machine.Subscribe(ctx, collection, func(ctx context.Context, msg machine.Message) (bool, error) {
				switch document := msg.Body.(type) {
				case *Document:
					if err := fn(ctx, []*Document{document}); err != nil {
						return false, err
					}
				case []*Document:
					if err := fn(ctx, document); err != nil {
						return false, err
					}
				}
				return true, nil
			})
		}(collection)
	}
	wg.Wait()
	return nil
}
