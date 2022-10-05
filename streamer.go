package wolverine

import (
	"context"

	"github.com/autom8ter/machine/v4"
)

func (d *db) ChangeStream(ctx context.Context, collections []string, fn ChangeStreamHandler) error {
	m := machine.New()
	for _, collection := range collections {
		collection := collection
		m.Go(ctx, func(ctx context.Context) error {
			return d.machine.Subscribe(ctx, collection, func(ctx context.Context, msg machine.Message) (bool, error) {
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
		})
	}
	return d.wrapErr(m.Wait(), "")
}
