package wolverine

import (
	"context"

	"github.com/autom8ter/machine/v4"
	"github.com/palantir/stacktrace"
)

func (d *db) ChangeStream(ctx context.Context, collections []string, fn ChangeStreamHandler) error {
	m := machine.New()
	for _, collection := range collections {
		collection := collection
		m.Go(ctx, func(ctx context.Context) error {
			return d.machine.Subscribe(ctx, collection, func(ctx context.Context, msg machine.Message) (bool, error) {
				switch event := msg.Body.(type) {
				case *Event:
					if err := fn(ctx, event); err != nil {
						return false, err
					}
				case Event:
					if err := fn(ctx, &event); err != nil {
						return false, err
					}
				}
				return true, nil
			})
		})
	}
	return stacktrace.Propagate(m.Wait(), "")
}
