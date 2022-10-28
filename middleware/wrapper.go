package middleware

import (
	"context"
	"fmt"
	"github.com/autom8ter/wolverine/core"
)

type coreWrapper struct {
	persist      PersistFunc
	aggregate    AggregateFunc
	query        QueryFunc
	scan         ScanFunc
	changeStream ChangeStreamFunc
	close        CloseFunc
}

func (c coreWrapper) Scan(ctx context.Context, collection *core.Collection, scan core.Scan, scanner core.ScanFunc) error {
	return c.scan(ctx, collection, scan, scanner)
}

func (c coreWrapper) Close(ctx context.Context) error {
	if c.close == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.close(ctx)
}

func (c coreWrapper) Persist(ctx context.Context, collection *core.Collection, change core.StateChange) error {
	if c.persist == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.persist(ctx, collection, change)
}

func (c coreWrapper) Aggregate(ctx context.Context, collection *core.Collection, query core.AggregateQuery) (core.Page, error) {
	if c.aggregate == nil {
		return core.Page{}, fmt.Errorf("unimplemented")
	}
	return c.aggregate(ctx, collection, query)
}

func (c coreWrapper) Query(ctx context.Context, collection *core.Collection, query core.Query) (core.Page, error) {
	if c.query == nil {
		return core.Page{}, fmt.Errorf("unimplemented")
	}
	return c.query(ctx, collection, query)
}

func (c coreWrapper) ChangeStream(ctx context.Context, collection *core.Collection, fn core.ChangeStreamHandler) error {
	if c.changeStream == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.changeStream(ctx, collection, fn)
}
