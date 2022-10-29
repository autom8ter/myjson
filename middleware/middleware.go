package middleware

import (
	"context"
	"fmt"
	"github.com/autom8ter/wolverine"
)

// Middleware is a set of wrapper functions that alter core functionality
type Middleware struct {
	Persist      PersistWare
	Query        QueryWare
	ChangeStream ChangeStreamWare
	Scan         ScanWare
}

// applies the middleware to the coreWrapper and returns a new Core instance
func (m Middleware) Apply(c wolverine.CoreAPI) wolverine.CoreAPI {
	return applyCoreMiddleware(c, m)
}

func applyCoreMiddleware(c wolverine.CoreAPI, m Middleware) wolverine.CoreAPI {
	wrapped := coreWrapper{}
	if m.Persist != nil {
		wrapped.persist = m.Persist(c.Persist)
	}
	if m.Query != nil {
		wrapped.query = m.Query(c.Query)
	}
	if m.ChangeStream != nil {
		wrapped.changeStream = m.ChangeStream(c.ChangeStream)
	}
	if m.Scan != nil {
		wrapped.scan = m.Scan(c.Scan)
	}
	return wrapped
}

// PersistFunc persists changes to a collection
type PersistFunc func(ctx context.Context, collection *wolverine.Collection, change wolverine.StateChange) error

// PersistWare wraps a PersistFunc and returns a new one
type PersistWare func(PersistFunc) PersistFunc

// QueryFunc queries documents in a collection
type QueryFunc func(ctx context.Context, collection *wolverine.Collection, query wolverine.Query) (wolverine.Page, error)

// QueryWare wraps a QueryFunc and returns a new one
type QueryWare func(QueryFunc) QueryFunc

// ScanFunc queries documents in a collection
type ScanFunc func(ctx context.Context, collection *wolverine.Collection, scan wolverine.Scan, scanner wolverine.ScanFunc) error

// ScanWare wraps a ScanFunc and returns a new one
type ScanWare func(ScanFunc) ScanFunc

// ChangeStreamFunc listens to changes in a ccollection
type ChangeStreamFunc func(ctx context.Context, collection *wolverine.Collection, fn wolverine.ChangeStreamHandler) error

// ChangeStreamWare wraps a ChangeStreamFunc and returns a new one
type ChangeStreamWare func(ChangeStreamFunc) ChangeStreamFunc

// Close closes the runtime
type CloseFunc func(ctx context.Context) error

type coreWrapper struct {
	persist      PersistFunc
	query        QueryFunc
	scan         ScanFunc
	changeStream ChangeStreamFunc
	close        CloseFunc
}

func (c coreWrapper) Scan(ctx context.Context, collection *wolverine.Collection, scan wolverine.Scan, scanner wolverine.ScanFunc) error {
	return c.scan(ctx, collection, scan, scanner)
}

func (c coreWrapper) Close(ctx context.Context) error {
	if c.close == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.close(ctx)
}

func (c coreWrapper) Persist(ctx context.Context, collection *wolverine.Collection, change wolverine.StateChange) error {
	if c.persist == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.persist(ctx, collection, change)
}

func (c coreWrapper) Query(ctx context.Context, collection *wolverine.Collection, query wolverine.Query) (wolverine.Page, error) {
	if c.query == nil {
		return wolverine.Page{}, fmt.Errorf("unimplemented")
	}
	return c.query(ctx, collection, query)
}

func (c coreWrapper) ChangeStream(ctx context.Context, collection *wolverine.Collection, fn wolverine.ChangeStreamHandler) error {
	if c.changeStream == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.changeStream(ctx, collection, fn)
}
