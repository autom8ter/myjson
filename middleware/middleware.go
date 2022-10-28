package middleware

import (
	"context"
	"github.com/autom8ter/wolverine/core"
)

// Middleware is a set of wrapper functions that alter core functionality
type Middleware struct {
	Persist      PersistWare
	Aggregate    AggregateWare
	Query        QueryWare
	ChangeStream ChangeStreamWare
	Scan         ScanWare
}

// ApplyMiddleware applies the middleware to the coreWrapper and returns a new Core instance
func ApplyCoreMiddleware(c core.CoreAPI, m Middleware) core.CoreAPI {
	wrapped := coreWrapper{}
	if m.Persist != nil {
		wrapped.persist = m.Persist(c.Persist)
	}
	if m.Aggregate != nil {
		wrapped.aggregate = m.Aggregate(c.Aggregate)
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
type PersistFunc func(ctx context.Context, collection *core.Collection, change core.StateChange) error

// PersistWare wraps a PersistFunc and returns a new one
type PersistWare func(PersistFunc) PersistFunc

// AggregateFunc aggregates documents to a collection
type AggregateFunc func(ctx context.Context, collection *core.Collection, query core.AggregateQuery) (core.Page, error)

// AggregateWare wraps a AggregateFunc and returns a new one
type AggregateWare func(AggregateFunc) AggregateFunc

// QueryFunc queries documents in a collection
type QueryFunc func(ctx context.Context, collection *core.Collection, query core.Query) (core.Page, error)

// QueryWare wraps a QueryFunc and returns a new one
type QueryWare func(QueryFunc) QueryFunc

// ScanFunc queries documents in a collection
type ScanFunc func(ctx context.Context, collection *core.Collection, scan core.Scan, scanner core.ScanFunc) error

// ScanWare wraps a ScanFunc and returns a new one
type ScanWare func(ScanFunc) ScanFunc

// ChangeStreamFunc listens to changes in a ccollection
type ChangeStreamFunc func(ctx context.Context, collection *core.Collection, fn core.ChangeStreamHandler) error

// ChangeStreamWare wraps a ChangeStreamFunc and returns a new one
type ChangeStreamWare func(ChangeStreamFunc) ChangeStreamFunc

// Close closes the runtime
type CloseFunc func(ctx context.Context) error
