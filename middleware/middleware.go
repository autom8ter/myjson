package middleware

import (
	"context"
	"fmt"
	"github.com/autom8ter/wolverine"
)

// Middleware is a set of wrapper functions that alter core database functionality
type Middleware struct {
	Persist        PersistWare
	ChangeStream   ChangeStreamWare
	Scan           ScanWare
	SetCollections SetCollectionsWare
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
	if m.ChangeStream != nil {
		wrapped.changeStream = m.ChangeStream(c.ChangeStream)
	}
	if m.Scan != nil {
		wrapped.scan = m.Scan(c.Scan)
	}
	if m.SetCollections != nil {
		wrapped.setCollections = m.SetCollections(c.SetCollections)
	}
	return wrapped
}

// SetCollectionsFunc sets collections in a database
type SetCollectionsFunc func(ctx context.Context, collections []*wolverine.Collection) error

// SetCollectionsWare wraps a SetCollectionsFunc and returns a new one
type SetCollectionsWare func(collectionsFunc SetCollectionsFunc) SetCollectionsFunc

// PersistFunc persists changes to a collection
type PersistFunc func(ctx context.Context, collection string, change wolverine.StateChange) error

// PersistWare wraps a PersistFunc and returns a new one
type PersistWare func(PersistFunc) PersistFunc

// ScanFunc queries documents in a collection
type ScanFunc func(ctx context.Context, collection string, scan wolverine.Scan, scanner wolverine.ScanFunc) (wolverine.IndexMatch, error)

// ScanWare wraps a ScanFunc and returns a new one
type ScanWare func(ScanFunc) ScanFunc

// ChangeStreamFunc listens to changes in a ccollection
type ChangeStreamFunc func(ctx context.Context, collection string, fn wolverine.ChangeStreamHandler) error

// ChangeStreamWare wraps a ChangeStreamFunc and returns a new one
type ChangeStreamWare func(ChangeStreamFunc) ChangeStreamFunc

// Close closes the runtime
type CloseFunc func(ctx context.Context) error

type coreWrapper struct {
	persist        PersistFunc
	scan           ScanFunc
	changeStream   ChangeStreamFunc
	setCollections SetCollectionsFunc
	close          CloseFunc
}

func (c coreWrapper) Scan(ctx context.Context, collection string, scan wolverine.Scan, scanner wolverine.ScanFunc) (wolverine.IndexMatch, error) {
	return c.scan(ctx, collection, scan, scanner)
}

func (c coreWrapper) Close(ctx context.Context) error {
	if c.close == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.close(ctx)
}

func (c coreWrapper) Persist(ctx context.Context, collection string, change wolverine.StateChange) error {
	if c.persist == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.persist(ctx, collection, change)
}

func (c coreWrapper) ChangeStream(ctx context.Context, collection string, fn wolverine.ChangeStreamHandler) error {
	if c.changeStream == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.changeStream(ctx, collection, fn)
}

func (c coreWrapper) SetCollections(ctx context.Context, collections []*wolverine.Collection) error {
	if c.setCollections == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.setCollections(ctx, collections)
}
