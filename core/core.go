package core

import (
	"context"
	"fmt"
	"io"
)

// CoreAPI is the core api powering database functionality
type CoreAPI interface {
	//  Persist persists changes to a collection
	Persist(ctx context.Context, collection *Collection, change StateChange) error
	// Aggregate aggregates documents in the database
	Aggregate(ctx context.Context, collection *Collection, query AggregateQuery) (Page, error)
	// Search executes a full text search
	Search(ctx context.Context, collection *Collection, query SearchQuery) (Page, error)
	// Query queries for documents
	Query(ctx context.Context, collection *Collection, query Query) (Page, error)
	// Get gets a single document
	Get(ctx context.Context, collection *Collection, id string) (*Document, error)
	// Get gets a list of documents
	GetAll(ctx context.Context, collection *Collection, ids []string) ([]*Document, error)
	// ChangeStream streams state changes to the given function
	ChangeStream(ctx context.Context, collection *Collection, fn ChangeStreamHandler) error
	// Backup backs up the database to the given writer
	Backup(ctx context.Context, w io.Writer, since uint64) error
	// Restore restores the database from the given reader
	Restore(ctx context.Context, r io.Reader) error
	// Close closes the database
	Close(ctx context.Context) error
}

// Core implements the CoreAPI interface
type Core struct {
	persist      PersistFunc
	aggregate    AggregateFunc
	search       SearchFunc
	query        QueryFunc
	get          GetFunc
	getAll       GetAllFunc
	changeStream ChangeStreamFunc
	close        CloseFunc
	backup       BackupFunc
	restore      RestoreFunc
}

// WithPersist sets the persist func
func (c Core) WithPersist(fn PersistFunc) Core {
	c.persist = fn
	return c
}

// WithAggregate sets the aggregate func
func (c Core) WithAggregate(fn AggregateFunc) Core {
	c.aggregate = fn
	return c
}

// WithSearch sets the search func
func (c Core) WithSearch(fn SearchFunc) Core {
	c.search = fn
	return c
}

// WithQuery sets the query func
func (c Core) WithQuery(fn QueryFunc) Core {
	c.query = fn
	return c
}

// WithGet sets the get func
func (c Core) WithGet(fn GetFunc) Core {
	c.get = fn
	return c
}

// WithGetAll sets the get all func
func (c Core) WithGetAll(fn GetAllFunc) Core {
	c.getAll = fn
	return c
}

// WithChangeStream sets the change stream func
func (c Core) WithChangeStream(fn ChangeStreamFunc) Core {
	c.changeStream = fn
	return c
}

// WithClose sets the close func
func (c Core) WithClose(fn CloseFunc) Core {
	c.close = fn
	return c
}

// WithBackup sets the backupfunc
func (c Core) WithBackup(fn BackupFunc) Core {
	c.backup = fn
	return c
}

// WithRestore sets the restorefunc
func (c Core) WithRestore(fn RestoreFunc) Core {
	c.restore = fn
	return c
}

// Backup backs up the database
func (c Core) Backup(ctx context.Context, w io.Writer, since uint64) error {
	if c.backup == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.backup(ctx, w, since)
}

// Restore restores the database from the reader
func (c Core) Restore(ctx context.Context, r io.Reader) error {
	if c.restore == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.restore(ctx, r)
}

// Close closes the database
func (c Core) Close(ctx context.Context) error {
	if c.close == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.close(ctx)
}

// Persist persists a state change. Persist is the only way to execute a mutation
func (c Core) Persist(ctx context.Context, collection *Collection, change StateChange) error {
	if c.persist == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.persist(ctx, collection, change)
}

// Aggregate aggregates documents- the return type is not compatible with the collection's schema
func (c Core) Aggregate(ctx context.Context, collection *Collection, query AggregateQuery) (Page, error) {
	if c.aggregate == nil {
		return Page{}, fmt.Errorf("unimplemented")
	}
	return c.aggregate(ctx, collection, query)
}

// Search executes a full text search
func (c Core) Search(ctx context.Context, collection *Collection, query SearchQuery) (Page, error) {
	if c.search == nil {
		return Page{}, fmt.Errorf("unimplemented")
	}
	return c.search(ctx, collection, query)
}

// Query queries a set of documents
func (c Core) Query(ctx context.Context, collection *Collection, query Query) (Page, error) {
	if c.query == nil {
		return Page{}, fmt.Errorf("unimplemented")
	}
	return c.query(ctx, collection, query)
}

// Get gets a document by id
func (c Core) Get(ctx context.Context, collection *Collection, id string) (*Document, error) {
	if c.get == nil {
		return nil, fmt.Errorf("unimplemented")
	}
	return c.get(ctx, collection, id)
}

// GetAll gets all documents by id
func (c Core) GetAll(ctx context.Context, collection *Collection, ids []string) ([]*Document, error) {
	if c.getAll == nil {
		return nil, fmt.Errorf("unimplemented")
	}
	return c.getAll(ctx, collection, ids)
}

// ChangeStream streams state changes to the provided function
func (c Core) ChangeStream(ctx context.Context, collection *Collection, fn ChangeStreamHandler) error {
	if c.changeStream == nil {
		return fmt.Errorf("unimplemented")
	}
	return c.changeStream(ctx, collection, fn)
}

// Middleware is a set of wrapper functions that alter core functionality
type Middleware struct {
	Persist      []PersistWare
	Aggregate    []AggregateWare
	Search       []SearchWare
	Query        []QueryWare
	Get          []GetWare
	GetAll       []GetAllWare
	ChangeStream []ChangeStreamWare
}

// Apply applies the middleware and returns a new Core instance
func (c Core) Apply(m Middleware) Core {
	core := Core{
		persist:      c.persist,
		aggregate:    c.aggregate,
		search:       c.search,
		query:        c.query,
		get:          c.get,
		getAll:       c.getAll,
		changeStream: c.changeStream,
		close:        c.close,
		backup:       c.backup,
		restore:      c.restore,
	}
	if m.Persist != nil {
		for _, m := range m.Persist {
			core.persist = m(core.persist)
		}
	}
	if m.Aggregate != nil {
		for _, m := range m.Aggregate {
			core.aggregate = m(core.aggregate)
		}
	}
	if m.Search != nil {
		for _, m := range m.Search {
			core.search = m(core.search)
		}
	}
	if m.Query != nil {
		for _, m := range m.Query {
			core.query = m(core.query)
		}
	}
	if m.Get != nil {
		for _, m := range m.Get {
			core.get = m(core.get)
		}
	}
	if m.GetAll != nil {
		for _, m := range m.GetAll {
			core.getAll = m(core.getAll)
		}
	}
	if m.ChangeStream != nil {
		for _, m := range m.ChangeStream {
			core.changeStream = m(core.changeStream)
		}
	}
	return core
}

// PersistFunc persists changes to a collection
type PersistFunc func(ctx context.Context, collection *Collection, change StateChange) error

// PersistWare wraps a PersistFunc and returns a new one
type PersistWare func(PersistFunc) PersistFunc

// AggregateFunc aggregates documents to a collection
type AggregateFunc func(ctx context.Context, collection *Collection, query AggregateQuery) (Page, error)

// AggregateWare wraps a AggregateFunc and returns a new one
type AggregateWare func(AggregateFunc) AggregateFunc

// SearchFunc searches documents in a collection
type SearchFunc func(ctx context.Context, collection *Collection, query SearchQuery) (Page, error)

// SearchWare wraps a SearchFunc and returns a new one
type SearchWare func(SearchFunc) SearchFunc

// QueryFunc queries documents in a collection
type QueryFunc func(ctx context.Context, collection *Collection, query Query) (Page, error)

// QueryWare wraps a QueryFunc and returns a new one
type QueryWare func(QueryFunc) QueryFunc

// GetFunc gets documents in a collection
type GetFunc func(ctx context.Context, collection *Collection, id string) (*Document, error)

// GetWare wraps a GetFunc and returns a new one
type GetWare func(GetFunc) GetFunc

// GetAllFunc gets multiple documents in a collection
type GetAllFunc func(ctx context.Context, collection *Collection, ids []string) ([]*Document, error)

// GetAllWare wraps a GetAllFunc and returns a new one
type GetAllWare func(GetAllFunc) GetAllFunc

// ChangeStreamFunc listens to changes in a ccollection
type ChangeStreamFunc func(ctx context.Context, collection *Collection, fn ChangeStreamHandler) error

// ChangeStreamWare wraps a ChangeStreamFunc and returns a new one
type ChangeStreamWare func(ChangeStreamFunc) ChangeStreamFunc

// Close closes the runtime
type CloseFunc func(ctx context.Context) error

// RestoreFunc restores data from the provided reader
type RestoreFunc func(ctx context.Context, r io.Reader) error

// BackupFunc backs up data into the provided writer
type BackupFunc func(ctx context.Context, w io.Writer, since uint64) error
