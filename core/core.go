package core

import (
	"context"
	"github.com/autom8ter/wolverine/schema"
	"io"
)

type Core struct {
	Persist      PersistFunc
	Aggregate    AggregateFunc
	Search       SearchFunc
	Query        QueryFunc
	Get          GetFunc
	GetAll       GetAllFunc
	ChangeStream ChangeStreamFunc
	Close        CloseFunc
	Backup       BackupFunc
	Restore      RestoreFunc
}

type Middleware struct {
	Persist      []PersistWare
	Aggregate    []AggregateWare
	Search       []SearchWare
	Query        []QueryWare
	Get          []GetWare
	GetAll       []GetAllWare
	ChangeStream []ChangeStreamWare
}

func (c Core) Apply(m Middleware) Core {
	core := Core{
		Persist:      c.Persist,
		Aggregate:    c.Aggregate,
		Search:       c.Search,
		Query:        c.Query,
		Get:          c.Get,
		GetAll:       c.GetAll,
		ChangeStream: c.ChangeStream,
		Close:        c.Close,
		Backup:       c.Backup,
		Restore:      c.Restore,
	}
	if m.Persist != nil {
		for _, m := range m.Persist {
			core.Persist = m(core.Persist)
		}
	}
	if m.Aggregate != nil {
		for _, m := range m.Aggregate {
			core.Aggregate = m(core.Aggregate)
		}
	}
	if m.Search != nil {
		for _, m := range m.Search {
			core.Search = m(core.Search)
		}
	}
	if m.Query != nil {
		for _, m := range m.Query {
			core.Query = m(core.Query)
		}
	}
	if m.Get != nil {
		for _, m := range m.Get {
			core.Get = m(core.Get)
		}
	}
	if m.GetAll != nil {
		for _, m := range m.GetAll {
			core.GetAll = m(core.GetAll)
		}
	}
	if m.ChangeStream != nil {
		for _, m := range m.ChangeStream {
			core.ChangeStream = m(core.ChangeStream)
		}
	}
	return core
}

// PersistFunc persists changes to a collection
type PersistFunc func(ctx context.Context, collection *schema.Collection, change schema.StateChange) error

// PersistWare wraps a PersistFunc and returns a new one
type PersistWare func(PersistFunc) PersistFunc

// AggregateFunc aggregates documents to a collection
type AggregateFunc func(ctx context.Context, collection *schema.Collection, query schema.AggregateQuery) (schema.Page, error)

// AggregateWare wraps a AggregateFunc and returns a new one
type AggregateWare func(AggregateFunc) AggregateFunc

// SearchFunc searches documents in a collection
type SearchFunc func(ctx context.Context, collection *schema.Collection, query schema.SearchQuery) (schema.Page, error)

// SearchWare wraps a SearchFunc and returns a new one
type SearchWare func(SearchFunc) SearchFunc

// QueryFunc queries documents in a collection
type QueryFunc func(ctx context.Context, collection *schema.Collection, query schema.Query) (schema.Page, error)

// QueryWare wraps a QueryFunc and returns a new one
type QueryWare func(QueryFunc) QueryFunc

// GetFunc gets documents in a collection
type GetFunc func(ctx context.Context, collection *schema.Collection, id string) (schema.Document, error)

// GetWare wraps a GetFunc and returns a new one
type GetWare func(GetFunc) GetFunc

// GetAllFunc gets multiple documents in a collection
type GetAllFunc func(ctx context.Context, collection *schema.Collection, ids []string) ([]schema.Document, error)

// GetAllWare wraps a GetAllFunc and returns a new one
type GetAllWare func(GetAllFunc) GetAllFunc

// ChangeStreamFunc listens to changes in a ccollection
type ChangeStreamFunc func(ctx context.Context, collection *schema.Collection, fn schema.ChangeStreamHandler) error

// ChangeStreamWare wraps a ChangeStreamFunc and returns a new one
type ChangeStreamWare func(ChangeStreamFunc) ChangeStreamFunc

// Close closes the runtime
type CloseFunc func(ctx context.Context) error

// RestoreFunc restores data from the provided reader
type RestoreFunc func(ctx context.Context, r io.Reader) error

// BackupFunc backs up data into the provided writer
type BackupFunc func(ctx context.Context, w io.Writer, since uint64) error
