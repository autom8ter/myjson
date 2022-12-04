package gokvkit

import (
	"context"
	"github.com/autom8ter/gokvkit/model"
	"github.com/palantir/stacktrace"
)

// OnPersist is a hook function triggered whenever a command is persisted
type OnPersist struct {
	// Name is the name of the hook
	Name string
	// Before indicates whether the hook should execute before or after the command is persisted
	Before bool
	// Func is the function to execute
	Func func(ctx context.Context, db *DB, command *model.Command) error
}

// Valid returns nil if the hook is valid
func (v OnPersist) Valid() error {
	if v.Name == "" {
		return stacktrace.NewError("empty hook name")
	}
	if v.Func == nil {
		return stacktrace.NewError("empty hook function")
	}
	return nil
}

// OnWhere is a hook function triggered before queries/scans are executed. They may be used for a varietey of purposes (ex: query authorization hooks)
type OnWhere struct {
	Name string
	Func func(ctx context.Context, db *DB, where []model.QueryJsonWhereElem) ([]model.QueryJsonWhereElem, error)
}

// Valid returns nil if the hook is valid
func (v OnWhere) Valid() error {
	if v.Name == "" {
		return stacktrace.NewError("empty hook name")
	}
	if v.Func == nil {
		return stacktrace.NewError("empty hook function")
	}
	return nil
}

// OnRead is a hook function triggered on each passing result of a read-based request
type OnRead struct {
	Name string
	Func func(ctx context.Context, db *DB, document *model.Document) (*model.Document, error)
}

// Valid returns nil if the hook is valid
func (v OnRead) Valid() error {
	if v.Name == "" {
		return stacktrace.NewError("empty hook name")
	}
	if v.Func == nil {
		return stacktrace.NewError("empty hook function")
	}
	return nil
}

// OnInit is a hook function triggered whenever the database starts
type OnInit struct {
	Name string
	Func func(ctx context.Context, db *DB) error
}

// Valid returns nil if the hook is valid
func (v OnInit) Valid() error {
	if v.Name == "" {
		return stacktrace.NewError("empty hook name")
	}
	if v.Func == nil {
		return stacktrace.NewError("empty hook function")
	}
	return nil
}
