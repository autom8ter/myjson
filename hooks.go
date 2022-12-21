package gokvkit

import (
	"context"
)

// OnPersist is a hook function triggered whenever a command is persisted
type OnPersist struct {
	// Name is the name of the hook
	Name string `validate:"required"`
	// Before indicates whether the hook should execute before or after the command is persisted
	Before bool
	// Func is the function to execute
	Func func(ctx context.Context, tx Tx, command *Command) error `validate:"required"`
}

// OnInit is a hook function triggered whenever the database starts
type OnInit struct {
	// Name is the name of the hook
	Name string `validate:"required"`
	// Func is the function to execute
	Func func(ctx context.Context, db *DB) error `validate:"required"`
}

// OnCommit is a hook function triggered before a transaction is commited
type OnCommit struct {
	// Name is the name of the hook
	Name string `validate:"required"`
	// Before indicates whether the hook should execute before or after the transaction is commited
	Before bool
	// Func is the function to execute
	Func func(ctx context.Context, tx Tx) error `validate:"required"`
}

// OnRollback is a hook function triggered whenever a transaction is rolled back
type OnRollback struct {
	// Name is the name of the hook
	Name string `validate:"required"`
	// Before indicates whether the hook should execute before or after the transaction is rolled back
	Before bool
	// Func is the function to execute
	Func func(ctx context.Context, tx Tx) `validate:"required"`
}
