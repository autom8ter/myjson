package kv

import (
	"context"
	"time"
)

// DB is a key value database implementation
type DB interface {
	// Tx executes the given function against a database transaction
	Tx(opts TxOpts, fn func(Tx) error) error
	// NewTx creates a new database transaction.
	NewTx(opts TxOpts) (Tx, error)
	// NewLocker returns a mutex/locker with the given lease duration
	NewLocker(key []byte, leaseInterval time.Duration) (Locker, error)
	// DropPrefix drops keys with the given prefix(s) from the database
	DropPrefix(ctx context.Context, prefix ...[]byte) error
	ChangeStreamer
	// Close closes the key value database
	Close(ctx context.Context) error
}

// IterOpts are options when creating an iterator
type IterOpts struct {
	// Prefix indicates that keys must match the given prefix
	Prefix []byte `json:"prefix"`
	// UpperBound indicates that keys must be <= the upper bound
	UpperBound []byte `json:"upperBound"`
	// Seek seeks to the given bytes before beginning to iterate
	Seek []byte `json:"seek"`
	// Reverse scans the index in reverse
	Reverse bool `json:"reverse"`
}

// TxOpts are options when creating a database transaction
type TxOpts struct {
	IsReadOnly bool `json:"isReadOnly,omitempty"`
	IsBatch    bool `json:"isBatch,omitempty"`
}

// Tx is a database transaction interface
type Tx interface {
	// Getter gets the specified key in the database(if it exists)
	Getter
	// Mutator executes mutations against the database
	Mutator
	// NewIterator creates a new iterator
	NewIterator(opts IterOpts) (Iterator, error)
	// Commit commits the transaction
	Commit(ctx context.Context) error
	// Rollback rolls back any changes made by the transaction
	Rollback(ctx context.Context) error
	// Close closes the transaction
	Close(ctx context.Context)
}

// Getter gets the specified key in the database(if it exists). If the key does not exist, a nil byte slice and no error is returned
type Getter interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
}

// Iterator is a key value database iterator. Keys should be sorted lexicographically.
type Iterator interface {
	// Seek seeks to the given key
	Seek(key []byte)
	// Close closes the iterator
	Close()
	// Valid returns true if the iterator is still valid
	Valid() bool
	// Key returns the key at the current cursor position
	Key() []byte
	// Value returns the value at the current cursor position
	Value() ([]byte, error)
	// Next iterates to the next item
	Next() error
}

// Item is a key value pair in the database
type Item interface {
	// Key is the items key - it is a unique identifier for it's value
	Key() []byte
	// Value is the bytes that correspond to the item's key
	Value() ([]byte, error)
}

// Setter sets specified key/value in the database. If ttl is empty, the key should never expire
type Setter interface {
	Set(ctx context.Context, key, value []byte) error
}

// Deleter deletes specified keys from the database
type Deleter interface {
	Delete(ctx context.Context, key []byte) error
}

// Mutator executes mutations against the database
type Mutator interface {
	// Setter sets specified key/value in the database
	Setter
	// Deleter deletes specified keys from the database
	Deleter
}

type Locker interface {
	TryLock(ctx context.Context) (bool, error)
	Unlock()
	IsLocked(ctx context.Context) (bool, error)
}

// KVConfig configures a key value database from the given provider
type KVConfig struct {
	// Provider is the name of the kv provider (badger)
	Provider string `json:"provider"`
	// Params are the kv providers params
	Params map[string]any `json:"params"`
}

// TxOp is an transaction operation type
type TxOp string

const (
	// SETOP sets a key value pair
	SETOP TxOp = "SET"
	// DELOP deletes a key value pair
	DELOP TxOp = "DEL"
)

// CDC or change data capture holds an transaction operation type and a key value pair.
// It is used as a container for streaming changes to key value pairs
type CDC struct {
	Operation TxOp   `json:"operation"`
	Key       []byte `json:"key"`
	Value     []byte `json:"value,omitempty"`
}

type ChangeStreamHandler func(cdc CDC) (bool, error)

type ChangeStreamer interface {
	ChangeStream(ctx context.Context, prefix []byte, fn ChangeStreamHandler) error
}
