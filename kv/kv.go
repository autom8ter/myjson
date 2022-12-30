package kv

import "time"

// DB is a key value database implementation
type DB interface {
	// Tx executes the given function against a database transaction
	Tx(readOnly bool, fn func(Tx) error) error
	// NewTx creates a new database transaction.
	NewTx(readOnly bool) Tx
	// NewBatch returns a batch kv writer
	NewBatch() Batch
	// NewLocker returns a mutex/locker with the given lease duration
	NewLocker(key []byte, leaseInterval time.Duration) Locker
	// DropPrefix drops keys with the given prefix(s) from the database
	DropPrefix(prefix ...[]byte) error
	// Close closes the key value database
	Close() error
}

// IterOpts are options when creating an iterator
type IterOpts struct {
	// Prefix is the key prefix to return
	Prefix []byte `json:"prefix"`
	// Seek seeks to the given bytes
	Seek []byte `json:"seek"`
	// Reverse scans the index in reverse
	Reverse bool `json:"reverse"`
}

// Tx is a database transaction interface
type Tx interface {
	// Getter gets the specified key in the database(if it exists)
	Getter
	// Mutator executes mutations against the database
	Mutator
	// NewIterator creates a new iterator
	NewIterator(opts IterOpts) Iterator
	// Commit commits the transaction
	Commit() error
	// Rollback rolls back any changes made by the transaction
	Rollback()
	// Close closes the transaction
	Close()
}

// Getter gets the specified key in the database(if it exists). If the key does not exist, a nil byte slice and no error is returned
type Getter interface {
	Get(key []byte) ([]byte, error)
}

// Iterator is a key value database iterator. Keys should be sorted lexicographically.
type Iterator interface {
	// Seek seeks to the given key
	Seek(key []byte)
	// Close closes the iterator
	Close()
	// Valid returns true if the iterator is still valid
	Valid() bool
	// Item returns the item at the current cursor position
	Item() Item
	// Next iterates to the next item
	Next()
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
	Set(key, value []byte, ttl time.Duration) error
}

// Deleter deletes specified keys from the database
type Deleter interface {
	Delete(key []byte) error
}

// Mutator executes mutations against the database
type Mutator interface {
	// Setter sets specified key/value in the database
	Setter
	// Deleter deletes specified keys from the database
	Deleter
}

// Batch is a batch write operation for persisting 1-many changes to the database
type Batch interface {
	// Flush flushes the batch to the database - it should be called after all Set(s)/Delete(s)
	Flush() error
	// Mutator executes mutations against the database
	Mutator
}

type Locker interface {
	TryLock() (bool, error)
	Unlock()
	IsLocked() (bool, error)
}

// KVConfig configures a key value database from the given provider
type KVConfig struct {
	// Provider is the name of the kv provider (badger)
	Provider string `json:"provider"`
	// Params are the kv providers params
	Params map[string]any `json:"params"`
}
