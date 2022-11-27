package kv

// DB is a key value database implementation
type DB interface {
	// Tx executes the given function against a database transaction
	Tx(isUpdate bool, fn func(Tx) error) error
	// Batch returns a batch kv writer
	Batch() Batch
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
	// Get gets the specified key in the database(if it exists)
	Get(key []byte) ([]byte, error)
	// Set sets the specified key/value in the database
	Set(key, value []byte) error
	// Delete deletes the specified key from the database
	Delete(key []byte) error
	// NewIterator creates a new iterator
	NewIterator(opts IterOpts) Iterator
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

// Batch is a batch write operation for persisting 1-many changes to the database
type Batch interface {
	// Flush flushes the batch to the database - it should be called after all Set(s)/Delete(s)
	Flush() error
	Set(key, value []byte) error
	Delete(key []byte) error
}

// KVConfig configures a key value database from the given provider
type KVConfig struct {
	// Provider is the name of the kv provider (badger)
	Provider string `json:"provider"`
	// Params are the kv providers params
	Params map[string]any `json:"params"`
}
