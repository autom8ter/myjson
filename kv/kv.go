package kv

type DB interface {
	Tx(isUpdate bool, fn func(Tx) error) error
	Batch() Batch
	Close() error
}

type IterOpts struct {
	Prefix  []byte
	Seek    []byte
	Reverse bool
}

type Tx interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	NewIterator(opts IterOpts) Iterator
}

type Iterator interface {
	Seek(key []byte)
	Close()
	Valid() bool
	Item() Item
	Next()
}

type Item interface {
	Key() []byte
	Value() ([]byte, error)
}

type Batch interface {
	Flush() error
	Set(key, value []byte) error
	Delete(key []byte) error
}
