package badger

import "github.com/dgraph-io/badger/v3"

type item struct {
	item *badger.Item
}

func (i item) Key() []byte {
	return i.Key()
}

func (i item) Value() ([]byte, error) {
	return i.item.ValueCopy(nil)
}
