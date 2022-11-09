package badger

type item struct {
	key   []byte
	value func() ([]byte, error)
}

func (i item) Key() []byte {
	return i.key
}

func (i item) Value() ([]byte, error) {
	return i.value()
}
