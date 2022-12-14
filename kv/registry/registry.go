package registry

import (
	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/kv"
)

// KVDBOpener opens a key value database
type KVDBOpener func(params map[string]interface{}) (kv.DB, error)

var registeredOpeners = map[string]KVDBOpener{}

// Register registers a KVDBOpener opener by name
func Register(name string, opener KVDBOpener) {
	registeredOpeners[name] = opener
}

// Open opens a registered key value database
func Open(name string, params map[string]interface{}) (kv.DB, error) {
	opener, ok := registeredOpeners[name]
	if !ok {
		return nil, errors.New(errors.NotFound, "%s is not registered", name)
	}
	return opener(params)
}
