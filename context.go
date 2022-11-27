package gokvkit

import (
	"context"
	"encoding/json"
	"sync"
)

type ctxKey int

const (
	metadataKey ctxKey = 0
)

// Context holds key value pairs associated with a go Context
type Context struct {
	tags sync.Map
}

// NewContext creates a context with the given tags
func NewContext(tags map[string]any) *Context {
	m := &Context{}
	if tags != nil {
		m.SetAll(tags)
	}
	return m
}

// String return a json string of the context
func (m *Context) String() string {
	bits, _ := m.MarshalJSON()
	return string(bits)
}

// MarshalJSON returns the context values as json bytes
func (m *Context) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Map())
}

// UnmarshalJSON decodes the metadata from json bytes
func (m *Context) UnmarshalJSON(bytes []byte) error {
	data := map[string]any{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}
	m.SetAll(data)
	return nil
}

// SetAll sets the key value fields on the context
func (m *Context) SetAll(data map[string]any) {
	for k, v := range data {
		m.tags.Store(k, v)
	}
}

// Set sets a key value pair on the context
func (m *Context) Set(key string, value any) {
	m.SetAll(map[string]any{
		key: value,
	})
}

// Del deletes a key from the context
func (m *Context) Del(key string) {
	m.tags.Delete(key)
}

// Get gets a key from the context if it exists
func (m *Context) Get(key string) (any, bool) {
	return m.tags.Load(key)
}

// Exists returns true if the key exists in the context
func (m *Context) Exists(key string) bool {
	_, ok := m.tags.Load(key)
	return ok
}

// Map returns the context keyvalues as a map
func (m *Context) Map() map[string]any {
	data := map[string]any{}
	m.tags.Range(func(key, value any) bool {
		data[key.(string)] = value
		return true
	})
	return data
}

// ToContext adds the context to the input go context
func (m *Context) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, metadataKey, m)
}

// GetContext gets metadata from the context if it exists
func GetContext(ctx context.Context) (*Context, bool) {
	m, ok := ctx.Value(metadataKey).(*Context)
	if ok {
		return m, true
	}
	return &Context{}, false
}
