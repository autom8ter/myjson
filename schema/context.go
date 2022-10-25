package schema

import (
	"context"
	"encoding/json"
	"sync"
)

type ctxKey int

const (
	metadataKey ctxKey = 0
)

type Context struct {
	sync.RWMutex
	tags sync.Map
}

func NewContext(tags map[string]any) *Context {
	m := &Context{}
	if tags != nil {
		m.SetAll(tags)
	}
	return m
}

func (m *Context) String() string {
	bits, _ := m.MarshalJSON()
	return string(bits)
}

func (m *Context) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Map())
}

func (m *Context) UnmarshalJSON(bytes []byte) error {
	data := map[string]any{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}
	m.SetAll(data)
	return nil
}

func (m *Context) SetAll(data map[string]any) {
	for k, v := range data {
		m.tags.Store(k, v)
	}
}

func (m *Context) Set(key string, value any) {
	m.tags.Store(key, value)
}

func (m *Context) Del(key string) {
	m.tags.Delete(key)
}

func (m *Context) Get(key string) (any, bool) {
	return m.tags.Load(key)
}

func (m *Context) Exists(key string) bool {
	_, ok := m.tags.Load(key)
	return ok
}

func (m *Context) Map() map[string]any {
	data := map[string]any{}
	m.tags.Range(func(key, value any) bool {
		data[key.(string)] = value
		return true
	})
	return data
}

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
