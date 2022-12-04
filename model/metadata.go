package model

import (
	"context"
	"encoding/json"
	"sync"
)

type ctxKey int

const (
	metadataKey ctxKey = 0
)

// Metadata holds key value pairs associated with a go Context
type Metadata struct {
	tags sync.Map
}

// NewMetadata creates a Metadata with the given tags
func NewMetadata(tags map[string]any) *Metadata {
	m := &Metadata{}
	if tags != nil {
		m.SetAll(tags)
	}
	return m
}

// String return a json string of the context
func (m *Metadata) String() string {
	bits, _ := m.MarshalJSON()
	return string(bits)
}

// MarshalJSON returns the metadata values as json bytes
func (m *Metadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Map())
}

// UnmarshalJSON decodes the metadata from json bytes
func (m *Metadata) UnmarshalJSON(bytes []byte) error {
	data := map[string]any{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}
	m.SetAll(data)
	return nil
}

// SetAll sets the key value fields on the metadata
func (m *Metadata) SetAll(data map[string]any) {
	for k, v := range data {
		m.tags.Store(k, v)
	}
}

// Set sets a key value pair on the metadata
func (m *Metadata) Set(key string, value any) {
	m.SetAll(map[string]any{
		key: value,
	})
}

// Del deletes a key from the metadata
func (m *Metadata) Del(key string) {
	m.tags.Delete(key)
}

// Get gets a key from the metadata if it exists
func (m *Metadata) Get(key string) (any, bool) {
	return m.tags.Load(key)
}

// Exists returns true if the key exists in the metadata
func (m *Metadata) Exists(key string) bool {
	_, ok := m.tags.Load(key)
	return ok
}

// Map returns the metadata keyvalues as a map
func (m *Metadata) Map() map[string]any {
	data := map[string]any{}
	m.tags.Range(func(key, value any) bool {
		data[key.(string)] = value
		return true
	})
	return data
}

// ToContext adds the metadata to the input go context
func (m *Metadata) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, metadataKey, m)
}

// GetMetadata gets metadata from the context if it exists
func GetMetadata(ctx context.Context) (*Metadata, bool) {
	m, ok := ctx.Value(metadataKey).(*Metadata)
	if ok {
		return m, true
	}
	return &Metadata{}, false
}
