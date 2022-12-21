package gokvkit

import (
	"sync"
)

// Cache is a caching interface for in-memory state
type Cache[T any] interface {
	// Get gets a value, it returns nil if no value was found
	Get(key string) T
	// Exists returns true if the key has a value
	Exists(key string) bool
	// Set sets the key value pair
	Set(key string, value T)
	// SetFunc sets the key value pair within a callback function
	SetFunc(key string, fn func(T) T)
	// Del deletes a key if it exists
	Del(key string)
	// Range
	Range(fn func(key string, t T) bool)
	// AsMap returns the cache kv pairs as a map
	AsMap() map[string]T
}

func newInMemCache[T any](data map[string]T) Cache[T] {
	if data == nil {
		data = map[string]T{}
	}
	return &inMemMap[T]{
		data: data,
	}
}

// inMemMap is backed by a concurrency & type safe map
type inMemMap[T any] struct {
	mu   sync.RWMutex
	data map[string]T
}

func (m *inMemMap[T]) Get(key string) T {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data[key]
}

func (m *inMemMap[T]) Exists(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[key]
	return ok
}

func (m *inMemMap[T]) Set(key string, value T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = map[string]T{}
	}
	m.data[key] = value
}

func (m *inMemMap[T]) SetFunc(key string, fn func(T) T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = map[string]T{}
	}
	m.data[key] = fn(m.data[key])
}

func (m *inMemMap[T]) Del(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

func (m *inMemMap[T]) Range(fn func(key string, t T) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for key, m := range m.data {
		if !fn(key, m) {
			break
		}
	}
}

func (m *inMemMap[T]) AsMap() map[string]T {
	data := map[string]T{}
	m.Range(func(key string, entry T) bool {
		data[key] = entry
		return true
	})
	return data
}
