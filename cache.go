package gokvkit

import (
	"sync"
)

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
