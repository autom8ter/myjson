package safe

import (
	"sync"
)

// Map is a concurrency & type safe map
type Map[T any] struct {
	mu   sync.RWMutex
	data map[string]T
}

func NewMap[T any](data map[string]T) *Map[T] {
	return &Map[T]{
		data: data,
	}
}

func (m *Map[T]) Get(key string) T {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data[key]
}

func (m *Map[T]) Exists(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[key]
	return ok
}

func (m *Map[T]) Set(key string, value T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = map[string]T{}
	}
	m.data[key] = value
}

func (m *Map[T]) SetFunc(key string, fn func(T) T) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = map[string]T{}
	}
	m.data[key] = fn(m.data[key])
}

func (m *Map[T]) Del(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

func (m *Map[T]) Range(fn func(key string, t T) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for key, m := range m.data {
		if !fn(key, m) {
			break
		}
	}
}

func (m *Map[T]) AsMap() map[string]T {
	data := map[string]T{}
	m.Range(func(key string, entry T) bool {
		data[key] = entry
		return true
	})
	return data
}
