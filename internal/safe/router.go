package safe

import (
	"net/http"
	"sync"
)

// Router is a concurrency & type safe http router
type Router struct {
	mu   sync.RWMutex
	data map[string]map[string]map[string]http.Handler
}

func NewRouter() *Router {
	return &Router{
		data: map[string]map[string]map[string]http.Handler{},
	}
}

func (m *Router) Get(collection, operation, method string) http.Handler {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.data[collection] == nil {
		return nil
	}
	if m.data[collection][operation] == nil {
		return nil
	}
	return m.data[collection][operation][method]
}

func (m *Router) Exists(collection, operation, method string) bool {
	return m.Get(collection, operation, method) != nil
}

func (m *Router) Set(collection, operation, method string, handler http.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data[collection] == nil {
		m.data[collection] = map[string]map[string]http.Handler{}
	}
	if m.data[collection][operation] == nil {
		m.data[collection][operation] = map[string]http.Handler{}
	}
	m.data[collection][operation][method] = handler
}

func (m *Router) SetFunc(collection, operation string, fn func(map[string]http.Handler)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data[collection] == nil {
		m.data[collection] = map[string]map[string]http.Handler{}
	}
	if m.data[collection][operation] == nil {
		m.data[collection][operation] = map[string]http.Handler{}
	}
	fn(m.data[collection][operation])
}

func (m *Router) Del(collection, operation, method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data[collection] == nil {
		return
	}
	if m.data[collection][operation] == nil {
		return
	}
	delete(m.data[collection][operation], method)
}
