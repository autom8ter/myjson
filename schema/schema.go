package schema

import "sync"

type Schema struct {
	mu          sync.RWMutex
	collections map[string]*Collection
}

func NewSchema(collections []*Collection) *Schema {
	s := &Schema{
		mu:          sync.RWMutex{},
		collections: map[string]*Collection{},
	}
	for _, c := range collections {
		s.Set(c)
	}
	return s
}

func (s *Schema) Get(collection string) *Collection {
	return s.collections[collection]
}

func (s *Schema) Set(c *Collection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collections[c.collection] = c
}

func (s *Schema) Del(collection string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.collections, collection)
}

func (s *Schema) CollectionNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for k, _ := range s.collections {
		names = append(names, k)
	}
	return names
}
