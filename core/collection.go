package core

import (
	"encoding/json"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/prefix"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
)

// Collection is a collection of records of a given type. It is
type Collection struct {
	JSONSchema
}

// NewCollection creates a new Collection from the provided JSONSchema
func NewCollection(schema JSONSchema) *Collection {
	return &Collection{
		JSONSchema: schema,
	}
}

// NewCollectionFromBytes creates a new Collection from the provided JSONSchema bytes
func NewCollectionFromBytes(jsonSchema []byte) (*Collection, error) {
	scheme, err := NewJSONSchema(jsonSchema)
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return NewCollection(scheme), nil
}

// NewCollectionFromBytes creates a new Collection from the provided JSONSchema bytes - it panics on error
func NewCollectionFromBytesP(jsonSchema []byte) *Collection {
	scheme, err := NewJSONSchema(jsonSchema)
	if err != nil {
		panic(stacktrace.Propagate(err, "failed to parse json schema"))
	}
	return NewCollection(scheme)
}

// LoadCollectionsFromDir loads all yaml/json collections in the specified directory
func LoadCollectionsFromDir(collectionsDir string) ([]*Collection, error) {
	var collections []*Collection
	dir := os.DirFS(collectionsDir)
	if err := fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		if d.IsDir() {
			return nil
		}

		switch {
		case strings.HasSuffix(path, ".yaml"):
			files, err := dir.Open(path)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			f, err := ioutil.ReadAll(files)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			data := map[string]interface{}{}
			if err := yaml.Unmarshal(f, &data); err != nil {
				return stacktrace.Propagate(err, "")
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			schema, err := NewJSONSchema(jsonData)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			collections = append(collections, NewCollection(schema))
		case strings.HasSuffix(path, ".json"):
			files, err := dir.Open(path)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			f, err := ioutil.ReadAll(files)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			schema, err := NewJSONSchema(f)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			collections = append(collections, NewCollection(schema))
		}
		return nil
	}); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return collections, nil
}

// OptimizeIndex selects the optimal index to use given the where/orderby clause
func (c *Collection) OptimizeIndex(where []Where, order OrderBy) (IndexMatch, error) {
	var whereFields []string
	var whereValues = map[string]any{}
	for _, w := range where {
		if w.Op != "==" && w.Op != Eq {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	index, err := c.getIndex(whereFields, order.Field)
	if err != nil {
		return IndexMatch{}, stacktrace.Propagate(err, "")
	}
	return index, nil
}

// PrimaryIndex returns a reference to the primary index
func (c *Collection) PrimaryIndex() *prefix.PrefixIndexRef {
	return prefix.NewPrefixedIndex(c.Collection(), []string{c.PrimaryKey()})
}

// GetPrimaryKeyRef gets a reference to the documents primary key in the primary index
func (c *Collection) GetPrimaryKeyRef(documentID string) ([]byte, error) {
	if documentID == "" {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "empty document id for property: %s", c.PrimaryKey())
	}
	return c.PrimaryIndex().GetPrefix(map[string]any{
		c.PrimaryKey(): documentID,
	}, documentID), nil
}

// SetPrimaryKey sets the documents primary key
func (c *Collection) SetPrimaryKey(d *Document, id string) error {
	return stacktrace.Propagate(d.Set(c.PrimaryKey(), id), "failed to set primary key")
}

// GetIndex gets the
func (c *Collection) getIndex(whereFields []string, orderBy string) (IndexMatch, error) {
	var (
		target  *Index
		matched int
		ordered bool
	)
	indexing := c.Indexing()
	if !indexing.HasIndexes() {
		return IndexMatch{
			Ref:     c.PrimaryIndex(),
			Fields:  []string{c.PrimaryKey()},
			Ordered: orderBy == c.PrimaryKey() || orderBy == "",
		}, nil
	}
	for _, index := range indexing.Indexes {
		isOrdered := index.Fields[0] == orderBy
		var totalMatched int
		for i, f := range whereFields {
			if index.Fields[i] == f {
				totalMatched++
			}
		}
		if totalMatched > matched || (!ordered && isOrdered) {
			target = index
			ordered = isOrdered
		}
	}
	if target != nil && len(target.Fields) > 0 {
		return IndexMatch{
			Ref:     prefix.NewPrefixedIndex(c.Collection(), target.Fields),
			Fields:  target.Fields,
			Ordered: ordered,
		}, nil
	}
	return IndexMatch{
		Ref:     c.PrimaryIndex(),
		Fields:  []string{c.PrimaryKey()},
		Ordered: orderBy == c.PrimaryKey() || orderBy == "",
	}, nil
}

// GetPkey gets the documents primary key(if it exists)
func (c *Collection) GetPKey(d *Document) string {
	return cast.ToString(d.Get(c.PrimaryKey()))
}
