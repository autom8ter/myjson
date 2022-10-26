package schema

import (
	"encoding/json"
	"github.com/autom8ter/wolverine/errors"
	"github.com/autom8ter/wolverine/internal/prefix"
	"github.com/palantir/stacktrace"
	"github.com/segmentio/ksuid"
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

// Collection returns the name of the collection based on the schema's 'collection' field on the collection's schema
func (c *Collection) Collection() string {
	return c.Config().Collection
}

// Indexing returns the list of the indexes based on the schema's 'indexing' field on the collection's schema
func (c *Collection) Indexing() Indexing {
	return c.Config().Indexing
}

// PKey returns the collections primary key
func (c *Collection) PKey() string {
	return c.Config().PrimaryKey
}

// HasRelationships returns whether the collection has any foreign keys
func (c *Collection) HasRelationships() bool {
	return len(c.Config().ForeignKeys) > 0
}

// FKeys returns the collections foreign keys
func (c *Collection) FKeys() map[string]ForeignKey {
	return c.Config().ForeignKeys
}

// OptimizeQueryIndex selects the optimal index to use given the where/orderby clause
func (c *Collection) OptimizeQueryIndex(where []Where, order OrderBy) (QueryIndexMatch, error) {
	var whereFields []string
	var whereValues = map[string]any{}
	for _, w := range where {
		if w.Op != "==" && w.Op != Eq {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	index, err := c.getQueryIndex(whereFields, order.Field)
	if err != nil {
		return QueryIndexMatch{}, stacktrace.Propagate(err, "")
	}
	return index, nil
}

func (c *Collection) PrimaryQueryIndex() *prefix.PrefixIndexRef {
	return c.QueryIndexPrefix(QueryIndex{
		Fields: []string{c.Config().PrimaryKey},
	})
}

// GetPrimaryKeyRef gets a reference to the documents primary key
func (c *Collection) GetPrimaryKeyRef(documentID string) ([]byte, error) {
	if documentID == "" {
		return nil, stacktrace.NewErrorWithCode(errors.ErrTODO, "empty document id for property: %s", c.Config().PrimaryKey)
	}
	return c.PrimaryQueryIndex().GetPrefix(map[string]any{
		c.Config().PrimaryKey: documentID,
	}, documentID), nil
}

// SetID sets the documents primary key
func (c *Collection) SetID(d *Document) error {
	return stacktrace.Propagate(d.Set(c.Config().PrimaryKey, ksuid.New().String()), "")
}

func (c *Collection) QueryIndexPrefix(i QueryIndex) *prefix.PrefixIndexRef {
	return prefix.NewPrefixedIndex(c.Config().Collection, i.Fields)
}

// GetQueryIndex gets the
func (c *Collection) getQueryIndex(whereFields []string, orderBy string) (QueryIndexMatch, error) {
	var (
		target  *QueryIndex
		matched int
		ordered bool
	)
	indexing := c.Indexing()
	if !indexing.HasQueryIndex() {
		return QueryIndexMatch{
			Ref:     c.PrimaryQueryIndex(),
			Fields:  []string{c.Config().PrimaryKey},
			Ordered: orderBy == c.Config().PrimaryKey || orderBy == "",
		}, nil
	}
	for _, index := range indexing.Query {
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
		return QueryIndexMatch{
			Ref:     c.QueryIndexPrefix(*target),
			Fields:  target.Fields,
			Ordered: ordered,
		}, nil
	}
	return QueryIndexMatch{
		Ref:     c.PrimaryQueryIndex(),
		Fields:  []string{c.Config().PrimaryKey},
		Ordered: orderBy == c.Config().PrimaryKey || orderBy == "",
	}, nil
}

func (c *Collection) GetDocumentID(d *Document) string {
	return cast.ToString(d.Get(c.Config().PrimaryKey))
}
