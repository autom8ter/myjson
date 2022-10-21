package wolverine

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"
	"github.com/palantir/stacktrace"
	"github.com/xeipuuv/gojsonschema"

	"github.com/autom8ter/wolverine/internal/prefix"
)

type db struct {
	config      Config
	kv          *badger.DB
	mu          sync.RWMutex
	collections sync.Map
	machine     machine.Machine
	fullText    sync.Map
	aggIndexes  []*aggIndex
}

func New(ctx context.Context, cfg Config) (DB, error) {
	config := &cfg
	opts := badger.DefaultOptions(config.Path)
	if config.Path == "inmem" {
		opts.InMemory = true
		opts.Dir = ""
		opts.ValueDir = ""
	}
	opts = opts.WithLoggingLevel(badger.ERROR)
	kv, err := badger.Open(opts)
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}

	d := &db{
		config:      *config,
		kv:          kv,
		mu:          sync.RWMutex{},
		collections: sync.Map{},
		machine:     machine.New(),
	}
	if err := d.loadCollections(ctx); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	for _, c := range d.getInmemCollections() {
		if err := d.loadFullText(c, false); err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
	}

	if config.ReIndex {
		if err := d.ReIndex(ctx); err != nil {

		}
	}
	if config.Migrate {
		if err := d.Migrate(ctx, config.Migrations); err != nil {
			return nil, stacktrace.Propagate(err, "migration failure")
		}
	}
	return d, nil
}

func (d *db) getFullText(collection string) bleve.Index {
	results, ok := d.fullText.Load(collection)
	if !ok {
		return nil
	}
	indexes, ok := results.([]bleve.Index)
	if !ok {
		return nil
	}
	if len(indexes) == 0 {
		return nil
	}
	return indexes[len(indexes)-1]
}

func (d *db) setFullText(collection string, index bleve.Index) {
	results, ok := d.fullText.Load(collection)
	if !ok {
		d.fullText.Store(collection, []bleve.Index{index})
		return
	}
	indexes, ok := results.([]bleve.Index)
	if !ok {
		d.fullText.Store(collection, []bleve.Index{index})
		return
	}
	indexes = append(indexes, index)
	d.fullText.Store(collection, indexes)
}

func (d *db) loadFullText(collection *Collection, reindex bool) error {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.TypeField = "_collection"
	newPath := fmt.Sprintf("%s/search/%s/index_%v.db", d.config.Path, collection.Collection(), time.Now().Unix())
	switch {
	case d.config.Path == "inmem" && !reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index", collection.Collection())
		}
		d.setFullText(collection.Collection(), i)
	case d.config.Path == "inmem" && reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index", collection.Collection())
		}
		d.setFullText(collection.Collection(), i)
	case reindex && d.config.Path != "inmem":
		i, err := bleve.New(newPath, indexMapping)
		if err != nil {
			return stacktrace.Propagate(err, "failed to create %s search index at path: %s", collection.Collection(), newPath)
		}
		d.setFullText(collection.Collection(), i)
	default:
		lastPath := d.getLastPath(collection)
		i, err := bleve.Open(lastPath)
		if err == nil {
			d.setFullText(collection.Collection(), i)
		} else {
			i, err = bleve.New(newPath, indexMapping)
			if err != nil {
				return stacktrace.Propagate(err, "failed to create %s search index at path: %s", collection.Collection(), newPath)
			}
			d.setFullText(collection.Collection(), i)
		}
	}
	return nil
}

func (d *db) getLastPath(collection *Collection) string {
	fileSystem := os.DirFS(fmt.Sprintf("%s/search/%s", d.config.Path, collection.Collection()))
	var paths []string
	if err := fs.WalkDir(fileSystem, ".", func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		paths = append(paths, path)
		return nil
	}); err != nil {
		panic(err)
	}
	if len(paths) == 0 {
		return ""
	}
	sort.Strings(paths)
	return paths[len(paths)-1]
}

func (d *db) loadCollections(ctx context.Context) error {
	collections, err := d.GetCollections(ctx)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	sysCollection, err := LoadCollection(systemCollectionSchema)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	collections = append(collections, sysCollection)
	for _, collection := range collections {
		if collection.Schema != "" {
			schema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(collection.Schema))
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			collection.loadedSchema = schema
		}
		d.collections.Store(collection.Collection(), collection)
	}
	return nil
}

func chooseIndex(collection *Collection, whereFields []string, orderBy string) (*prefix.PrefixIndexRef, []string, bool, error) {
	//sort.Strings(queryFields)
	//if orderBy != "" {
	//	for _, index := range collection.Indexes() {
	//		if index.Fields[0] == orderBy {
	//			return index.prefix(collection.Collection()), true, nil
	//		}
	//	}
	//	return nil, false, stacktrace.NewErrorWithCode(ErrIndexRequired, "an index is required on %s/%s when used in order by", collection.Collection(), orderBy)
	//}
	var (
		target  Index
		matched int
		ordered bool
	)
	for _, index := range collection.Indexes() {
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
	if len(target.Fields) > 0 {
		return target.prefix(collection.Collection()), target.Fields, ordered, nil
	}
	return Index{
		Fields: []string{"_id"},
	}.prefix(collection.Collection()), []string{"_id"}, orderBy == "_id", nil
}

func (d *db) getInmemCollection(collection string) (*Collection, bool) {
	c, ok := d.collections.Load(collection)
	if !ok {
		return nil, ok
	}
	return c.(*Collection), ok
}

func (d *db) getInmemCollections() []*Collection {
	var c []*Collection
	d.collections.Range(func(key, value any) bool {
		c = append(c, value.(*Collection))
		return true
	})
	return c
}

func (d *db) getQueryPrefix(collection string, where []Where, order OrderBy) ([]byte, []string, bool, error) {
	c, ok := d.getInmemCollection(collection)
	if !ok {
		return nil, nil, false, nil
	}
	var whereFields []string
	var whereValues = map[string]any{}
	for _, w := range where {
		if w.Op != "==" && w.Op != Eq {
			continue
		}
		whereFields = append(whereFields, w.Field)
		whereValues[w.Field] = w.Value
	}
	index, indexedFields, ordered, err := chooseIndex(c, whereFields, order.Field)
	if err != nil {
		return nil, indexedFields, ordered, stacktrace.Propagate(err, "")
	}
	return []byte(index.GetIndex("", whereValues)), indexedFields, ordered, nil
}

func (d *db) collectionNames() []string {
	var names []string
	collections := d.getInmemCollections()
	for _, c := range collections {
		names = append(names, c.Collection())
	}
	return names
}
