package coreimp

import (
	"fmt"
	"github.com/autom8ter/wolverine/core"
	"github.com/blevesearch/bleve"
	"github.com/palantir/stacktrace"
	"os"
)

func openFullTextIndex(storagePath string, schema *core.Collection, reindex bool) (bleve.Index, error) {
	if !schema.Indexing().SearchEnabled {
		return nil, nil
	}

	indexMapping := bleve.NewIndexMapping()

	path := fmt.Sprintf("%s/search/%s/index.db", storagePath, schema.Collection())
	if reindex {
		os.RemoveAll(path)
	}
	switch {
	case storagePath == "" && !reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index", schema.Collection())
		}
		return i, nil
	case storagePath == "" && reindex:
		i, err := bleve.NewMemOnly(indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index", schema.Collection())
		}
		return i, nil
	case reindex && storagePath != "":
		i, err := bleve.New(path, indexMapping)
		if err != nil {
			return nil, stacktrace.Propagate(err, "failed to create %s search index at path: %s", schema.Collection(), path)
		}
		return i, nil
	default:
		i, err := bleve.Open(path)
		if err == nil {
			return i, nil
		} else {
			i, err = bleve.New(path, indexMapping)
			if err != nil {
				return nil, stacktrace.Propagate(err, "failed to create %s search index at path: %s", schema.Collection(), path)
			}
			return i, nil
		}
	}
}