package wolverine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/robfig/cron/v3"
)

type db struct {
	Logger
	config      Config
	kv          *badger.DB
	fullText    map[string]bleve.Index
	mu          sync.RWMutex
	collections map[string]Collection
	cron        *cron.Cron
}

func New(ctx context.Context, config Config) (DB, error) {
	opts := badger.DefaultOptions(config.Path)
	if config.Path == "inmem" {
		opts.InMemory = true
		opts.Dir = ""
		opts.ValueDir = ""
	}
	opts = opts.WithLoggingLevel(badger.ERROR)
	kv, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	collections := map[string]Collection{
		"system": {
			Name: "system",
		},
	}
	fulltext := map[string]bleve.Index{}
	for _, collection := range config.Collections {
		collections[collection.Name] = collection
		for _, i := range collection.Indexes {
			if i.FullText {
				indexMapping := bleve.NewIndexMapping()
				if len(i.Fields) > 0 && i.Fields[0] != "*" {
					//document := bleve.NewDocumentMapping()
					//for _, f := range i.Fields {
					//	disabled := bleve.NewDocumentDisabledMapping()
					//	document.AddSubDocumentMapping(f, disabled)
					//}
					//indexMapping.AddDocumentMapping("index", document)
				}

				if config.Path == "inmem" {
					i, err := bleve.NewMemOnly(indexMapping)
					if err != nil {
						return nil, err
					}
					fulltext[collection.Name] = i
				} else {
					path := fmt.Sprintf("%s/search/%s.bleve", config.Path, collection.Name)
					i, err := bleve.Open(path)
					if err == nil {
						fulltext[collection.Name] = i
					} else {
						i, err = bleve.New(path, indexMapping)
						if err != nil {
							return nil, err
						}
						fulltext[collection.Name] = i
					}
				}
			}
		}
	}
	d := &db{
		config:      config,
		kv:          kv,
		fullText:    fulltext,
		mu:          sync.RWMutex{},
		collections: collections,
		cron:        cron.New(),
	}
	for _, c := range config.CronJobs {
		_, err := d.cron.AddFunc(c.Schedule, func() {
			c.Function(ctx, d)
		})
		if err != nil {
			return nil, err
		}
	}
	d.Logger = config.Logger
	if d.Logger == nil {
		level := "info"
		if config.Debug {
			level = "debug"
		}
		lgger, err := NewLogger(level, map[string]any{})
		if err != nil {
			return nil, err
		}
		d.Logger = lgger
	}
	if config.ReIndex {
		if err := d.ReIndex(ctx); err != nil {
			return nil, fmt.Errorf("failed to reindex: %s", err)
		}
	}
	if err := d.Migrate(ctx, config.Migrations); err != nil {
		return nil, err
	}
	go d.cleanupExpiredSearch(ctx)
	d.cron.Start()
	return d, nil
}

func (d *db) cleanupExpiredSearch(ctx context.Context) error {
	return d.kv.Subscribe(ctx, func(kv *badger.KVList) error {
		for _, item := range kv.Kv {
			var record Record
			if err := json.Unmarshal(item.Value, &record); err != nil {
				return err
			}
			if _, ok := d.fullText[record.GetCollection()]; ok {
				if item.ExpiresAt <= uint64(time.Now().Unix()) {
					if err := d.fullText[record.GetCollection()].Delete(record.GetID()); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}, []pb.Match{
		{
			Prefix: []byte(""),
		},
	})
}

func (d *db) getQueryPrefix(collection string, where []Where) []byte {
	var whereVals = map[string]any{}
	for _, w := range where {
		if w.Op != "==" {
			continue
		}
		whereVals[w.Field] = w.Value
	}
	prefix := []byte(fmt.Sprintf("%s.", collection))
	for _, i := range d.collections[collection].Indexes {
		var vals []any
		for _, f := range i.Fields {
			if val, ok := whereVals[f]; ok {
				vals = append(vals, val)
			}
		}
		if len(vals) == len(i.Fields) {
			prefix = fieldIndexPrefix(collection, vals)
		}
	}
	return prefix
}
