package wolverine

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/blevesearch/bleve"
	"github.com/hashicorp/go-multierror"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

// Migration is an atomic database migration
type Migration struct {
	Name     string
	Function func(ctx context.Context, db DB) error
}

func (d *db) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	err := d.machine.Wait()
	d.fullText.Range(func(key, value any) bool {
		if indexes, ok := value.([]bleve.Index); ok {
			for _, idx := range indexes {
				err = multierror.Append(err, idx.Close())
			}
		}
		d.fullText.Delete(key)
		return true
	})

	err = multierror.Append(err, d.kv.Sync())
	err = multierror.Append(err, d.kv.Close())
	if err, ok := err.(*multierror.Error); ok && len(err.Errors) > 0 {
		return stacktrace.Propagate(err, "database close failure")
	}
	return nil
}

const (
	lastBackupID     = "last_backup"
	lastMigrationID  = "last_migration"
	systemCollection = "system"
)

// ReIndex locks and then reindexes the database
func (d *db) ReIndex(ctx context.Context) error {
	if err := d.loadCollections(ctx); err != nil {
		return stacktrace.Propagate(err, "failed to reindex collections")
	}
	egp, ctx := errgroup.WithContext(ctx)
	for _, c := range d.getInmemCollections() {
		c := c
		egp.Go(func() error {
			return d.ReIndexCollection(ctx, c.Collection())
		})
	}
	if err := egp.Wait(); err != nil {
		return stacktrace.Propagate(err, "failed to reindex collections")
	}
	return stacktrace.Propagate(d.loadCollections(ctx), "failed to reindex collections")
}

// ReIndexCollection reindexes a specific collection
func (d *db) ReIndexCollection(ctx context.Context, collection string) error {
	c, ok := d.getInmemCollection(collection)
	if !ok {
		return stacktrace.NewError("unsupported collection: %s", collection)
	}
	if err := d.loadFullText(c, true); err != nil {
		return stacktrace.Propagate(err, "failed to reindex collection: %s", collection)
	}
	egp, _ := errgroup.WithContext(ctx)
	var page int
	for {

		results, err := d.Query(ctx, c.Collection(), Query{
			Select:  nil,
			Where:   nil,
			Page:    page,
			Limit:   1000,
			OrderBy: OrderBy{},
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to reindex collection: %s", collection)
		}
		if len(results.Documents) == 0 {
			break
		}
		var toSet []*Document
		var toDelete []string
		for _, r := range results.Documents {
			result, _ := d.Get(ctx, c.Collection(), r.GetID())
			if result != nil {
				toSet = append(toSet, result)
			} else {
				toDelete = append(toDelete, r.GetID())
				_ = d.Delete(ctx, c.Collection(), r.GetID())
			}
		}
		if len(toSet) > 0 {
			egp.Go(func() error {
				return stacktrace.Propagate(d.BatchSet(ctx, c.Collection(), toSet), "")
			})
		}
		if len(toDelete) > 0 {
			egp.Go(func() error {
				return stacktrace.Propagate(d.BatchDelete(ctx, c.Collection(), toDelete), "")
			})
		}
		page = results.NextPage
	}
	if err := egp.Wait(); err != nil {
		return stacktrace.Propagate(err, "failed to reindex collection: %s", collection)
	}
	return stacktrace.Propagate(d.loadCollections(ctx), "failed to reindex collection: %s", collection)
}

func (d *db) Backup(ctx context.Context, w io.Writer) error {
	_, err := d.kv.Backup(w, 0)
	if err != nil {
		return stacktrace.Propagate(err, "failed backup")
	}
	return nil
}

func (d *db) IncrementalBackup(ctx context.Context, w io.Writer) error {
	record, _ := d.Get(ctx, systemCollection, lastBackupID)
	var (
		err  error
		next uint64
	)
	if record == nil || record.Empty() {
		next, err = d.kv.Backup(w, uint64(0))
		if err != nil {
			return stacktrace.Propagate(err, "failed incremental backup")
		}
		record = NewDocument()
		record.SetID(lastBackupID)
	} else {
		next, err = d.kv.Backup(w, cast.ToUint64(record.Get("properties.version")))
		if err != nil {
			return stacktrace.Propagate(err, "failed incremental backup")
		}
	}
	record.Set("properties.version", int(next))
	return stacktrace.Propagate(d.Set(ctx, systemCollection, record), "failed incremental backup")
}

func (d *db) Restore(ctx context.Context, r io.Reader) error {
	if err := d.kv.Load(r, 256); err != nil {
		return stacktrace.Propagate(err, "")
	}
	return stacktrace.Propagate(d.ReIndex(ctx), "")
}

func (d *db) Migrate(ctx context.Context, migrations []Migration) error {
	existing, _ := d.Get(ctx, systemCollection, lastMigrationID)
	if existing == nil || existing.Empty() {
		existing = NewDocument()
		existing.SetID(lastMigrationID)
	}

	version := cast.ToInt(existing.Get("properties.version"))
	for i, migration := range migrations[version:] {
		now := time.Now()
		if err := migration.Function(ctx, d); err != nil {
			if derr := d.Set(ctx, systemCollection, existing); derr != nil {
				return derr
			}
			return stacktrace.Propagate(err, "failed to execute migration")
		}
		existing.Set("properties.version", i+1)
		existing.Set("properties.name", migration.Name)
		existing.Set("properties.processing_time", time.Since(now).String())
	}
	if err := d.Set(ctx, systemCollection, existing); err != nil {
		return stacktrace.Propagate(err, "failed to execute migration")
	}
	return nil
}

func (d *db) GetCollection(ctx context.Context, collection string) (*Collection, error) {
	id := fmt.Sprintf("collections.%s", collection)
	existing, err := d.Get(ctx, systemCollection, id)
	if err != nil {
		return nil, stacktrace.Propagate(err, "failed to get collection: %s", collection)
	}
	c, err := LoadCollection(cast.ToString(existing.Get("properties.schema")))
	if err != nil {
		return nil, stacktrace.Propagate(err, "failed to get collection: %s", collection)
	}
	return c, nil
}

func (d *db) GetCollections(ctx context.Context) ([]*Collection, error) {
	var collections []*Collection
	results, err := d.Query(ctx, systemCollection, Query{Limit: 1000})
	if err != nil {
		return nil, stacktrace.Propagate(err, "failed to get collections")
	}
	for _, result := range results.Documents {
		if strings.HasPrefix(result.GetID(), "collections.") {
			existing, err := d.Get(ctx, systemCollection, result.GetID())
			if err != nil {
				return nil, stacktrace.Propagate(err, "failed to get collections")
			}
			c, err := LoadCollection(cast.ToString(existing.Get("properties.schema")))
			if err != nil {
				return nil, stacktrace.Propagate(err, "failed to get collections")
			}
			collections = append(collections, c)
		}
	}
	return collections, nil
}

func (d *db) SetCollection(ctx context.Context, collection *Collection) error {
	if collection == nil {
		return nil
	}
	id := fmt.Sprintf("collections.%s", collection.Collection())
	existing, _ := d.Get(ctx, systemCollection, id)
	if existing == nil || existing.Empty() {
		existing = NewDocument()
		existing.SetID(id)
	}
	existing.Set("properties", map[string]interface{}{
		"schema": collection.Schema,
	})

	if err := d.Set(ctx, systemCollection, existing); err != nil {
		return stacktrace.Propagate(err, "")
	}
	d.collections.Store(collection.Collection(), collection)
	if err := d.loadCollections(ctx); err != nil {
		return stacktrace.Propagate(err, "failed to set collection")
	}
	return stacktrace.Propagate(d.ReIndexCollection(ctx, collection.Collection()), "failed to set collection")
}

func (d *db) SetCollections(ctx context.Context, collections []*Collection) error {
	m := machine.New()
	for _, c := range collections {
		c := c
		m.Go(ctx, func(ctx context.Context) error {
			return d.SetCollection(ctx, c)
		})
	}
	return stacktrace.Propagate(m.Wait(), "failed to set collections")
}

var systemCollectionSchema = `
{
  "$id": "https://wolverine.io/system.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "System",
  "collection": "system",
  "type": "object",
  "required": [
    "_id",
    "properties"
  ],
  "properties": {
    "_id": {
      "type": "string",
      "description": "The document's id."
    },
    "properties": {
      "type": "object",
      "description": "system properties"
    }
  }
}
`
