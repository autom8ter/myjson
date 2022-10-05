package wolverine

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cast"
	"github.com/xeipuuv/gojsonschema"
	"golang.org/x/sync/errgroup"
)

// Migration is an atomic database migration
type Migration struct {
	Name     string
	Function func(ctx context.Context, db DB) error
}

func (d *db) Config() Config {
	return d.config
}

func (d *db) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cron.Stop()
	err := d.machine.Wait()
	for _, i := range d.fullText {
		err = multierror.Append(err, i.Close())
	}
	err = multierror.Append(err, d.kv.Sync())
	err = multierror.Append(err, d.kv.Close())
	if err, ok := err.(*multierror.Error); ok && len(err.Errors) > 0 {
		d.Logger.Error(ctx, "error closing database", err, map[string]interface{}{})
		return d.wrapErr(err, "")
	}
	return d.wrapErr(err, "")
}

const (
	lastBackupID     = "last_backup"
	lastMigrationID  = "last_migration"
	systemCollection = "system"
)

// dropIndexes drops all the indexes
func (d *db) dropIndexes(ctx context.Context) error {
	return d.kv.DropPrefix([]byte("index."))
}

// ReIndex locks and then reindexes the database
func (d *db) ReIndex(ctx context.Context) error {
	//if err := d.dropIndexes(ctx); err != nil {
	//	return err
	//}
	egp, ctx := errgroup.WithContext(ctx)
	for _, c := range d.config.Collections {
		c := c
		egp.Go(func() error {
			var startAt string
			for {
				results, err := d.Query(ctx, c.Name, Query{
					Select:  nil,
					Where:   nil,
					StartAt: startAt,
					Limit:   1000,
					OrderBy: OrderBy{},
				})
				if err != nil {
					return d.wrapErr(err, "")
				}
				if len(results) == 0 {
					break
				}
				for _, r := range results {
					result, _ := d.Get(ctx, c.Name, r.GetID())
					if result != nil {
						if err := d.Set(ctx, c.Name, result); err != nil {
							return err
						}
					} else {
						d.Delete(ctx, c.Name, r.GetID())
					}
				}
				startAt = results[len(results)-1].GetID()
			}
			return nil
		})
	}
	return egp.Wait()
}

// ReIndexCollection reindexes a specific collection
func (d *db) ReIndexCollection(ctx context.Context, collection string) error {
	//if err := d.dropIndexes(ctx); err != nil {
	//	return err
	//}
	var startAt string
	for {
		results, err := d.Query(ctx, collection, Query{
			Select:  nil,
			Where:   nil,
			StartAt: startAt,
			Limit:   1000,
			OrderBy: OrderBy{},
		})
		if err != nil {
			return d.wrapErr(err, "")
		}
		if len(results) == 0 {
			break
		}
		for _, r := range results {
			result, _ := d.Get(ctx, collection, r.GetID())
			if result != nil {
				if err := d.Set(ctx, collection, result); err != nil {
					return err
				}
			} else {
				d.Delete(ctx, collection, r.GetID())
			}
		}
		startAt = results[len(results)-1].GetID()
	}
	return nil
}

func (d *db) Backup(ctx context.Context, w io.Writer) error {
	_, err := d.kv.Backup(w, 0)
	if err != nil {
		return d.wrapErr(err, "")
	}
	return nil
}

func (d *db) IncrementalBackup(ctx context.Context, w io.Writer) error {
	record, _ := d.Get(ctx, systemCollection, lastBackupID)
	var (
		err  error
		next uint64
	)
	if record.Empty() {
		next, err = d.kv.Backup(w, uint64(0))
		if err != nil {
			return d.wrapErr(err, "")
		}
	} else {
		next, err = d.kv.Backup(w, cast.ToUint64(record.Get("version")))
		if err != nil {
			return d.wrapErr(err, "")
		}
	}
	r := NewDocument()
	r.SetID("last_backup")
	r.SetCollection(systemCollection)
	r.Set("version", int(next))
	return d.Set(ctx, systemCollection, r)
}

func (d *db) Restore(ctx context.Context, r io.Reader) error {
	if err := d.kv.Load(r, 256); err != nil {
		return d.wrapErr(err, "")
	}
	return d.wrapErr(d.ReIndex(ctx), "")
}

func (d *db) Migrate(ctx context.Context, migrations []Migration) error {
	existing, _ := d.Get(ctx, systemCollection, lastMigrationID)
	if existing.Empty() {
		existing = NewDocument()
	}
	existing.SetCollection(systemCollection)
	existing.SetID(lastMigrationID)
	version := cast.ToInt(existing.Get("version"))
	d.Debug(ctx, fmt.Sprintf("migration: last version=%v", version), map[string]interface{}{
		"document": existing.String(),
	})
	for i, migration := range migrations[version:] {
		d.Info(ctx, fmt.Sprintf("migration: starting %s", migration.Name), map[string]interface{}{
			"document": existing.String(),
		})
		now := time.Now()
		if err := migration.Function(ctx, d); err != nil {
			if derr := d.Set(ctx, systemCollection, existing); derr != nil {
				return derr
			}
			return d.wrapErr(err, "")
		}
		existing.Set("version", i+1)
		existing.Set("name", migration.Name)
		existing.Set("processing_time", time.Since(now).String())
		d.Debug(ctx, fmt.Sprintf("migration: %s executed successfully", migration.Name), map[string]interface{}{
			"document": existing.String(),
		})
	}
	if err := d.Set(ctx, systemCollection, existing); err != nil {
		return d.wrapErr(err, "")
	}
	return nil
}

func (d *db) GetCollection(ctx context.Context, collection string) (*Collection, error) {
	id := fmt.Sprintf("collections.%s", collection)
	existing, err := d.Get(ctx, systemCollection, id)
	if err != nil {
		return nil, err
	}
	var c = &Collection{}
	if err := existing.ScanJSON(c); err != nil {
		return nil, d.wrapErr(err, "")
	}
	if c.JSONSchema != "" {
		schema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.JSONSchema))
		if err != nil {
			return nil, stacktrace.Propagate(err, "")
		}
		c.loadedSchema = schema
	}
	return c, nil
}

func (d *db) GetCollections(ctx context.Context) ([]*Collection, error) {
	var collections []*Collection
	results, err := d.Query(ctx, systemCollection, Query{})
	if err != nil {
		return nil, d.wrapErr(err, "")
	}
	for _, result := range results {
		if strings.HasPrefix(result.GetID(), "collection.") {
			existing, err := d.Get(ctx, systemCollection, result.GetID())
			if err != nil {
				return nil, err
			}
			var c = &Collection{}
			if err := existing.ScanJSON(c); err != nil {
				return nil, d.wrapErr(err, "")
			}
			if c.JSONSchema != "" {
				schema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(c.JSONSchema))
				if err != nil {
					return nil, stacktrace.Propagate(err, "")
				}
				c.loadedSchema = schema
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
	id := fmt.Sprintf("collections.%s", collection.Name)
	existing, _ := d.Get(ctx, systemCollection, id)
	if existing.Empty() {
		existing = NewDocument()
		existing.SetCollection(systemCollection)
		existing.SetID(id)
	}
	newDoc, err := NewDocumentFromAny(collection)
	if err != nil {
		return d.wrapErr(err, "")
	}

	existing.Merge(newDoc)

	if err := d.Set(ctx, systemCollection, existing); err != nil {
		return d.wrapErr(err, "")
	}
	return d.ReIndexCollection(ctx, collection.Name)
}
