package wolverine

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

func (d *db) Config() Config {
	return d.config
}

func (d *db) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cron.Stop()
	var err error
	for _, i := range d.fullText {
		err = multierror.Append(i.Close())
	}
	err = multierror.Append(d.kv.Sync())
	err = multierror.Append(d.kv.Close())
	return err
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
	if err := d.dropIndexes(ctx); err != nil {
		return err
	}
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
					return err
				}
				if len(results) == 0 {
					break
				}
				for _, r := range results {
					if err := d.Set(ctx, r); err != nil {
						return err
					}
				}
				startAt = results[len(results)-1].GetID()
			}
			return nil
		})
	}
	return egp.Wait()
}

func (d *db) Backup(ctx context.Context, w io.Writer) error {
	_, err := d.kv.Backup(w, 0)
	if err != nil {
		return err
	}
	return nil
}

func (d *db) IncrementalBackup(ctx context.Context, w io.Writer) error {
	record, _ := d.Get(ctx, systemCollection, lastBackupID)
	var (
		err  error
		next uint64
	)
	if record == nil {
		next, err = d.kv.Backup(w, uint64(0))
		if err != nil {
			return err
		}
	} else {
		next, err = d.kv.Backup(w, cast.ToUint64(record["version"]))
		if err != nil {
			return err
		}
	}
	r := Record{}
	r.SetID("last_backup")
	r.SetCollection(systemCollection)
	r.Set("version", int(next))
	return d.Set(ctx, r)
}

func (d *db) Restore(ctx context.Context, r io.Reader) error {
	if err := d.kv.Load(r, 256); err != nil {
		d.mu.Unlock()
		return err
	}
	return d.ReIndex(ctx)
}

func (d *db) Migrate(ctx context.Context, migrations []Migration) error {
	existing, _ := d.Get(ctx, systemCollection, lastMigrationID)
	defer func() {

	}()
	if existing == nil {
		existing = map[string]interface{}{}
	}
	existing.SetCollection(systemCollection)
	existing.SetID(lastMigrationID)
	version := cast.ToInt(existing["version"])
	d.Debug(ctx, fmt.Sprintf("migration: last version=%v", version), existing)
	for i, migration := range migrations[version:] {
		d.Info(ctx, fmt.Sprintf("migration: starting %s", migration.Name), existing)
		now := time.Now()
		if err := migration.Function(ctx, d); err != nil {
			if derr := d.Set(ctx, existing); derr != nil {
				return derr
			}
			return err
		}
		existing.Set("version", i+1)
		existing.Set("name", migration.Name)
		existing.Set("processing_time", time.Since(now).String())
		d.Debug(ctx, fmt.Sprintf("migration: %s executed successfully", migration.Name), existing)
	}
	if err := d.Set(ctx, existing); err != nil {
		return err
	}
	return nil
}
