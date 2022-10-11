package wolverine_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
)

func TestSystem(t *testing.T) {
	t.Run("set collection", func(t *testing.T) {
		db, err := wolverine.New(context.Background(), wolverine.Config{
			Path:    "inmem",
			Debug:   true,
			ReIndex: false,
		})
		assert.Nil(t, err)
		for _, c := range defaultCollections {
			assert.Nil(t, db.SetCollection(context.Background(), c))
		}
		for _, c := range defaultCollections {
			cv, err := db.GetCollection(context.Background(), c.Collection())
			assert.Nil(t, err)
			assert.Equal(t, c.Collection(), cv.Collection())
		}
		results, err := db.GetCollections(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, len(defaultCollections), len(results))
	})
	t.Run("set collections", func(t *testing.T) {
		db, err := wolverine.New(context.Background(), wolverine.Config{
			Path:    "inmem",
			Debug:   true,
			ReIndex: false,
		})
		assert.Nil(t, err)
		assert.Nil(t, db.SetCollections(context.Background(), defaultCollections))
		for _, c := range defaultCollections {
			cv, err := db.GetCollection(context.Background(), c.Collection())
			assert.Nil(t, err)
			assert.Equal(t, c.Collection(), cv.Collection())
		}
		results, err := db.GetCollections(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, len(defaultCollections), len(results))
	})
	t.Run("backup restore", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			buf := bytes.NewBuffer(nil)
			var usrs []*wolverine.Document
			for i := 0; i < 5; i++ {
				u := newUserDoc()
				usrs = append(usrs, u)
				assert.Nil(t, db.Set(ctx, "user", u))
			}
			assert.Nil(t, db.Backup(ctx, buf))
			restored, err := wolverine.New(ctx, wolverine.Config{
				Path:    "inmem",
				Debug:   true,
				ReIndex: false,
			})
			assert.Nil(t, err)
			assert.Nil(t, restored.Restore(ctx, buf))
			for _, u := range usrs {
				result, err := restored.Get(ctx, "user", u.GetID())
				assert.Nil(t, err)
				assert.NotNil(t, result)
			}
			assert.Nil(t, restored.ReIndex(ctx))
			for _, u := range usrs {
				result, err := restored.Get(ctx, "user", u.GetID())
				assert.Nil(t, err)
				assert.NotNil(t, result)
			}
		}))
	})
	t.Run("incremental backup restore", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			buf := bytes.NewBuffer(nil)
			var usrs []*wolverine.Document
			for i := 0; i < 5; i++ {
				u := newUserDoc()
				usrs = append(usrs, u)
				assert.Nil(t, db.Set(ctx, "user", u))
			}
			assert.Nil(t, db.IncrementalBackup(ctx, buf))
			for i := 0; i < 5; i++ {
				u := newUserDoc()
				usrs = append(usrs, u)
				assert.Nil(t, db.Set(ctx, "user", u))
			}
			assert.Nil(t, db.IncrementalBackup(ctx, buf))
			restored, err := wolverine.New(ctx, wolverine.Config{
				Path:    "inmem",
				Debug:   true,
				ReIndex: false,
			})
			assert.Nil(t, err)
			assert.Nil(t, restored.Restore(ctx, buf))
			for _, u := range usrs {
				result, err := restored.Get(ctx, "user", u.GetID())
				assert.Nil(t, err)
				assert.NotNil(t, result)
			}
			assert.Nil(t, restored.ReIndex(ctx))
			for _, u := range usrs {
				result, err := restored.Get(ctx, "user", u.GetID())
				assert.Nil(t, err)
				assert.NotNil(t, result)
			}
		}))
	})
	t.Run("migrate backup restore", func(t *testing.T) {
		assert.Nil(t, testDB(defaultCollections, func(ctx context.Context, db wolverine.DB) {
			buf := bytes.NewBuffer(nil)
			var usrs []*wolverine.Document

			err := db.Migrate(ctx, []wolverine.Migration{
				{
					Name: "batch set users",
					Function: func(ctx context.Context, db wolverine.DB) error {
						for i := 0; i < 5; i++ {
							u := newUserDoc()
							usrs = append(usrs, u)
						}
						return db.BatchSet(ctx, "user", usrs)
					},
				},
			})
			assert.Nil(t, err)
			err = db.Migrate(ctx, []wolverine.Migration{
				{
					Name: "batch set users",
					Function: func(ctx context.Context, db wolverine.DB) error {
						for i := 0; i < 5; i++ {
							u := newUserDoc()
							usrs = append(usrs, u)
						}
						return db.BatchSet(ctx, "user", usrs)
					},
				},
			})
			assert.Nil(t, db.Backup(ctx, buf))
			assert.Equal(t, 5, len(usrs))
			restored, err := wolverine.New(ctx, wolverine.Config{
				Path:    "inmem",
				Debug:   true,
				ReIndex: false,
			})
			assert.Nil(t, err)
			assert.Nil(t, restored.Restore(ctx, buf))
			for _, u := range usrs {
				result, err := restored.Get(ctx, "user", u.GetID())
				assert.Nil(t, err)
				assert.NotNil(t, result)
			}
			assert.Nil(t, restored.ReIndex(ctx))
			for _, u := range usrs {
				result, err := restored.Get(ctx, "user", u.GetID())
				assert.Nil(t, err)
				assert.NotNil(t, result)
			}
		}))
	})
}
