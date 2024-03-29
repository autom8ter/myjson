package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"

	// import embed package
	_ "embed"

	// import badger kv provider
	_ "github.com/autom8ter/myjson/kv/badger"
)

var (
	//go:embed testdata/scripts.js
	GlobalScript string
	//go:embed testdata/task.yaml
	TaskSchema string
	//go:embed testdata/user.yaml
	UserSchema string
	//go:embed testdata/account.yaml
	AccountSchema  string
	AllCollections = []string{AccountSchema, UserSchema, TaskSchema}
)

func NewUserDoc() *myjson.Document {
	doc, err := myjson.NewDocumentFrom(map[string]interface{}{
		"_id":  gofakeit.UUID(),
		"name": gofakeit.Name(),
		"contact": map[string]interface{}{
			"email": fmt.Sprintf("%v.%s", gofakeit.IntRange(0, 100), gofakeit.Email()),
		},
		"account_id":      fmt.Sprint(gofakeit.IntRange(0, 100)),
		"language":        gofakeit.Language(),
		"birthday_month":  gofakeit.Month(),
		"favorite_number": gofakeit.Second(),
		"gender":          gofakeit.Gender(),
		"age":             gofakeit.IntRange(0, 100),
		"timestamp":       gofakeit.DateRange(time.Now().Truncate(7200*time.Hour), time.Now()),
		"annotations":     gofakeit.Map(),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

func NewTaskDoc(usrID string) *myjson.Document {
	doc, err := myjson.NewDocumentFrom(map[string]interface{}{
		"_id":     gofakeit.UUID(),
		"user":    usrID,
		"content": gofakeit.LoremIpsumSentence(5),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

type TestFunc func(ctx context.Context, t *testing.T, db myjson.Database)

type TestConfig struct {
	Opts        []myjson.DBOpt
	Persist     bool
	Collections []string
	Values      string
	Roles       []string
	Timeout     time.Duration
}

func Test(t *testing.T, cfg TestConfig, fn TestFunc) func(*testing.T) {
	ctx := context.Background()
	var (
		db  myjson.Database
		err error
	)
	var closers []func()
	if cfg.Persist {
		_ = os.MkdirAll("tmp", 0700)
		dir, err := os.MkdirTemp("./tmp", "")
		assert.NoError(t, err)
		closers = append(closers, func() {
			os.RemoveAll(dir)
		})
		db, err = myjson.Open(ctx, "badger", map[string]any{
			"storage_path": dir,
		}, cfg.Opts...)
	} else {
		db, err = myjson.Open(ctx, "badger", map[string]any{}, cfg.Opts...)
	}
	assert.NoError(t, err)
	closers = append(closers, func() {
		db.Close(ctx)
	})
	assert.NoError(t, err)
	if len(cfg.Collections) > 0 {
		assert.NoError(t, db.Configure(ctx, cfg.Values, cfg.Collections))
	}
	return func(t *testing.T) {
		if cfg.Timeout == 0 {
			cfg.Timeout = 5 * time.Minute
		}
		ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
		if len(cfg.Roles) > 0 {
			ctx = myjson.SetMetadataRoles(ctx, cfg.Roles)
		}
		fn(ctx, t, db)
		for _, closer := range closers {
			closer()
		}
	}
}

func TestDB(fn func(ctx context.Context, db myjson.Database), opts ...myjson.DBOpt) error {
	opts = append(opts, myjson.WithGlobalJavascriptFunctions([]string{GlobalScript}))
	_ = os.MkdirAll("tmp", 0700)
	dir, err := os.MkdirTemp("./tmp", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = myjson.SetMetadataRoles(ctx, []string{"super_user"})
	db, err := myjson.Open(ctx, "badger", map[string]any{
		"storage_path": dir,
	}, opts...)
	if err != nil {
		return err
	}
	if err := db.Configure(ctx, "", AllCollections); err != nil {
		return err
	}
	if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
		for i := 0; i <= 100; i++ {
			d, _ := myjson.NewDocumentFrom(map[string]any{
				"_id":  fmt.Sprint(i),
				"name": gofakeit.Company(),
			})
			if err := tx.Set(ctx, "account", d); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	defer db.Close(ctx)
	fn(ctx, db)
	return nil
}

func Seed(ctx context.Context, db myjson.Database, accounts int, usersPerAccount int, tasksPerUser int) error {
	if err := db.Tx(ctx, kv.TxOpts{IsBatch: true}, func(ctx context.Context, tx myjson.Tx) error {
		for i := 0; i <= accounts; i++ {
			d, _ := myjson.NewDocumentFrom(map[string]any{
				"_id":  fmt.Sprint(i),
				"name": gofakeit.Company(),
			})
			if err := tx.Set(ctx, "account", d); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := SeedUsers(ctx, db, usersPerAccount, tasksPerUser); err != nil {
		return err
	}
	return nil
}

func SeedUsers(ctx context.Context, db myjson.Database, perAccount int, tasksPerUser int) error {
	results, err := db.Query(ctx, "account", myjson.Q().Query())
	if err != nil {
		return err
	}
	if err := db.Tx(ctx, kv.TxOpts{IsBatch: true}, func(ctx context.Context, tx myjson.Tx) error {
		for _, a := range results.Documents {
			for i := 0; i < perAccount; i++ {
				u := NewUserDoc()
				if err := u.Set("account_id", a.Get("_id")); err != nil {
					return err
				}
				if err := tx.Set(ctx, "user", u); err != nil {
					return err
				}

				for i := 0; i < tasksPerUser; i++ {
					t := NewTaskDoc(u.GetString("_id"))
					_, err := tx.Create(ctx, "task", t)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
