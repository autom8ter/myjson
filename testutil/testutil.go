package testutil

import (
	"context"
	"fmt"
	"github.com/autom8ter/gokvkit"
	"io/ioutil"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	_ "embed"
	_ "github.com/autom8ter/gokvkit/kv/badger"
)

var (
	//go:embed testdata/task.json
	TaskSchema string
	//go:embed testdata/user.json
	UserSchema     string
	TaskCollection = gokvkit.NewCollection("task", "_id",
		gokvkit.WithIndex(gokvkit.Index{
			Collection: "task",
			Name:       "task_user_idx",
			Fields:     []string{"user"},
			Unique:     false,
			Primary:    false,
		}),
		gokvkit.WithValidatorHooks(gokvkit.MustJSONSchema([]byte(TaskSchema))),
	)
	UserCollection = gokvkit.NewCollection("user", "_id",
		gokvkit.WithIndex(gokvkit.Index{
			Collection: "user",
			Name:       "user_lanaguage_idx",
			Fields:     []string{"language"},
			Unique:     false,
			Primary:    false,
		}),
		gokvkit.WithIndex(gokvkit.Index{
			Collection: "user",
			Name:       "user_email_idx",
			Fields:     []string{"contact.email"},
			Unique:     true,
			Primary:    false,
		}),
		gokvkit.WithIndex(gokvkit.Index{
			Collection: "user",
			Name:       "user_account_idx",
			Fields:     []string{"account_id"},
			Unique:     false,
			Primary:    false,
		}),
		gokvkit.WithValidatorHooks(gokvkit.MustJSONSchema([]byte(UserSchema))),
	)
	AllCollections = []*gokvkit.Collection{UserCollection, TaskCollection}
)

func NewUserDoc() *gokvkit.Document {
	doc, err := gokvkit.NewDocumentFrom(map[string]interface{}{
		"_id":  gofakeit.UUID(),
		"name": gofakeit.Name(),
		"contact": map[string]interface{}{
			"email": fmt.Sprintf("%v.%s", gofakeit.IntRange(0, 100), gofakeit.Email()),
		},
		"account_id":      gofakeit.IntRange(0, 100),
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

func NewTaskDoc(usrID string) *gokvkit.Document {
	doc, err := gokvkit.NewDocumentFrom(map[string]interface{}{
		"_id":     gofakeit.UUID(),
		"user":    usrID,
		"content": gofakeit.LoremIpsumSentence(5),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

func TestDB(fn func(ctx context.Context, db *gokvkit.DB), collections ...*gokvkit.Collection) error {
	if len(collections) == 0 {
		collections = append(collections, AllCollections...)
	}
	os.MkdirAll("tmp", 0700)
	dir, err := ioutil.TempDir("./tmp", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := gokvkit.New(ctx, gokvkit.Config{
		KV: gokvkit.KVConfig{
			Provider: "badger",
			Params: map[string]any{
				"storage_path": dir,
			},
		},
		Collections: collections,
	})
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Second)
	defer db.Close(ctx)
	fn(ctx, db)
	return nil
}
