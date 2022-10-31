package testutil

import (
	"context"
	"github.com/autom8ter/wolverine"
	"io/ioutil"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	_ "embed"
)

var (
	//go:embed testdata/task.json
	taskSchema string
	//go:embed testdata/user.json
	userSchema     string
	TaskCollection = wolverine.NewCollection("task", "_id",
		wolverine.WithIndex(wolverine.Index{
			Collection: "task",
			Name:       "task_user_idx",
			Fields:     []string{"user"},
			Unique:     false,
			Primary:    false,
		}),
		wolverine.WithValidator(wolverine.JSONSchema([]byte(taskSchema))),
	)
	UserCollection = wolverine.NewCollection("user", "_id",
		wolverine.WithIndex(wolverine.Index{
			Collection: "user",
			Name:       "user_lanaguage_idx",
			Fields:     []string{"language"},
			Unique:     false,
			Primary:    false,
		}),
		wolverine.WithIndex(wolverine.Index{
			Collection: "user",
			Name:       "user_email_idx",
			Fields:     []string{"contact.email"},
			Unique:     true,
			Primary:    false,
		}),
		wolverine.WithIndex(wolverine.Index{
			Collection: "user",
			Name:       "user_account_idx",
			Fields:     []string{"account_id"},
			Unique:     false,
			Primary:    false,
		}),
		wolverine.WithValidator(wolverine.JSONSchema([]byte(userSchema))),
	)
	AllCollections = []*wolverine.Collection{UserCollection, TaskCollection}
)

func NewUserDoc() *wolverine.Document {
	doc, err := wolverine.NewDocumentFrom(map[string]interface{}{
		"_id":  gofakeit.UUID(),
		"name": gofakeit.Name(),
		"contact": map[string]interface{}{
			"email": gofakeit.Email(),
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

func NewTaskDoc(usrID string) *wolverine.Document {
	doc, err := wolverine.NewDocumentFrom(map[string]interface{}{
		"_id":     gofakeit.UUID(),
		"user":    usrID,
		"content": gofakeit.LoremIpsumSentence(5),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

func TestDB(fn func(ctx context.Context, db *wolverine.DB), collections ...*wolverine.Collection) error {
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

	db, err := wolverine.New(ctx, wolverine.Config{
		KV: wolverine.KVConfig{
			Provider: "badger",
			Params: map[string]string{
				"storage_path": dir,
			},
		},
		Collections: collections,
	})
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	fn(ctx, db)
	return nil
}
