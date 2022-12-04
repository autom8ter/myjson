package testutil

import (
	"context"
	"fmt"
	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/model"
	"github.com/palantir/stacktrace"
	"io/ioutil"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	_ "embed"
	_ "github.com/autom8ter/gokvkit/kv/badger"
)

var (
	//go:embed testdata/task.yaml
	TaskSchema string
	//go:embed testdata/user.yaml
	UserSchema     string
	AllCollections = [][]byte{[]byte(UserSchema), []byte(TaskSchema)}
)

func NewUserDoc() *model.Document {
	doc, err := model.NewDocumentFrom(map[string]interface{}{
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

func NewTaskDoc(usrID string) *model.Document {
	doc, err := model.NewDocumentFrom(map[string]interface{}{
		"_id":     gofakeit.UUID(),
		"user":    usrID,
		"content": gofakeit.LoremIpsumSentence(5),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

func TestDB(fn func(ctx context.Context, db *gokvkit.DB), collections ...[]byte) error {
	collections = append(collections, AllCollections...)
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
	})
	if err != nil {
		return err
	}
	for _, c := range collections {
		if err := db.ConfigureCollection(ctx, c); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	time.Sleep(1 * time.Second)
	defer db.Close(ctx)
	fn(ctx, db)
	return nil
}
