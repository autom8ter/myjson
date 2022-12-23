package testutil

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/autom8ter/gokvkit"

	"github.com/brianvoe/gofakeit/v6"

	_ "embed"

	_ "github.com/autom8ter/gokvkit/kv/badger"
)

var (
	//go:embed testdata/task.yaml
	TaskSchema string
	//go:embed testdata/user.yaml
	UserSchema string
	//go:embed testdata/account.yaml
	AccountSchema  string
	AllCollections = [][]byte{[]byte(UserSchema), []byte(TaskSchema), []byte(AccountSchema)}
)

func NewUserDoc() *gokvkit.Document {
	doc, err := gokvkit.NewDocumentFrom(map[string]interface{}{
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

func TestDB(fn func(ctx context.Context, db gokvkit.Database), collections ...[]byte) error {
	collections = append(collections, AllCollections...)
	os.MkdirAll("tmp", 0700)
	dir, err := ioutil.TempDir("./tmp", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := gokvkit.New(ctx, "badger", map[string]any{
		"storage_path": dir,
	})
	if err != nil {
		return err
	}
	for _, c := range collections {
		if err := db.ConfigureCollection(ctx, c); err != nil {
			return err
		}
	}

	if err := db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
		for i := 0; i <= 100; i++ {
			d, _ := gokvkit.NewDocumentFrom(map[string]any{
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
	if err := db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
		for i := 0; i <= 100; i++ {
			if err := tx.Delete(ctx, "account", fmt.Sprint(i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
