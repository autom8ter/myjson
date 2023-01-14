package testutil

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"

	"github.com/brianvoe/gofakeit/v6"
	// import embed package
	_ "embed"

	// import badger kv provider
	_ "github.com/autom8ter/myjson/kv/badger"
)

var (
	//go:embed testdata/task.yaml
	TaskSchema string
	//go:embed testdata/user.yaml
	UserSchema string
	//go:embed testdata/account.yaml
	AccountSchema  string
	AllCollections = map[string]string{
		"task":    TaskSchema,
		"user":    UserSchema,
		"account": AccountSchema,
	}
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

func TestDB(fn func(ctx context.Context, db myjson.Database), opts ...myjson.DBOpt) error {
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
	if err := db.Configure(ctx, AllCollections); err != nil {
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
