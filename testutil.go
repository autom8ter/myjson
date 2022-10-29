package wolverine

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	_ "embed"
)

func init() {
	var err error
	UserCollection, err = NewCollectionFromBytes([]byte(userSchema))
	if err != nil {
		panic(err)
	}
	TaskCollection, err = NewCollectionFromBytes([]byte(taskSchema))
	if err != nil {
		panic(err)
	}
}

var (
	//go:embed testdata/user.json
	userSchema string
	//go:embed testdata/task.json
	taskSchema     string
	TaskCollection = NewCollectionFromBytesP([]byte(taskSchema))
	UserCollection = NewCollectionFromBytesP([]byte(userSchema))
	AllCollections = []*Collection{UserCollection, TaskCollection}
)

func NewUserDoc() *Document {
	doc, err := NewDocumentFrom(map[string]interface{}{
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

func NewTaskDoc(usrID string) *Document {
	doc, err := NewDocumentFrom(map[string]interface{}{
		"_id":     gofakeit.UUID(),
		"user":    usrID,
		"content": gofakeit.LoremIpsumSentence(5),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

const MyEmail = "colemanword@gmail.com"

func TestDB(fn func(ctx context.Context, db *DB), collections ...*Collection) error {
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

	db, err := New(ctx, Config{
		Params: map[string]string{
			"storage_path": dir,
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
