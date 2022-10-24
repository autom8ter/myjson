package testutil

import (
	"context"
	"github.com/autom8ter/wolverine/schema"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	_ "embed"
	"github.com/autom8ter/wolverine"
)

func init() {
	if err := UserCollection.ParseSchema(); err != nil {
		panic(err)
	}
	if err := TaskCollection.ParseSchema(); err != nil {
		panic(err)
	}
}

var (
	//go:embed user.json
	userSchema string
	//go:embed task.json
	taskSchema     string
	UserCollection = &schema.Collection{Schema: userSchema}
	TaskCollection = &schema.Collection{Schema: taskSchema}
	AllCollections = []*schema.Collection{UserCollection, TaskCollection}
)

func NewUserDoc() *schema.Document {
	doc, err := schema.NewDocumentFrom(map[string]interface{}{
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

func NewTaskDoc(usrID string) *schema.Document {
	doc, err := schema.NewDocumentFrom(map[string]interface{}{
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

func TestDB(collections []*schema.Collection, fn func(ctx context.Context, db *wolverine.DB)) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := wolverine.New(ctx, wolverine.Config{
		Collections: collections,
	})
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	fn(ctx, db)
	return nil
}
