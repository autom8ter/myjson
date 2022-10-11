package testutil

import (
	"context"
	"time"

	"github.com/brianvoe/gofakeit/v6"

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
	UserCollection = &wolverine.Collection{
		Schema: `
{
  "$id": "https://example.com/user.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "User",
  "collection": "user",
  "type": "object",
  "full_text": true,
  "indexes": [
    {
      "fields": [
        "contact.email"
      ]
    }
  ],
  "required": [
    "_id",
    "name",
    "age",
    "contact",
    "gender",
    "account_id"
  ],
  "properties": {
    "_id": {
      "type": "string",
      "description": "The user's id."
    },
    "name": {
      "type": "string",
      "description": "The user's name."
    },
    "contact": {
      "type": "object",
      "properties": {
        "email": {
          "type": "string",
          "description": "The user's email."
        }
      }
    },
    "age": {
      "description": "Age in years which must be equal to or greater than zero.",
      "type": "integer",
      "minimum": 0
    },
    "account_id": {
      "type": "integer",
      "minimum": 0
    },
    "language": {
      "type": "string",
      "description": "The user's first language."
    },
    "gender": {
      "type": "string",
      "description": "The user's gender.",
      "enum": [
        "male",
        "female"
      ]
    },
    "timestamp": {
      "type": "string"
    },
    "annotations": {
      "type": "object"
    }
  }
}
`}
	TaskCollection = &wolverine.Collection{
		Schema: `
{
  "$id": "https://example.com/task.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Task",
  "collection": "task",
  "type": "object",
  "indexes": [
    {
      "fields": [
        "user"
      ]
    }
  ],
  "required": [
    "_id",
    "user",
    "content"
  ],
  "properties": {
    "_id": {
      "type": "string",
      "description": "The user's id."
    },
    "user": {
      "type": "string",
      "description": "The id of the user who owns the task"
    },
    "content": {
      "type": "string",
      "description": "The content of the task"
    }
  }
}
`}
	AllCollections = []*wolverine.Collection{UserCollection, TaskCollection}
)

func NewUserDoc() *wolverine.Document {
	doc, err := wolverine.NewDocumentFromMap(map[string]interface{}{
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
	doc, err := wolverine.NewDocumentFromMap(map[string]interface{}{
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

func TestDB(collections []*wolverine.Collection, fn func(ctx context.Context, db wolverine.DB)) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := wolverine.New(ctx, wolverine.Config{
		Path:  "inmem",
		Debug: true,
	})
	if err != nil {
		return err
	}
	if err := db.SetCollections(ctx, collections); err != nil {
		return err
	}
	defer db.Close(ctx)
	fn(ctx, db)
	return nil
}
