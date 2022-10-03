package main

import (
	"github.com/brianvoe/gofakeit/v6"

	"github.com/autom8ter/wolverine"
)

func randomTask() *wolverine.Document {
	t, _ := wolverine.NewDocumentFromMap(map[string]interface{}{
		"_id":         gofakeit.UUID(),
		"_collection": "task",
		"account_id":  gofakeit.IntRange(0, 50),
		"owner":       gofakeit.Email(),
		"content":     gofakeit.LoremIpsumSentence(15),
		"done":        gofakeit.Bool(),
		"created_at":  gofakeit.Date().Unix(),
	})
	return t
}
