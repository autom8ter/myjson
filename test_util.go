package gokvkit

import (
	"fmt"
	"time"

	"github.com/autom8ter/gokvkit/model"
	"github.com/brianvoe/gofakeit/v6"
)

func newUserDoc() *model.Document {
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