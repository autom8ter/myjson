package model_test

import (
	"testing"
	"time"

	"github.com/autom8ter/gokvkit/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestCommand(t *testing.T) {
	t.Run("basic validation", func(t *testing.T) {
		c := model.Command{}
		assert.NotNil(t, c.Validate())
		c.Collection = "task"
		assert.NotNil(t, c.Validate())
		c.Metadata = model.NewMetadata(map[string]any{})
		assert.NotNil(t, c.Validate())
		c.Timestamp = time.Now()
		assert.NotNil(t, c.Validate())
		c.After = model.NewDocument()
		assert.NotNil(t, c.Validate())
		c.Action = model.Set
		assert.NotNil(t, c.Validate())
		c.DocID = gofakeit.UUID()
		assert.Nil(t, c.Validate())
	})
	t.Run("delete validation", func(t *testing.T) {
		c := &model.Command{
			Collection: "task",
			Action:     model.Delete,
			DocID:      gofakeit.UUID(),
			Before:     model.NewDocument(),
			After:      nil,
			Timestamp:  time.Now(),
			Metadata:   model.NewMetadata(map[string]any{}),
		}
		assert.Nil(t, c.Validate())
	})
	t.Run("delete validation error", func(t *testing.T) {
		c := &model.Command{
			Collection: "task",
			Action:     model.Delete,
			DocID:      gofakeit.UUID(),
			After:      model.NewDocument(),
			Timestamp:  time.Now(),
			Metadata:   model.NewMetadata(map[string]any{}),
		}
		assert.NotNil(t, c.Validate())
	})
	t.Run("set validation", func(t *testing.T) {
		c := &model.Command{
			Collection: "task",
			Action:     model.Set,
			DocID:      gofakeit.UUID(),
			After:      model.NewDocument(),
			Timestamp:  time.Now(),
			Metadata:   model.NewMetadata(map[string]any{}),
		}
		assert.Nil(t, c.Validate())
	})
	t.Run("set validation error", func(t *testing.T) {
		c := &model.Command{
			Collection: "task",
			Action:     model.Set,
			DocID:      gofakeit.UUID(),
			Before:     model.NewDocument(),
			Timestamp:  time.Now(),
			Metadata:   model.NewMetadata(map[string]any{}),
		}
		assert.NotNil(t, c.Validate())
	})
	t.Run("update validation", func(t *testing.T) {
		c := &model.Command{
			Collection: "task",
			Action:     model.Update,
			DocID:      gofakeit.UUID(),
			After:      model.NewDocument(),
			Timestamp:  time.Now(),
			Metadata:   model.NewMetadata(map[string]any{}),
		}
		assert.Nil(t, c.Validate())
	})
	t.Run("update validation error", func(t *testing.T) {
		c := &model.Command{
			Collection: "task",
			Action:     model.Update,
			DocID:      gofakeit.UUID(),
			Before:     model.NewDocument(),
			Timestamp:  time.Now(),
			Metadata:   model.NewMetadata(map[string]any{}),
		}
		assert.NotNil(t, c.Validate())
	})
	t.Run("create validation", func(t *testing.T) {
		c := &model.Command{
			Collection: "task",
			Action:     model.Create,
			DocID:      gofakeit.UUID(),
			After:      model.NewDocument(),
			Timestamp:  time.Now(),
			Metadata:   model.NewMetadata(map[string]any{}),
		}
		assert.Nil(t, c.Validate())
	})
	t.Run("create validation error", func(t *testing.T) {
		c := &model.Command{
			Collection: "task",
			Action:     model.Create,
			DocID:      gofakeit.UUID(),
			Before:     model.NewDocument(),
			Timestamp:  time.Now(),
			Metadata:   model.NewMetadata(map[string]any{}),
		}
		assert.NotNil(t, c.Validate())
	})
}
