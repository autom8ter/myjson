package openapi

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestTx(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			o, err := New(Config{
				Title:       "testing",
				Version:     "v0.0.0",
				Description: "testing openapi schema",
				Port:        8080,
			})
			assert.NoError(t, err)
			oapi := o.(*openAPIServer)
			assert.NoError(t, oapi.registerRoutes(ctx, db))
			s := httptest.NewServer(oapi.router)
			defer s.Close()
			client := NewTxClient(s.URL)
			tx, err := client.NewTx(nil)
			assert.NoError(t, err, s.URL)

			defer tx.Close()
			for i := 0; i < 100; i++ {
				assert.NoError(t, tx.Write(ctx, TxInput{
					Action:     Set,
					Collection: "account",
					DocID:      "0",
					Value: myjson.D().Set(map[string]any{
						"name": gofakeit.Company(),
					}).Doc(),
				}))
				out, err := tx.Read(ctx)
				assert.NoError(t, err)
				fmt.Println(out)
			}
			assert.NoError(t, tx.Write(ctx, TxInput{
				Action: Commit,
			}))
		}))
	})
	t.Run("commit", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			o, err := New(Config{
				Title:       "testing",
				Version:     "v0.0.0",
				Description: "testing openapi schema",
				Port:        8080,
			})
			assert.NoError(t, err)
			oapi := o.(*openAPIServer)
			assert.NoError(t, oapi.registerRoutes(ctx, db))
			s := httptest.NewServer(oapi.router)
			defer s.Close()
			client := NewTxClient(s.URL)
			tx, err := client.NewTx(nil)
			assert.NoError(t, err, s.URL)

			defer tx.Close()
			for i := 0; i < 100; i++ {
				assert.NoError(t, tx.Write(ctx, TxInput{
					Action:     Set,
					Collection: "account",
					DocID:      "0",
					Value: myjson.D().Set(map[string]any{
						"name": gofakeit.Company(),
					}).Doc(),
				}))
				out, err := tx.Read(ctx)
				assert.NoError(t, err)
				fmt.Println(out)
			}
			assert.NoError(t, tx.Write(ctx, TxInput{
				Action: Rollback,
			}))
		}))
	})
	t.Run("query", func(t *testing.T) {
		assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			o, err := New(Config{
				Title:       "testing",
				Version:     "v0.0.0",
				Description: "testing openapi schema",
				Port:        8080,
			})
			assert.NoError(t, err)
			oapi := o.(*openAPIServer)
			assert.NoError(t, oapi.registerRoutes(ctx, db))
			s := httptest.NewServer(oapi.router)
			defer s.Close()
			client := NewTxClient(s.URL)
			tx, err := client.NewTx(nil)
			assert.NoError(t, err, s.URL)

			defer tx.Close()
			assert.NoError(t, tx.Write(ctx, TxInput{
				Action:     Query,
				Collection: "account",
				Query:      myjson.Q().Query(),
			}))
			out, err := tx.Read(ctx)
			assert.NoError(t, err)
			fmt.Println(out)
		}))
	})
}
