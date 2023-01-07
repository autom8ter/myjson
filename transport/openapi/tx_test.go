package openapi

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestTx(t *testing.T) {
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
		c, _, err := websocket.DefaultDialer.Dial(strings.ReplaceAll(s.URL, "http", "ws")+"/api/tx", nil)
		assert.NoError(t, err, s.URL)
		client := TxClient{conn: c}
		input := make(chan TxInput)
		done := client.Process(ctx, input, func(output TxOutput) error {
			fmt.Println(output)
			return nil
		})
		for i := 0; i < 100; i++ {
			input <- TxInput{
				Action:     Set,
				Collection: "account",
				DocID:      "0",
				Value: myjson.D().Set(map[string]any{
					"name": gofakeit.Company(),
				}).Doc(),
			}
		}
		input <- TxInput{
			Action: Commit,
		}
		assert.NoError(t, <-done)
	}))
}
