package handlers

import (
	"context"
	"net/http"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"

	"github.com/gorilla/websocket"
)

type TxAction string

const (
	Rollback TxAction = "rollback"
	Commit   TxAction = "commit"
	Set      TxAction = "set"
	Delete   TxAction = "delete"
	Update   TxAction = "update"
	Create   TxAction = "create"
)

// TxInput is an input to a transaction
type TxInput struct {
	Action     TxAction          `json:"action,omitempty"`
	Collection string            `json:"collection,omitempty"`
	DocID      string            `json:"docID,omitempty"`
	Value      *gokvkit.Document `json:"value,omitempty"`
}

// TxOutput is an output of a transaction
type TxOutput struct {
	Value *gokvkit.Document `json:"value,omitempty"`
	Error *errors.Error     `json:"error,omitempty"`
}

func TxHandler(o api.OpenAPIServer, upgrader websocket.Upgrader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to upgrade socket tx request"))
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		tx := o.DB().NewTx(true)
		for {
			select {
			case <-ctx.Done():
				conn.Close()
				return
			default:
				var msg TxInput
				if err := conn.ReadJSON(&msg); err != nil {
					conn.WriteJSON(&TxOutput{
						Value: nil,
						Error: errors.Extract(err),
					})
					continue
				}
				switch msg.Action {
				case Rollback:
					tx.Rollback(ctx)
					cancel()
					break
				case Commit:
					if err := tx.Commit(ctx); err != nil {
						conn.WriteJSON(&TxOutput{
							Value: nil,
							Error: errors.Extract(err),
						})
					}
					cancel()
					break
				case Set:
					err := tx.Set(ctx, msg.Collection, msg.Value)
					var output = &TxOutput{
						Value: msg.Value,
						Error: errors.Extract(err),
					}
					conn.WriteJSON(output)
				case Update:
					err := tx.Update(ctx, msg.Collection, msg.DocID, msg.Value.Value())
					var output = &TxOutput{
						Value: msg.Value,
						Error: errors.Extract(err),
					}
					conn.WriteJSON(output)
				case Create:
					_, err := tx.Create(ctx, msg.Collection, msg.Value)
					var output = &TxOutput{
						Value: msg.Value,
						Error: errors.Extract(err),
					}
					conn.WriteJSON(output)
				case Delete:
					err := tx.Delete(ctx, msg.Collection, msg.DocID)
					var output = &TxOutput{
						Value: nil,
						Error: errors.Extract(err),
					}
					conn.WriteJSON(output)
				}
			}
		}
	}
}
