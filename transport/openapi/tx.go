package openapi

import (
	"context"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
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
	Query    TxAction = "query"
)

// TxInput is an input to a transaction
type TxInput struct {
	Action     TxAction         `json:"action,omitempty"`
	Collection string           `json:"collection,omitempty"`
	DocID      string           `json:"docID,omitempty"`
	Value      *myjson.Document `json:"value,omitempty"`
	Query      myjson.Query     `json:"query,omitempty"`
}

// TxOutput is an output of a transaction
type TxOutput struct {
	Input  TxInput          `json:"input,omitempty"`
	Result *myjson.Document `json:"result,omitempty"`
	Error  *errors.Error    `json:"error,omitempty"`
}

func (o *openAPIServer) txHandler(db myjson.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		conn, err := o.upgrader.Upgrade(w, r, nil)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to upgrade socket tx request"))
			return
		}
		defer conn.Close()
		defer conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		tx, err := db.NewTx(kv.TxOpts{
			IsReadOnly: r.URL.Query().Get("readonly") == "true",
			IsBatch:    r.URL.Query().Get("batch") == "true",
		})
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to create tx"))
			return
		}
		defer tx.Close(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				var txInput TxInput
				if err := conn.ReadJSON(&txInput); err != nil {
					conn.WriteJSON(&TxOutput{
						Input: txInput,
						Error: errors.Extract(err),
					})
					return
				}
				if txInput.Collection != "" {
					if !db.HasCollection(ctx, txInput.Collection) {
						conn.WriteJSON(&TxOutput{
							Input:  txInput,
							Result: txInput.Value,
							Error:  errors.Extract(errors.New(errors.Validation, "collection does not exist")),
						})
						continue
					}
					schema := db.GetSchema(ctx, txInput.Collection)
					if txInput.DocID != "" && txInput.Value != nil && schema.GetPrimaryKey(txInput.Value) == "" {
						if err := schema.SetPrimaryKey(txInput.Value, txInput.DocID); err != nil {
							conn.WriteJSON(&TxOutput{
								Input:  txInput,
								Result: txInput.Value,
								Error:  errors.Extract(errors.Wrap(err, errors.Validation, "failed to set document primary key")),
							})
							continue
						}
					}
				}

				switch txInput.Action {
				case Rollback:
					tx.Rollback(ctx)
					cancel()
					return
				case Commit:
					tx.Commit(ctx)
					cancel()
					return
				case Set:
					err := tx.Set(ctx, txInput.Collection, txInput.Value)
					var output = &TxOutput{
						Input:  txInput,
						Result: txInput.Value,
						Error:  errors.Extract(err),
					}
					conn.WriteJSON(output)
				case Update:
					err := tx.Update(ctx, txInput.Collection, txInput.DocID, txInput.Value.Value())
					var output = &TxOutput{
						Input:  txInput,
						Result: txInput.Value,
						Error:  errors.Extract(err),
					}
					conn.WriteJSON(output)
				case Create:
					_, err := tx.Create(ctx, txInput.Collection, txInput.Value)
					var output = &TxOutput{
						Input:  txInput,
						Result: txInput.Value,
						Error:  errors.Extract(err),
					}
					conn.WriteJSON(output)
				case Delete:
					err := tx.Delete(ctx, txInput.Collection, txInput.DocID)
					var output = &TxOutput{
						Input:  txInput,
						Result: nil,
						Error:  errors.Extract(err),
					}
					conn.WriteJSON(output)
				case Query:
					results, err := tx.Query(ctx, txInput.Collection, txInput.Query)
					if err != nil {
						conn.WriteJSON(&TxOutput{
							Input:  txInput,
							Result: nil,
							Error:  errors.Extract(err),
						})
						continue
					}
					doc, err := myjson.NewDocumentFrom(results)
					if err != nil {
						conn.WriteJSON(&TxOutput{
							Input:  txInput,
							Result: nil,
							Error:  errors.Extract(err),
						})
						continue
					}
					var output = &TxOutput{
						Input:  txInput,
						Result: doc,
						Error:  errors.Extract(err),
					}
					conn.WriteJSON(output)
				}
			}
		}
	}
}
