package openapi

import (
	"context"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
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
	Action     TxAction         `json:"action,omitempty"`
	Collection string           `json:"collection,omitempty"`
	DocID      string           `json:"docID,omitempty"`
	Value      *myjson.Document `json:"value,omitempty"`
}

// TxOutput is an output of a transaction
type TxOutput struct {
	Value *myjson.Document `json:"value,omitempty"`
	Error *errors.Error    `json:"error,omitempty"`
}

func (o *openAPIServer) txHandler(db myjson.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		conn, err := o.upgrader.Upgrade(w, r, nil)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to upgrade socket tx request"))
			return
		}
		defer conn.Close()
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
				var msg TxInput
				if err := conn.ReadJSON(&msg); err != nil {
					conn.WriteJSON(&TxOutput{
						Value: nil,
						Error: errors.Extract(err),
					})
					return
				}
				if msg.Collection != "" {
					if !db.HasCollection(ctx, msg.Collection) {
						conn.WriteJSON(&TxOutput{
							Value: msg.Value,
							Error: errors.Extract(errors.New(errors.Validation, "collection does not exist")),
						})
						continue
					}
					schema := db.GetSchema(ctx, msg.Collection)
					if msg.DocID != "" && msg.Value != nil && schema.GetPrimaryKey(msg.Value) == "" {
						if err := schema.SetPrimaryKey(msg.Value, msg.DocID); err != nil {
							conn.WriteJSON(&TxOutput{
								Value: msg.Value,
								Error: errors.Extract(errors.Wrap(err, errors.Validation, "failed to set document primary key")),
							})
							continue
						}
					}
				}

				switch msg.Action {
				case Rollback:
					tx.Rollback(ctx)
					cancel()
					return
				case Commit:
					tx.Commit(ctx)
					cancel()
					return
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

type TxClient struct {
	conn *websocket.Conn
}

func (t TxClient) Process(ctx context.Context, input chan TxInput) (chan TxOutput, chan error) {
	output := make(chan TxOutput)
	errs := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		egp, ctx := errgroup.WithContext(ctx)
		egp.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					close(output)
					return nil
				default:
					var msg TxOutput
					if err := t.conn.ReadJSON(&msg); err != nil {
						return nil
					}
					output <- msg
				}
			}
		})
		egp.Go(func() error {
			for msg := range input {
				if err := t.conn.WriteJSON(msg); err != nil {
					return err
				}
			}
			cancel()
			return nil
		})
		errs <- egp.Wait()
		t.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		close(errs)
	}()
	return output, errs
}
