package handlers

import (
	"context"
	"net/http"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"
	"github.com/go-chi/chi/v5"
)

func DeleteDocHandler(o api.OpenAPIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		if err := o.DB().Tx(r.Context(), true, func(ctx context.Context, tx gokvkit.Tx) error {
			err := tx.Delete(ctx, collection, docID)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
