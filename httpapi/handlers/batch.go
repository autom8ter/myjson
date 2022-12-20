package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"

	"github.com/go-chi/chi/v5"
)

func BatchSetHandler(o api.OpenAPIServer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		var docs []*gokvkit.Document
		if err := json.NewDecoder(r.Body).Decode(&docs); err != nil {
			httpError.Error(w, errors.Wrap(err, errors.Validation, "failed to decode query"))
			return
		}
		if err := o.DB().Tx(r.Context(), true, func(ctx context.Context, tx gokvkit.Tx) error {
			for _, d := range docs {
				if err := tx.Set(ctx, collection, d); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
