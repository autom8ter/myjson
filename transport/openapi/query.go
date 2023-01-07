package openapi

import (
	"encoding/json"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
	"github.com/gorilla/mux"
)

func (o *openAPIServer) queryHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := mux.Vars(r)["collection"]
		if !db.HasCollection(r.Context(), collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		var q myjson.Query
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to decode query"))
			return
		}
		results, err := db.Query(r.Context(), collection, q)
		if err != nil {
			httpError.Error(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&results)
	})
}
