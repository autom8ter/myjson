package handlers

import (
	"io"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
	"github.com/gorilla/mux"
	"gopkg.in/yaml.v3"
)

func GetSchemasHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var resp = map[string]any{}
		var collections = db.Collections(r.Context())
		for _, c := range collections {
			schema, _ := db.GetSchema(r.Context(), c).MarshalYAML()
			resp[c] = string(schema)
		}
		w.WriteHeader(http.StatusOK)
		yaml.NewEncoder(w).Encode(&resp)
	})
}

func GetSchemaHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := mux.Vars(r)["collection"]
		if !db.HasCollection(r.Context(), collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		schema, _ := db.GetSchema(r.Context(), collection).MarshalYAML()
		w.WriteHeader(http.StatusOK)
		w.Write(schema)
	})
}

func PutSchemaHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bits, err := io.ReadAll(r.Body)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to read request body"))
			return
		}
		if err := db.ConfigureCollection(r.Context(), bits); err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to configure collection"))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
