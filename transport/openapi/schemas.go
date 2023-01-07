package openapi

import (
	"io"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
	"github.com/ghodss/yaml"
	"github.com/gorilla/mux"
)

func (o *openAPIServer) getSchemasHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var resp = map[string]any{}
		var collections = db.Collections(r.Context())
		for _, c := range collections {
			schema, _ := db.GetSchema(r.Context(), c).MarshalYAML()
			resp[c] = string(schema)
		}
		bits, err := yaml.Marshal(&resp)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to marshal response"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bits)
	})
}

func (o *openAPIServer) getSchemaHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
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

func (o *openAPIServer) putSchemaHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
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
