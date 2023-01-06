package openapi

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/transport/openapi/handlers"
	"github.com/autom8ter/myjson/util"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
)

//go:embed templates/openapi.yaml.tmpl
var openapiTemplate string

// Config are custom params for generating an openapi specification
type Config struct {
	Title       string `json:"title" yaml:"title" validate:"required"`
	Version     string `json:"version" yaml:"version" validate:"required"`
	Description string `json:"description" yaml:"description" validate:"required"`
	Port        int    `json:"port" yaml:"port" validate:"required"`
}

type openAPIServer struct {
	params Config
	router *mux.Router
	mwares []mux.MiddlewareFunc
}

// New creates a new openapi server
func New(params Config, mwares ...mux.MiddlewareFunc) (myjson.Transport, error) {
	if err := util.ValidateStruct(params); err != nil {
		return nil, err
	}
	o := &openAPIServer{
		params: params,
		router: mux.NewRouter(),
		mwares: mwares,
	}
	return o, nil
}

func getSpec(ctx context.Context, db myjson.Database, cfg Config) ([]byte, error) {
	t, err := template.New("").Funcs(sprig.FuncMap()).Parse(openapiTemplate)
	if err != nil {
		return nil, err
	}
	var coll []map[string]interface{}
	var collections = db.Collections(ctx)
	for _, c := range collections {
		schema, _ := db.GetSchema(ctx, c).MarshalYAML()
		coll = append(coll, map[string]interface{}{
			"collection": c,
			"schema":     string(schema),
		})
	}
	buf := bytes.NewBuffer(nil)
	err = t.Execute(buf, map[string]any{
		"title":       cfg.Title,
		"description": cfg.Description,
		"version":     cfg.Version,
		"collections": coll,
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Serve starts an openapi http server serving the database
func (o *openAPIServer) Serve(ctx context.Context, db myjson.Database) error {
	r := o.router.Path("/api").Subrouter()
	r.Use(o.mwares...)
	r.Path("/openapi.yaml").
		Methods(http.MethodGet).
		HandlerFunc(handlers.SpecHandler(func(ctx context.Context) ([]byte, error) {
			return getSpec(ctx, db, o.params)
		}))
	r.HandleFunc("/tx", handlers.TxHandler(db, websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}))
	r.Path("/schema").
		Methods(http.MethodGet).
		HandlerFunc(handlers.GetSchemasHandler(db))
	r.Path("/schema/{collection}").
		Methods(http.MethodGet).
		HandlerFunc(handlers.GetSchemaHandler(db))
	r.Path("/schema/{collection}").
		Methods(http.MethodPut).
		HandlerFunc(handlers.PutSchemaHandler(db))
	r.Path("/collections/{collection}/docs").
		Methods(http.MethodPost).
		HandlerFunc(handlers.CreateDocHandler(db))
	r.Path("/collections/{collection}/docs/{docID}").
		Methods(http.MethodPut).
		HandlerFunc(handlers.SetDocHandler(db))
	r.Path("/collections/{collection}/docs/{docID}").
		Methods(http.MethodPatch).
		HandlerFunc(handlers.PatchDocHandler(db))
	r.Path("/collections/{collection}/docs/{docID}").
		Methods(http.MethodDelete).
		HandlerFunc(handlers.DeleteDocHandler(db))
	r.Path("/collections/{collection}/docs/{docID}").
		Methods(http.MethodGet).
		HandlerFunc(handlers.GetDocHandler(db))
	r.Path("/collections/{collection}/query").
		Methods(http.MethodPost).
		HandlerFunc(handlers.QueryHandler(db))
	r.Path("/collections/{collection}/batch").
		Methods(http.MethodPut).
		HandlerFunc(handlers.BatchSetHandler(db))
	egp, ctx := errgroup.WithContext(ctx)
	egp.Go(func() error {
		return http.ListenAndServe(fmt.Sprintf(":%v", o.params.Port), o.router)
	})
	return egp.Wait()
}
