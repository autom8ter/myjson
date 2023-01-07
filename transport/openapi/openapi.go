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
	"github.com/autom8ter/myjson/util"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
)

//go:embed openapi.yaml.tmpl
var openapiTemplate string

// Config are custom params for generating an openapi specification
type Config struct {
	Title       string `json:"title" yaml:"title" validate:"required"`
	Version     string `json:"version" yaml:"version" validate:"required"`
	Description string `json:"description" yaml:"description" validate:"required"`
	Port        int    `json:"port" yaml:"port" validate:"required"`
}

type openAPIServer struct {
	params   Config
	router   *mux.Router
	mwares   []mux.MiddlewareFunc
	upgrader websocket.Upgrader
}

// New creates a new openapi server
func New(params Config, mwares ...mux.MiddlewareFunc) (myjson.Transport, error) {
	if err := util.ValidateStruct(params); err != nil {
		return nil, err
	}
	o := &openAPIServer{
		params:   params,
		router:   mux.NewRouter(),
		mwares:   mwares,
		upgrader: websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 1024},
	}
	return o, nil
}

func (o *openAPIServer) getSpec(ctx context.Context, db myjson.Database) ([]byte, error) {
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
		"title":        o.params.Title,
		"description":  o.params.Description,
		"version":      o.params.Version,
		"query_schema": myjson.QuerySchema(),
		"page_schema":  myjson.PageSchema(),
		"collections":  coll,
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (o *openAPIServer) registerRoutes(db myjson.Database) {
	o.router.Use(o.mwares...)
	o.router.HandleFunc("/api/openapi.yaml", o.specHandler(db)).Methods(http.MethodGet)
	o.router.HandleFunc("/api/tx", o.txHandler(db))
	o.router.HandleFunc("/api/schema", o.getSchemasHandler(db)).Methods(http.MethodGet)
	o.router.HandleFunc("/api/schema/{collection}", o.getSchemaHandler(db)).Methods(http.MethodGet)
	o.router.HandleFunc("/api/schema/{collection}", o.putSchemaHandler(db)).Methods(http.MethodPut)
	o.router.HandleFunc("/api/collections/{collection}/docs", o.createDocHandler(db)).Methods(http.MethodPost)
	o.router.HandleFunc("/api/collections/{collection}/docs/{docID}", o.setDocHandler(db)).Methods(http.MethodPut)
	o.router.HandleFunc("/api/collections/{collection}/docs/{docID}", o.patchDocHandler(db)).Methods(http.MethodPatch)
	o.router.HandleFunc("/api/collections/{collection}/docs/{docID}", o.deleteDocHandler(db)).Methods(http.MethodDelete)
	o.router.HandleFunc("/api/collections/{collection}/docs/{docID}", o.getDocHandler(db)).Methods(http.MethodGet)
	o.router.HandleFunc("/api/collections/{collection}/query", o.queryHandler(db)).Methods(http.MethodPost)
	o.router.HandleFunc("/api/collections/{collection}/batch", o.batchSetHandler(db)).Methods(http.MethodPut)
}

// Serve starts an openapi http server serving the database
func (o *openAPIServer) Serve(ctx context.Context, db myjson.Database) error {
	o.registerRoutes(db)
	egp, ctx := errgroup.WithContext(ctx)
	egp.Go(func() error {
		return http.ListenAndServe(fmt.Sprintf(":%v", o.params.Port), o.router)
	})
	return egp.Wait()
}
