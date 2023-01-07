package openapi

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/util"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
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
	params        Config
	router        *mux.Router
	mwares        []mux.MiddlewareFunc
	upgrader      websocket.Upgrader
	spec          []byte
	specMu        sync.RWMutex
	openapiRouter routers.Router
	logger        *zap.Logger
}

// New creates a new openapi server
func New(params Config, mwares ...mux.MiddlewareFunc) (myjson.Transport, error) {
	if err := util.ValidateStruct(params); err != nil {
		return nil, err
	}
	l, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	o := &openAPIServer{
		params:   params,
		router:   mux.NewRouter(),
		mwares:   mwares,
		upgrader: websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 1024},
		logger:   l,
	}
	return o, nil
}

func getSpec(ctx context.Context, config Config, db myjson.Database) ([]byte, error) {
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
		"title":        config.Title,
		"description":  config.Description,
		"version":      config.Version,
		"query_schema": myjson.QuerySchema(),
		"page_schema":  myjson.PageSchema(),
		"collections":  coll,
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (o *openAPIServer) registerRoutes(ctx context.Context, db myjson.Database) error {
	if err := o.refreshSpec(db); err != nil {
		return err
	}
	o.router.Use(o.mwares...)
	o.router.HandleFunc("/openapi.yaml", o.specHandler()).Methods(http.MethodGet)
	o.router.HandleFunc("/openapi.json", o.specHandler()).Methods(http.MethodGet)
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
	return nil
}

func (o *openAPIServer) refreshSpec(db myjson.Database) error {
	o.specMu.Lock()
	defer o.specMu.Unlock()
	spec, err := getSpec(context.Background(), o.params, db)
	if err != nil {
		return err
	}
	o.spec = spec
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(o.spec)
	if err != nil {
		return err
	}
	if err := doc.Validate(loader.Context); err != nil {
		return err
	}
	router, err := gorillamux.NewRouter(doc)
	if err != nil {
		return err
	}
	o.openapiRouter = router
	return nil
}

// Serve starts an openapi http server serving the database
func (o *openAPIServer) Serve(ctx context.Context, db myjson.Database) error {
	defer o.logger.Sync()
	if err := o.registerRoutes(ctx, db); err != nil {
		return err
	}
	egp, ctx := errgroup.WithContext(ctx)
	egp.Go(func() error {
		return http.ListenAndServe(fmt.Sprintf(":%v", o.params.Port), o.router)
	})
	egp.Go(func() error {
		ticker := time.NewTimer(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := o.refreshSpec(db); err != nil {
					return err
				}
			}
		}
	})
	return egp.Wait()
}
