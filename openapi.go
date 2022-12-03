package gokvkit

import (
	"bytes"
	_ "embed"
	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/gokvkit/internal/safe"
	"text/template"
)

//go:embed openapi.yaml.tmpl
var openapiTemplate string

func getOpenAPISpec(collections *safe.Map[*collectionSchema]) ([]byte, error) {
	t, err := template.New("").Funcs(sprig.FuncMap()).Parse(openapiTemplate)
	if err != nil {
		return nil, err
	}
	var coll []map[string]interface{}
	collections.RangeR(func(key string, schema *collectionSchema) bool {
		coll = append(coll, map[string]interface{}{
			"collection": schema.collection,
			"schema":     string(schema.yamlRaw),
		})
		return true
	})
	buf := bytes.NewBuffer(nil)
	err = t.Execute(buf, map[string]any{
		"collections": coll,
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
