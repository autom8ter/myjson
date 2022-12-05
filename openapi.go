package gokvkit

import (
	"bytes"
	_ "embed"
	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/autom8ter/gokvkit/internal/util"
	"github.com/autom8ter/gokvkit/model"
	"github.com/tidwall/sjson"
	"text/template"
)

//go:embed openapi.yaml.tmpl
var openapiTemplate string

func getOpenAPISpec(collections *safe.Map[*collectionSchema], replacements map[string]any) ([]byte, error) {
	t, err := template.New("").Funcs(sprig.FuncMap()).Parse(openapiTemplate)
	if err != nil {
		return nil, err
	}
	var coll []map[string]interface{}
	querySchema, err := util.JSONToYAML([]byte(model.QuerySchema))
	if err != nil {
		return nil, err
	}
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
		"querySchema": string(querySchema),
	})
	if err != nil {
		return nil, err
	}
	var content = string(buf.Bytes())
	if replacements != nil {
		for k, v := range replacements {
			content, err = sjson.Set(content, k, v)
			if err != nil {
				return nil, err
			}
		}
	}
	return []byte(content), nil
}
