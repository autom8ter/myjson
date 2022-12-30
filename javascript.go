package gokvkit

import (
	"context"

	"github.com/dop251/goja"
)

func getJavascriptVM(ctx context.Context, db Database) (*goja.Runtime, error) {
	vm := goja.New()
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	if err := vm.Set("db", db); err != nil {
		return nil, err
	}
	if err := vm.Set("ctx", ctx); err != nil {
		return nil, err
	}
	if err := vm.Set("newDocumentFrom", NewDocumentFrom); err != nil {
		return nil, err
	}
	if err := vm.Set("newDocument", NewDocument); err != nil {
		return nil, err
	}
	return vm, nil
}
