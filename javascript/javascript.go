package javascript

import (
	"github.com/dop251/goja"
	"github.com/palantir/stacktrace"
)

type Function func(interface{}) (interface{}, error)

type Script string

func (s Script) Parse() (Function, error) {
	name := getFunctionName(string(s))
	vm := goja.New()
	_, err := vm.RunString(string(s))
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	var function func(interface{}) (interface{}, error)
	if err := vm.ExportTo(vm.Get(name), &function); err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return function, nil
}
