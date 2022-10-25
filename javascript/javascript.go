package javascript

import (
	"github.com/dop251/goja"
	"github.com/palantir/stacktrace"
)

// Object is a generic javascript object
type Object map[string]any

// Function is a javascript function
type Function struct {
	functions map[string]func(interface{}) (interface{}, error)
	Functions []string `json:"name"`
	Script    string   `json:"script"`
}

// NewFunction creates a new javascript function
func NewFunction(functions []string, script string) Function {
	return Function{
		Functions: functions,
		Script:    script,
	}
}

// Compile pre-compiles the function's script for better performance
func (f Function) Compile() (Function, error) {
	vm := goja.New()

	_, err := vm.RunString(f.Script)
	if err != nil {
		return Function{}, stacktrace.Propagate(err, "")
	}
	var functions = map[string]func(interface{}) (interface{}, error){}
	for _, functionName := range f.Functions {
		var function func(interface{}) (interface{}, error)
		if err := vm.ExportTo(vm.Get(functionName), &function); err != nil {
			return Function{}, stacktrace.Propagate(err, "")
		}
		functions[functionName] = function
	}

	return Function{
		functions: functions,
		Functions: f.Functions,
		Script:    f.Script,
	}, nil
}

// Exec executes a javascript function
func (f Function) Exec(function string, input any) (any, error) {
	fn, ok := f.functions[function]
	if !ok {
		return nil, stacktrace.NewError("unknown function: %s", function)
	}
	return fn(input)
}
