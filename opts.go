package myjson

import (
	"fmt"
	"strings"
)

// DBOpt is an option for configuring a collection
type DBOpt func(d *defaultDB)

// WithOptimizer overrides the default query optimizer provider
func WithOptimizer(o Optimizer) DBOpt {
	return func(d *defaultDB) {
		d.optimizer = o
	}
}

// WithJavascriptOverrides adds global variables or methods to the embedded javascript vm(s)
func WithJavascriptOverrides(overrides map[string]any) DBOpt {
	return func(d *defaultDB) {
		d.jsOverrides = overrides
	}
}

// WithGlobalJavasciptFunctions adds global javascript functions to the embedded javascript vm(s)
func WithGlobalJavascriptFunctions(scripts []string) DBOpt {
	return func(d *defaultDB) {
		d.globalScripts = fmt.Sprintln(strings.Join(scripts, "\n"))
	}
}
