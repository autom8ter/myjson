package schema

import (
	"context"
	"github.com/autom8ter/wolverine/internal/util"
	"github.com/autom8ter/wolverine/javascript"
	"github.com/palantir/stacktrace"
)

type HookPoint string

const (
	BeforeQuery       HookPoint = "beforeQuery"
	BeforeStateChange HookPoint = "beforeStateChange"
)

type Hook struct {
	Name            string
	HookPoint       HookPoint
	Func            HookFunc
	IsInternal      bool
	IsDeterministic bool
}

type HookFunc func(ctx context.Context, input any) (context.Context, any, error)

func JavascriptHookFunc(script string) HookFunc {
	return func(ctx context.Context, input any) (context.Context, any, error) {
		fn, err := javascript.Script(script).Parse()
		if err != nil {
			return ctx, nil, stacktrace.Propagate(err, "")
		}
		out, err := fn(input)
		if err != nil {
			return ctx, nil, stacktrace.Propagate(err, "")
		}
		var after StateChange
		if err := util.Decode(out, after); err != nil {
			return ctx, nil, stacktrace.Propagate(err, "")
		}
		return ctx, after, nil
	}
}
