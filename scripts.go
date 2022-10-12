package wolverine

import "context"

type Script func(ctx context.Context, db DB) error

func (d *db) RunScript(ctx context.Context, script Script) error {
	return script(ctx, d)
}
