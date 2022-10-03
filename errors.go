package wolverine

import (
	"github.com/palantir/stacktrace"
	"github.com/pkg/errors"
)

func (d *db) wrapErr(err error, msg string) error {
	if err != nil {
		if d.Config().Debug {
			return stacktrace.Propagate(err, msg)
		}
		return errors.Wrap(err, msg)
	}
	return nil
}
