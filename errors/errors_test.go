package errors_test

import (
	"fmt"
	"testing"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {
	t.Run("wrap nil error", func(t *testing.T) {
		var err error
		err = errors.Wrap(err, errors.NotFound, "")
		assert.Nil(t, err)
	})
	t.Run("wrap error", func(t *testing.T) {
		var err = fmt.Errorf("not found")
		err = errors.Wrap(err, errors.NotFound, "")
		assert.Equal(t, errors.NotFound, errors.Extract(err).Code)
	})
	t.Run("new error", func(t *testing.T) {
		err := errors.New(errors.NotFound, "not found")
		assert.Equal(t, errors.NotFound, errors.Extract(err).Code)
	})
	t.Run("new error then wrap", func(t *testing.T) {
		err := errors.New(0, "not found")
		err = errors.Wrap(err, errors.NotFound, "")
		assert.Equal(t, errors.NotFound, errors.Extract(err).Code)
	})
	t.Run("new error then wrap then remove", func(t *testing.T) {
		err := errors.New(0, "not found")
		err = errors.Wrap(err, errors.NotFound, "")
		e := errors.Extract(err).RemoveError()
		assert.Empty(t, e.Err)
	})
	t.Run("error json string", func(t *testing.T) {
		err := errors.New(0, "not found")
		err = errors.Wrap(err, errors.NotFound, "")
		e := errors.Extract(err).RemoveError()
		assert.JSONEq(t, `{ "code":404, "messages": ["not found"]}`, e.Error())
	})
}
