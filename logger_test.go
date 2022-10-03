package wolverine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
)

func TestLogger(t *testing.T) {
	t.Run("debug", func(t *testing.T) {
		logger, err := wolverine.NewLogger("debug", map[string]any{})
		assert.Nil(t, err)
		assert.NotNil(t, logger)
		logger.Debug(context.Background(), "debug logger", nil)
	})
	t.Run("info", func(t *testing.T) {
		logger, err := wolverine.NewLogger("info", map[string]any{})
		assert.Nil(t, err)
		assert.NotNil(t, logger)
		logger.Info(context.Background(), "info logger", nil)
	})
	t.Run("warn", func(t *testing.T) {
		logger, err := wolverine.NewLogger("warn", map[string]any{})
		assert.Nil(t, err)
		assert.NotNil(t, logger)
		logger.Warn(context.Background(), "warn logger", nil)
	})
	t.Run("error", func(t *testing.T) {
		logger, err := wolverine.NewLogger("error", map[string]any{})
		assert.Nil(t, err)
		assert.NotNil(t, logger)
		logger.Error(context.Background(), "error logger", fmt.Errorf("this is an error"), nil)
	})
}
