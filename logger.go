package wolverine

import (
	"context"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type defaultLogger struct {
	logger *zap.Logger
}

// NewLogger returns a structured json logger with the given level and default fields
func NewLogger(level string, defaultFields map[string]any) (Logger, error) {
	cfg := zap.NewProductionConfig()
	var opts = []zap.Option{
		zap.WithCaller(true),
		zap.AddCallerSkip(1),
	}
	for k, v := range defaultFields {
		opts = append(opts, zap.Fields(zap.Any(k, v)))
	}
	lvl := zap.NewAtomicLevelAt(getLevel(level))
	cfg.Level = lvl
	logger, err := cfg.Build(opts...)
	if err != nil {
		return nil, err
	}
	return &defaultLogger{logger: logger}, nil
}

func (d defaultLogger) Error(ctx context.Context, msg string, err error, tags map[string]interface{}) {
	var fields = []zap.Field{zap.Error(err)}
	for k, v := range tags {
		fields = append(fields, zap.Any(k, v))
	}
	d.logger.Error(msg, fields...)
}

func (d defaultLogger) Info(ctx context.Context, msg string, tags map[string]interface{}) {
	var fields []zap.Field
	for k, v := range tags {
		fields = append(fields, zap.Any(k, v))
	}
	d.logger.Info(msg, fields...)
}

func (d defaultLogger) Debug(ctx context.Context, msg string, tags map[string]interface{}) {
	var fields []zap.Field
	for k, v := range tags {
		fields = append(fields, zap.Any(k, v))
	}
	d.logger.Debug(msg, fields...)
}

func (d defaultLogger) Warn(ctx context.Context, msg string, tags map[string]interface{}) {
	var fields []zap.Field
	for k, v := range tags {
		fields = append(fields, zap.Any(k, v))
	}
	d.logger.Warn(msg, fields...)
}

func getLevel(level string) zapcore.Level {
	levelMap := map[string]zapcore.Level{
		"error":   zap.ErrorLevel,
		"warn":    zap.WarnLevel,
		"warning": zap.WarnLevel,
		"info":    zap.InfoLevel,
		"debug":   zap.DebugLevel,
	}
	l, ok := levelMap[strings.ToLower(level)]
	if !ok {
		return zap.InfoLevel
	}
	return l
}
