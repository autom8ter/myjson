package openapi

import (
	"context"

	"github.com/autom8ter/myjson"
	"go.uber.org/zap"
)

type Logger interface {
	Info(ctx context.Context, msg string, tags map[string]any)
	Error(ctx context.Context, msg string, tags map[string]any)
	Debug(ctx context.Context, msg string, tags map[string]any)
	Warn(ctx context.Context, msg string, tags map[string]any)
	Sync(ctx context.Context)
	WithTags(ctx context.Context, tags map[string]any) Logger
}

type zapLogger struct {
	logger *zap.Logger
}

func getTags(ctx context.Context, tags map[string]any) []zap.Field {
	var fields []zap.Field
	if tags != nil {
		for k, v := range tags {
			fields = append(fields, zap.Any(k, v))
		}
	}

	md, ok := myjson.GetMetadata(ctx)
	if ok {
		for k, v := range md.Map() {
			fields = append(fields, zap.Any(k, v))
		}
	}
	return fields
}

func (z zapLogger) Info(ctx context.Context, msg string, tags map[string]any) {
	z.logger.Info(msg, getTags(ctx, tags)...)
}

func (z zapLogger) Error(ctx context.Context, msg string, tags map[string]any) {
	z.logger.Error(msg, getTags(ctx, tags)...)
}

func (z zapLogger) Debug(ctx context.Context, msg string, tags map[string]any) {
	z.logger.Debug(msg, getTags(ctx, tags)...)
}

func (z zapLogger) Warn(ctx context.Context, msg string, tags map[string]any) {
	z.logger.Warn(msg, getTags(ctx, tags)...)
}

func (z zapLogger) Sync(ctx context.Context) {
	z.logger.Sync()
}

func (z zapLogger) WithTags(ctx context.Context, tags map[string]any) Logger {
	return zapLogger{
		logger: z.logger.With(getTags(ctx, tags)...),
	}
}
