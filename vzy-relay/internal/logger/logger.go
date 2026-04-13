package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

func New(level string) *slog.Logger {
	lvl := new(slog.LevelVar)
	switch strings.ToUpper(level) {
	case "DEBUG":
		lvl.Set(slog.LevelDebug)
	case "WARN":
		lvl.Set(slog.LevelWarn)
	case "ERROR":
		lvl.Set(slog.LevelError)
	default:
		lvl.Set(slog.LevelInfo)
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

type ctxKey struct{}

func With(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

func From(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	l, ok := ctx.Value(ctxKey{}).(*slog.Logger)
	if !ok || l == nil {
		return fallback
	}
	return l
}
