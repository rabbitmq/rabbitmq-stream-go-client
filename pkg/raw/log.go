package raw

import (
	"context"
	"golang.org/x/exp/slog"
)

// The provided key must be comparable and should not be of type string or any
// other built-in type to avoid collisions between packages using context.
//
// https://pkg.go.dev/context#WithValue
type contextKey struct{}

func LoggerFromCtxOrDiscard(ctx context.Context) *slog.Logger {
	v := ctx.Value(contextKey{})
	if v == nil {
		return slog.New(discardHandler{})
	}
	l := v.(slog.Logger)
	return &l
}

func NewContextWithLogger(ctx context.Context, logger slog.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

func NewDiscardLogger() *slog.Logger {
	return slog.New(discardHandler{})
}

type discardHandler struct{}

func (d discardHandler) Enabled(context.Context, slog.Level) bool {
	return false
}

func (d discardHandler) Handle(context.Context, slog.Record) error {
	// do nothing
	return nil
}

func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler {
	return d
}

func (d discardHandler) WithGroup(string) slog.Handler {
	return d
}
