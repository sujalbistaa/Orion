package telemetry

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// LogFormat selects the log encoding.
type LogFormat string

const (
	// FormatJSON is the default for anything that ships logs somewhere.
	FormatJSON LogFormat = "json"
	// FormatText is easier to read during local development.
	FormatText LogFormat = "text"
)

// NewLogger builds the process logger.
//
// Orion logs structurally everywhere: an operator correlating a node failure
// with the workloads it displaced needs to filter on node name, not to grep
// interpolated prose.
func NewLogger(level string, format LogFormat, component string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: parseLevel(level),
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Errors carry their message rather than a Go-syntax dump, so a log
			// aggregator can group on them.
			if a.Value.Kind() == slog.KindAny {
				if err, ok := a.Value.Any().(error); ok {
					return slog.String(a.Key, err.Error())
				}
			}
			return a
		},
	}

	var handler slog.Handler
	if format == FormatText {
		handler = slog.NewTextHandler(os.Stderr, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(handler).With("service", component)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// requestIDKey carries a correlation ID through a request's context.
type requestIDKey struct{}

// WithRequestID attaches a correlation ID to a context.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID returns the correlation ID, if any.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// LoggerFrom returns a logger annotated with the request ID, so every line
// emitted while serving a request can be tied back to it.
func LoggerFrom(ctx context.Context, base *slog.Logger) *slog.Logger {
	if id := RequestID(ctx); id != "" {
		return base.With("requestId", id)
	}
	return base
}
