package duckpg

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
)

const maxQueryLogRunes = 200

var defaultLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// loggedError is an error already reported to the client and the logger.
type loggedError struct{ err error }

func (e loggedError) Error() string { return e.err.Error() }

func (e loggedError) Unwrap() error { return e.err }

func (cfg Config) logger() *slog.Logger {
	if cfg.Logger != nil {
		return cfg.Logger
	}
	return defaultLogger
}

func logAccepted(cfg Config, remote string) {
	cfg.logger().LogAttrs(context.Background(), slog.LevelInfo, "connection accepted",
		slog.String("remote", remote),
		slog.String("user", ""),
	)
}

func logWire(pg *pgWire, level slog.Level, msg string, extra ...slog.Attr) {
	pg.cfg.logger().LogAttrs(context.Background(), level, msg, wireAttrs(pg, extra)...)
}

func wireAttrs(pg *pgWire, extra []slog.Attr) []slog.Attr {
	remote := ""
	if pg.conn != nil {
		if addr := pg.conn.RemoteAddr(); addr != nil {
			remote = addr.String()
		}
	}
	attrs := make([]slog.Attr, 0, 3+len(extra))
	attrs = append(attrs, slog.String("remote", remote))
	if pg.session != nil {
		attrs = append(attrs,
			slog.String("user", pg.session.user),
			slog.Uint64("pid", uint64(pg.session.processID)),
		)
	} else {
		attrs = append(attrs, slog.String("user", ""))
	}
	return append(attrs, extra...)
}

func logRunError(pg *pgWire, err error) {
	if err == nil {
		return
	}
	var logged loggedError
	if errors.As(err, &logged) {
		return
	}
	if isClientDisconnect(err) {
		logWire(pg, slog.LevelDebug, "client disconnected", slog.String("error", err.Error()))
		return
	}
	logWire(pg, slog.LevelError, "connection failed", slog.String("error", err.Error()))
}

func logSQL(pg *pgWire, query string) {
	logWire(pg, slog.LevelDebug, "sql query", slog.String("query", query))
}

func logSQLFailed(pg *pgWire, query string, err error) {
	logWire(pg, slog.LevelWarn, "sql failed",
		slog.String("query", truncateQuery(query)),
		slog.String("error", err.Error()),
	)
}

func isClientDisconnect(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}

func truncateQuery(query string) string {
	n := 0
	for i := range query {
		if n == maxQueryLogRunes {
			return query[:i] + "..."
		}
		n++
	}
	return query
}
