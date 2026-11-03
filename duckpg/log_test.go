package duckpg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func waitLog(t *testing.T, buf *lockedBuf, substr string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		text := buf.String()
		if strings.Contains(text, substr) {
			return text
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("log %q not found in:\n%s", substr, buf.String())
	return ""
}

func TestNilLoggerInfoLevel(t *testing.T) {
	logger := Config{}.logger()
	ctx := context.Background()
	assert.True(t, logger.Enabled(ctx, slog.LevelInfo))
	assert.True(t, logger.Enabled(ctx, slog.LevelWarn))
	assert.True(t, logger.Enabled(ctx, slog.LevelError))
	assert.False(t, logger.Enabled(ctx, slog.LevelDebug))
}

func TestIsClientDisconnect(t *testing.T) {
	assert.True(t, isClientDisconnect(io.EOF))
	assert.True(t, isClientDisconnect(fmt.Errorf("read: %w", io.ErrUnexpectedEOF)))
	assert.True(t, isClientDisconnect(fmt.Errorf("read: %w", net.ErrClosed)))
	assert.False(t, isClientDisconnect(errors.New("boom")))
}

func TestTruncateQuery(t *testing.T) {
	assert.Equal(t, "abc", truncateQuery("abc"))
	long := strings.Repeat("你", 201)
	assert.Equal(t, strings.Repeat("你", maxQueryLogRunes)+"...", truncateQuery(long))
}

func TestListenBadAddress(t *testing.T) {
	err := Listen(nil, "127.0.0.1:not-a-port", Config{})
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "listen 127.0.0.1:not-a-port"))
}

func TestAuthFailureLog(t *testing.T) {
	buf := &lockedBuf{}
	logger := slog.New(slog.NewTextHandler(buf, nil))
	addr, cleanup := startConfiguredServer(t, Config{
		Users:  map[string]string{"duckpg": "secret"},
		Logger: logger,
	})
	defer cleanup()

	_, err := connectAuthErr(t, addr, "duckpg", "wrong", "disable", nil)
	assert.Error(t, err)

	text := waitLog(t, buf, "password authentication failed")
	assert.True(t, strings.Contains(text, "level=WARN"))
	assert.True(t, strings.Contains(text, "remote=127.0.0.1:"))
	assert.True(t, strings.Contains(text, "user=duckpg"))
	assert.False(t, strings.Contains(text, "level=ERROR"))
}

func TestQueryHiddenAtInfo(t *testing.T) {
	buf := &lockedBuf{}
	logger := slog.New(slog.NewTextHandler(buf, nil))
	addr, cleanup := startConfiguredServer(t, Config{Logger: logger})
	defer cleanup()

	conn := connectPgx(t, addr, true)
	var got string
	err := conn.QueryRow(context.Background(), `SELECT 'duckpg_log_marker_8675309'`).Scan(&got)
	assert.NoError(t, err)
	assert.Equal(t, "duckpg_log_marker_8675309", got)
	assert.False(t, strings.Contains(buf.String(), "duckpg_log_marker_8675309"))
}

func TestFailedQueryTruncated(t *testing.T) {
	buf := &lockedBuf{}
	logger := slog.New(slog.NewTextHandler(buf, nil))
	addr, cleanup := startConfiguredServer(t, Config{Logger: logger})
	defer cleanup()

	conn := connectPgx(t, addr, true)
	body := strings.Repeat("m", 240)
	query := "SELECT '" + body + "' FROM duckpg_missing_log_table"
	_, err := conn.Exec(context.Background(), query)
	assert.Error(t, err)

	text := buf.String()
	assert.True(t, strings.Contains(text, "sql failed"))
	assert.False(t, strings.Contains(text, query))
	assert.True(t, strings.Contains(text, query[:maxQueryLogRunes]))
	assert.True(t, strings.Contains(text, "..."))
}

func TestExtendedQueryLoggedAtDebug(t *testing.T) {
	buf := &lockedBuf{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	addr, cleanup := startConfiguredServer(t, Config{Logger: logger})
	defer cleanup()

	conn := connectPgx(t, addr, false)
	var n int
	err := conn.QueryRow(context.Background(), `SELECT 24681357`).Scan(&n)
	assert.NoError(t, err)
	assert.Equal(t, 24681357, n)
	waitLog(t, buf, "24681357")
}

func TestClientDisconnectNotError(t *testing.T) {
	buf := &lockedBuf{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	addr, cleanup := startConfiguredServer(t, Config{Logger: logger})
	defer cleanup()

	conn := connectPgx(t, addr, true)
	assert.NoError(t, conn.PgConn().Conn().Close())

	text := waitLog(t, buf, "client disconnected")
	assert.True(t, strings.Contains(text, "level=DEBUG"))
	assert.False(t, strings.Contains(text, "level=ERROR"))
}
