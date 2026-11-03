package duckpg

import (
	"crypto/subtle"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgproto3"
)

// defaultServerVersion is the PostgreSQL server_version used when Config.ServerVersion is empty.
const defaultServerVersion = "15.0"

// Config controls optional startup behavior.
// A nil Users accepts every client. A non-nil Users requires the startup
// user to be a key and the cleartext password to match that entry.
// A nil TLS refuses SSLRequest with 'N'; a non-nil TLS completes the
// handshake after SSLRequest.
// An empty ServerVersion advertises PostgreSQL 15.0. version() appends the
// linked DuckDB library version.
// A nil Logger writes text logs to stderr at Info level. Debug logs include SQL text.
type Config struct {
	Users         map[string]string
	TLS           *tls.Config
	ServerVersion string
	Logger        *slog.Logger
}

// serverVersion is the PostgreSQL server_version for this configuration.
// Whitespace-only values use the default.
func (cfg Config) serverVersion() string {
	if v := strings.TrimSpace(cfg.ServerVersion); v != "" {
		return v
	}
	return defaultServerVersion
}

// Listen initializes the PostgreSQL wire server and listens for incoming connections.
// Each client gets a dedicated DuckDB connection from the pool for the lifetime of the socket.
// A non-nil Users map is copied before listening, so later changes to the caller's map have no effect.
func Listen(duckdb *sql.DB, address string, cfg Config) error {
	if cfg.Users != nil {
		users := make(map[string]string, len(cfg.Users))
		maps.Copy(users, cfg.Users)
		cfg.Users = users
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", address, err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("listen %s: %w", address, err)
		}
		logAccepted(cfg, conn.RemoteAddr().String())
		go serve(conn, duckdb, cfg)
	}
}

func serve(conn net.Conn, db *sql.DB, cfg Config) {
	pg := &pgWire{
		conn:    conn,
		backend: pgproto3.NewBackend(conn, conn),
		db:      db,
		cfg:     cfg,
	}
	defer func() { _ = pg.conn.Close() }()

	err := pg.run()
	logRunError(pg, err)
	logWire(pg, slog.LevelInfo, "connection closed")
}

type pgWire struct {
	conn    net.Conn
	backend *pgproto3.Backend
	session *session
	db      *sql.DB
	cfg     Config
}

func (pg *pgWire) run() error {
	stop, err := pg.startup()
	if pg.session != nil {
		defer func() {
			unregisterSession(pg.session)
			if cerr := pg.session.close(); cerr != nil {
				logWire(pg, slog.LevelError, "session close failed", slog.String("error", cerr.Error()))
			}
		}()
	}
	if err != nil || stop {
		return err
	}

	for {
		msg, err := pg.backend.Receive()
		if err != nil {
			return fmt.Errorf("receive message error: %w", err)
		}

		buf, err := pg.dispatch(msg)
		if err != nil {
			return fmt.Errorf("handle message failed: %w", err)
		}
		if buf == nil {
			// Nil buffer means Terminate (or equivalent clean close).
			return nil
		}

		if _, err = pg.conn.Write(buf); err != nil {
			return fmt.Errorf("write response buffer error: %w", err)
		}
	}
}

// startup reads SSL, GSS, and Cancel packets, then authenticates.
// stop is true when the connection is a CancelRequest and should not enter the query loop.
func (pg *pgWire) startup() (stop bool, err error) {
	for {
		msg, err := pg.backend.ReceiveStartupMessage()
		if err != nil {
			return false, fmt.Errorf("receive startup message error: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			if err := pg.handleSSLRequest(); err != nil {
				return false, err
			}
			continue
		case *pgproto3.StartupMessage:
			if err := pg.acceptStartup(m); err != nil {
				return false, err
			}
			return false, nil
		case *pgproto3.CancelRequest:
			cancelBackend(m.ProcessID, m.SecretKey)
			return true, nil
		case *pgproto3.GSSEncRequest:
			// Decline GSS encryption the same way as an unconfigured SSL request.
			if _, err = pg.conn.Write([]byte{'N'}); err != nil {
				return false, fmt.Errorf("write response buffer error: %w", err)
			}
			continue
		default:
			return false, fmt.Errorf("unknown startup message: %#v", msg)
		}
	}
}

func (pg *pgWire) handleSSLRequest() error {
	if pg.cfg.TLS == nil {
		if _, err := pg.conn.Write([]byte{'N'}); err != nil {
			return fmt.Errorf("write response buffer error: %w", err)
		}
		return nil
	}
	if _, err := pg.conn.Write([]byte{'S'}); err != nil {
		return fmt.Errorf("write response buffer error: %w", err)
	}
	tlsConn := tls.Server(pg.conn, pg.cfg.TLS)
	if err := tlsConn.Handshake(); err != nil {
		return fmt.Errorf("tls handshake: %w", err)
	}
	pg.conn = tlsConn
	pg.backend = pgproto3.NewBackend(tlsConn, tlsConn)
	return nil
}

func (pg *pgWire) acceptStartup(msg *pgproto3.StartupMessage) error {
	sess, err := newSession(pg.db)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	pg.session = sess
	pg.session.settings["server_version"] = pg.cfg.serverVersion()
	registerSession(sess)

	user := ""
	if msg.Parameters != nil {
		user = msg.Parameters["user"]
	}
	pg.session.user = user
	if err := pg.authenticate(user); err != nil {
		return err
	}

	buf, err := pg.startupResponse()
	if err != nil {
		return fmt.Errorf("startup response failed: %w", err)
	}
	if _, err = pg.conn.Write(buf); err != nil {
		return fmt.Errorf("write response buffer error: %w", err)
	}
	logWire(pg, slog.LevelInfo, "authentication succeeded")
	return nil
}

func (pg *pgWire) authenticate(user string) error {
	if pg.cfg.Users == nil {
		return nil
	}

	buf, err := encodeAuthenticationCleartextPassword(nil)
	if err != nil {
		return err
	}
	if _, err = pg.conn.Write(buf); err != nil {
		return fmt.Errorf("write response buffer error: %w", err)
	}

	msg, err := pg.backend.Receive()
	if err != nil {
		return fmt.Errorf("receive password message error: %w", err)
	}
	passwordMsg, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return pg.fatalClientError("08P01", fmt.Sprintf("expected PasswordMessage, got %T", msg))
	}
	want, found := pg.cfg.Users[user]
	if !found || !passwordMatch(passwordMsg.Password, want) {
		return pg.fatalClientError("28P01", "password authentication failed")
	}
	return nil
}

func (pg *pgWire) startupResponse() ([]byte, error) {
	buf, err := encodeAuthenticationOk(nil)
	if err != nil {
		return nil, err
	}

	params := [][2]string{
		{"server_version", pg.cfg.serverVersion()},
		{"server_encoding", "UTF8"},
		{"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"TimeZone", "UTC"},
		{"integer_datetimes", "on"},
		{"standard_conforming_strings", "on"},
	}
	for _, p := range params {
		buf, err = encodeParameterStatus(buf, p[0], p[1])
		if err != nil {
			return nil, err
		}
	}

	buf, err = encodeBackendKeyData(buf, pg.session.processID, pg.session.secretKey)
	if err != nil {
		return nil, err
	}

	return encodeReadyForQuery(buf, pg.session.txStatus)
}

// fatalClientError writes a FATAL ErrorResponse, logs it at Warn, and returns
// an error that serve does not log again.
func (pg *pgWire) fatalClientError(code, message string) error {
	buf, err := encodeErrorResponseStatus(nil, "FATAL", code, message)
	if err != nil {
		return err
	}
	if _, werr := pg.conn.Write(buf); werr != nil {
		return fmt.Errorf("write response buffer error: %w", werr)
	}
	logWire(pg, slog.LevelWarn, message, slog.String("sqlstate", code))
	return loggedError{err: errors.New(message)}
}

func passwordMatch(got, want string) bool {
	if len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
