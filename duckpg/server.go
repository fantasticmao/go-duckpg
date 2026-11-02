package duckpg

import (
	"crypto/subtle"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgproto3"
)

var infoLog = log.New(os.Stdout, "[go-duckpg] ", log.Ldate|log.Ltime)
var errorLog = log.New(os.Stderr, "[go-duckpg] ", log.Lshortfile)

// Config controls optional startup behavior.
// A nil Users accepts every client. A non-nil Users requires the startup
// user to be a key and the cleartext password to match that entry.
// A nil TLS refuses SSLRequest with 'N'; a non-nil TLS completes the
// handshake after SSLRequest.
type Config struct {
	Users map[string]string
	TLS   *tls.Config
}

// Listen initializes the PostgreSQL wire server and listens for incoming connections.
// Each client gets a dedicated DuckDB connection from the pool for the lifetime of the socket.
// A non-nil Users map is copied before listening, so later changes to the caller's map have no effect.
func Listen(duckdb *sql.DB, address string, cfg Config) error {
	if cfg.Users != nil {
		users := make(map[string]string, len(cfg.Users))
		for user, password := range cfg.Users {
			users[user] = password
		}
		cfg.Users = users
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		infoLog.Printf("connection accepted, remote addr: %s\n", conn.RemoteAddr())
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
	user := ""
	if pg.session != nil {
		user = pg.session.user
	}
	if err != nil {
		errorLog.Printf("pgwire server run error, user: %s, error: %s\n", user, err.Error())
	}
	infoLog.Printf("connection closed, user: %s, remote addr: %s\n", user, conn.RemoteAddr())
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
			_ = pg.session.close()
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
			if err := pg.openSession(m); err != nil {
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

func (pg *pgWire) openSession(msg *pgproto3.StartupMessage) error {
	sess, err := newSession(pg.db)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	pg.session = sess
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
	infoLog.Printf("authentication succeeded, user: %s, remote addr: %s\n", user, pg.conn.RemoteAddr())
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
	want, found := pg.cfg.Users[user]
	if !ok || !found || !passwordMatch(passwordMsg.Password, want) {
		return pg.authFailed()
	}
	return nil
}

func (pg *pgWire) authFailed() error {
	buf, err := encodeErrorResponseStatus(nil, "FATAL", "28P01", "password authentication failed")
	if err != nil {
		return err
	}
	if _, werr := pg.conn.Write(buf); werr != nil {
		return fmt.Errorf("write response buffer error: %w", werr)
	}
	return fmt.Errorf("password authentication failed")
}

func passwordMatch(got, want string) bool {
	if len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
