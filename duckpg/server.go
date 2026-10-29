package duckpg

import (
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

// Startup initializes the PostgreSQL wire server and listens for incoming connections.
// Each client gets a dedicated DuckDB connection from the pool for the lifetime of the socket.
func Startup(duckdb *sql.DB, address string) error {
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

		go func(conn net.Conn) {
			defer conn.Close()

			sess, err := newSession(duckdb)
			if err != nil {
				errorLog.Printf("create session error: %s\n", err.Error())
				return
			}
			defer sess.close()

			pg := &pgWire{
				conn:    conn,
				backend: pgproto3.NewBackend(conn, conn),
				session: sess,
			}

			if err := pg.run(); err != nil {
				errorLog.Printf("pgwire server run error: %s\n", err.Error())
			}
			infoLog.Printf("connection closed, remote addr: %s\n", conn.RemoteAddr())
		}(conn)
	}
}

type pgWire struct {
	conn    net.Conn
	backend *pgproto3.Backend
	session *session
}

func (pg *pgWire) run() error {
	if err := pg.handleStartup(); err != nil {
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

func (pg *pgWire) handleStartup() error {
	for {
		msg, err := pg.backend.ReceiveStartupMessage()
		if err != nil {
			return fmt.Errorf("receive startup message error: %w", err)
		}

		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			buf, err := pg.handleSSLRequest()
			if err != nil {
				return err
			}
			if _, err = pg.conn.Write(buf); err != nil {
				return fmt.Errorf("write response buffer error: %w", err)
			}
			// SSL refused; continue reading the next startup packet.
			continue
		case *pgproto3.StartupMessage:
			buf, err := pg.handleStartupMessage(m)
			if err != nil {
				return fmt.Errorf("handle startup message failed: %w", err)
			}
			if _, err = pg.conn.Write(buf); err != nil {
				return fmt.Errorf("write response buffer error: %w", err)
			}
			return nil
		case *pgproto3.CancelRequest:
			// Cancel not implemented yet; close this short-lived connection.
			return nil
		case *pgproto3.GSSEncRequest:
			// Decline GSS encryption the same way as SSL.
			if _, err = pg.conn.Write([]byte{'N'}); err != nil {
				return fmt.Errorf("write response buffer error: %w", err)
			}
			continue
		default:
			return fmt.Errorf("unknown startup message: %#v", msg)
		}
	}
}
