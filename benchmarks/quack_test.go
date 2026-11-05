package benchmarks

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

const quackToken = "bench_token"

type quackBackend struct {
	server      *sql.DB
	client      *sql.DB
	serveConn   *sql.Conn
	serveCancel context.CancelFunc
	serveDone   <-chan struct{}
	uri         string
	next        atomic.Uint64
}

func startQuack(ctx context.Context, rows int) (backend, checksum, error) {
	server, err := openDuckDB()
	if err != nil {
		return nil, checksum{}, err
	}
	q := &quackBackend{server: server}
	client, err := openDuckDB()
	if err != nil {
		return failBackend(q, err)
	}
	q.client = client

	logger.Info("install quack extension", "side", "server")
	if err := installQuack(ctx, server); err != nil {
		return failBackend(q, err)
	}
	conn, uri, serveDone, cancel, err := serveQuack(ctx, server)
	if err != nil {
		return failBackend(q, err)
	}
	q.serveConn = conn
	q.uri = uri
	q.serveDone = serveDone
	q.serveCancel = cancel
	logger.Info("quack listening", "uri", uri)

	if err := seedBench(ctx, server, rows); err != nil {
		return failBackend(q, err)
	}

	logger.Info("install quack extension", "side", "client")
	if err := installQuack(ctx, client); err != nil {
		return failBackend(q, err)
	}

	s, err := q.session(ctx)
	if err != nil {
		return failBackend(q, err)
	}
	sum, err := readChecksum(ctx, s, rows)
	closeErr := s.close()
	if err != nil {
		return failBackend(q, fmt.Errorf("quack: %w", err))
	}
	if closeErr != nil {
		return failBackend(q, fmt.Errorf("quack: %w", closeErr))
	}
	return q, sum, nil
}

func (q *quackBackend) name() string { return "quack" }

func (q *quackBackend) session(ctx context.Context) (session, error) {
	// Each session is its own DuckDB instance. One instance treats a second
	// remote query as a replacement for the query already in flight.
	db, err := openDuckDB()
	if err != nil {
		return nil, err
	}
	if err := installQuack(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	attach := fmt.Sprintf("ATTACH '%s' AS remote (TOKEN '%s')", q.uri, quackToken)
	if err := execRetry(ctx, db, attach, 10); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("attach quack: %w", err)
	}
	return &quackSession{db: db, conn: conn}, nil
}

func (q *quackBackend) dial(ctx context.Context) (session, error) {
	conn, err := q.client.Conn(ctx)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("dial_%d", q.next.Add(1))
	attach := fmt.Sprintf("ATTACH '%s' AS %s (TOKEN '%s')", q.uri, name, quackToken)
	if _, err := conn.ExecContext(ctx, attach); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "CONNECT "+name); err != nil {
		_, _ = conn.ExecContext(context.Background(), "DETACH "+name)
		_ = conn.Close()
		return nil, err
	}
	return &quackDial{conn: conn, db: q.client, name: name}, nil
}

// quackSession sends each statement through the attached catalog's query macro.
// duckdb-go prepares every statement, and DuckDB does not register that prepare
// on a session that has run CONNECT, so timed SQL stays on the local connection
// and the macro executes it on the server.
type quackSession struct {
	db   *sql.DB
	conn *sql.Conn
}

func wrapRemote(query string) string {
	return "FROM remote.query($bench$" + query + "$bench$)"
}

func (s *quackSession) exec(ctx context.Context, query string) error {
	rows, err := s.conn.QueryContext(ctx, wrapRemote(query))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

func (s *quackSession) query(ctx context.Context, query string) (resultRows, error) {
	rows, err := s.conn.QueryContext(ctx, wrapRemote(query))
	if err != nil {
		return nil, err
	}
	return sqlRowSet{rows}, nil
}

func (s *quackSession) close() error {
	err := s.conn.Close()
	if closeErr := s.db.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

// quackDial is an ATTACH and CONNECT session. close drops the connection
// instead of preparing DISCONNECT, which fails once CONNECT is active.
type quackDial struct {
	conn *sql.Conn
	db   *sql.DB
	name string
}

func (s *quackDial) exec(context.Context, string) error {
	return fmt.Errorf("quack dial session cannot execute SQL")
}

func (s *quackDial) query(context.Context, string) (resultRows, error) {
	return nil, fmt.Errorf("quack dial session cannot execute SQL")
}

func (s *quackDial) close() error {
	err := s.conn.Raw(func(any) error { return driver.ErrBadConn })
	if err != nil && !errors.Is(err, driver.ErrBadConn) {
		return err
	}
	if _, err := s.db.ExecContext(context.Background(), "DETACH "+s.name); err != nil {
		return err
	}
	return nil
}

func (q *quackBackend) close() error {
	if q.serveCancel != nil {
		q.serveCancel()
	}
	if q.serveDone != nil {
		select {
		case <-q.serveDone:
		case <-time.After(2 * time.Second):
		}
	}
	var err error
	if q.serveConn != nil {
		err = q.serveConn.Close()
	}
	if q.client != nil {
		if closeErr := q.client.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if q.server != nil {
		if closeErr := q.server.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func serveQuack(ctx context.Context, db *sql.DB) (
	conn *sql.Conn,
	uri string,
	done <-chan struct{},
	cancel context.CancelFunc,
	err error,
) {
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		conn, err = db.Conn(ctx)
		if err != nil {
			return nil, "", nil, nil, err
		}
		port, portErr := freePort()
		if portErr != nil {
			_ = conn.Close()
			return nil, "", nil, nil, portErr
		}
		uri = fmt.Sprintf("quack:127.0.0.1:%d", port)
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		call := fmt.Sprintf("CALL quack_serve('%s', token = '%s')", uri, quackToken)
		serveCtx, serveCancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		doneCh := make(chan struct{})
		go func(conn *sql.Conn) {
			defer close(doneCh)
			rows, queryErr := conn.QueryContext(serveCtx, call)
			if queryErr != nil {
				errCh <- queryErr
				return
			}
			defer rows.Close()
			for rows.Next() {
			}
			errCh <- rows.Err()
		}(conn)
		if waitErr := waitReady(addr, errCh, 15*time.Second); waitErr != nil {
			serveCancel()
			<-doneCh
			_ = conn.Close()
			last = waitErr
			continue
		}
		return conn, uri, doneCh, serveCancel, nil
	}
	return nil, "", nil, nil, fmt.Errorf("quack_serve: %w", last)
}

func installQuack(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `INSTALL quack`); err != nil {
		return fmt.Errorf("install quack extension (network access to the DuckDB "+
			"extension repository is required on first run): %w", err)
	}
	if _, err := db.ExecContext(ctx, `LOAD quack`); err != nil {
		return fmt.Errorf("load quack extension: %w", err)
	}
	return nil
}

func execRetry(ctx context.Context, db *sql.DB, query string, attempts int) error {
	var last error
	for i := 0; i < attempts; i++ {
		if _, err := db.ExecContext(ctx, query); err == nil {
			return nil
		} else {
			last = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return last
}
