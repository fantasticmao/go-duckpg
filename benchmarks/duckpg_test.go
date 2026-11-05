package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/fantasticmao/go-duckpg/duckpg"
	"github.com/jackc/pgx/v5"
)

type duckpgBackend struct {
	db   *sql.DB
	addr string
}

func startDuckpg(ctx context.Context, rows int) (backend, checksum, error) {
	db, err := openDuckDB()
	if err != nil {
		return nil, checksum{}, err
	}
	d := &duckpgBackend{db: db}
	if err := seedBench(ctx, db, rows); err != nil {
		return failBackend(d, err)
	}
	addr, err := listenDuckpg(db)
	if err != nil {
		return failBackend(d, err)
	}
	d.addr = addr
	s, err := d.session(ctx)
	if err != nil {
		return failBackend(d, err)
	}
	sum, err := readChecksum(ctx, s, rows)
	closeErr := s.close()
	if err != nil {
		return failBackend(d, fmt.Errorf("duckpg: %w", err))
	}
	if closeErr != nil {
		return failBackend(d, fmt.Errorf("duckpg: %w", closeErr))
	}
	return d, sum, nil
}

func (d *duckpgBackend) name() string { return "duckpg" }

func (d *duckpgBackend) session(ctx context.Context) (session, error) {
	return d.dial(ctx)
}

func (d *duckpgBackend) dial(ctx context.Context) (session, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://duckpg@%s/duckpg?sslmode=disable", d.addr))
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &pgxSession{conn: conn}, nil
}

func (d *duckpgBackend) close() error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}

func listenDuckpg(db *sql.DB) (string, error) {
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		port, err := freePort()
		if err != nil {
			return "", err
		}
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		errCh := make(chan error, 1)
		go func() {
			errCh <- duckpg.Listen(db, addr, duckpg.Config{})
		}()
		if err := waitDuckpg(addr, errCh); err != nil {
			last = err
			continue
		}
		logger.Info("duckpg listening", "addr", addr)
		return addr, nil
	}
	return "", fmt.Errorf("duckpg listen: %w", last)
}

func waitDuckpg(addr string, errCh <-chan error) error {
	deadline := time.Now().Add(3 * time.Second)
	var last error
	for {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://duckpg@%s/duckpg?sslmode=disable", addr))
		if err != nil {
			cancel()
			return err
		}
		cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
		conn, err := pgx.ConnectConfig(ctx, cfg)
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return nil
		}
		last = err
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %s: %w", addr, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type pgxSession struct {
	conn *pgx.Conn
}

func (s *pgxSession) exec(ctx context.Context, query string) error {
	_, err := s.conn.Exec(ctx, query)
	return err
}

func (s *pgxSession) query(ctx context.Context, query string) (resultRows, error) {
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgxRowSet{Rows: rows}, nil
}

func (s *pgxSession) close() error {
	return s.conn.Close(context.Background())
}

type pgxRowSet struct {
	pgx.Rows
}

func (r pgxRowSet) Close() { r.Rows.Close() }
