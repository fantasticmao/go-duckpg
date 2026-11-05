package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
)

type directBackend struct {
	db *sql.DB
}

func startDirect(ctx context.Context, rows int) (backend, checksum, error) {
	db, err := openDuckDB()
	if err != nil {
		return nil, checksum{}, err
	}
	d := &directBackend{db: db}
	if err := seedBench(ctx, db, rows); err != nil {
		return failBackend(d, err)
	}
	s, err := d.session(ctx)
	if err != nil {
		return failBackend(d, err)
	}
	sum, err := readChecksum(ctx, s, rows)
	closeErr := s.close()
	if err != nil {
		return failBackend(d, fmt.Errorf("direct: %w", err))
	}
	if closeErr != nil {
		return failBackend(d, fmt.Errorf("direct: %w", closeErr))
	}
	return d, sum, nil
}

func (d *directBackend) name() string { return "direct" }

func (d *directBackend) session(ctx context.Context) (session, error) {
	return d.dial(ctx)
}

func (d *directBackend) dial(ctx context.Context) (session, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &sqlSession{conn: conn}, nil
}

func (d *directBackend) close() error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}
