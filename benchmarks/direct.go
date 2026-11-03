package main

import (
	"context"
	"database/sql"
	"fmt"
)

type directBackend struct {
	db *sql.DB
}

func newDirect() *directBackend {
	return &directBackend{}
}

func (d *directBackend) Name() string { return "direct" }

func (d *directBackend) Prepare(ctx context.Context, rows int) (Checksum, error) {
	db, err := openDuckDB()
	if err != nil {
		return Checksum{}, err
	}
	d.db = db
	if err := seedBench(ctx, db, rows); err != nil {
		return Checksum{}, err
	}
	session, err := d.Session(ctx)
	if err != nil {
		return Checksum{}, err
	}
	defer session.Close()
	sum, err := readChecksum(ctx, session, rows)
	if err != nil {
		return Checksum{}, fmt.Errorf("direct: %w", err)
	}
	return sum, nil
}

func (d *directBackend) Session(ctx context.Context) (Session, error) {
	return d.Dial(ctx)
}

func (d *directBackend) Dial(ctx context.Context) (Session, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &sqlSession{conn: conn}, nil
}

func (d *directBackend) Close() error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}
