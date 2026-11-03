package duckpg

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/zeebo/assert"
)

func startTestServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	return startConfiguredServer(t, Config{})
}

func startConfiguredServer(t *testing.T, cfg Config) (addr string, cleanup func()) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	assert.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serve(conn, db, cfg)
		}
	}()

	cleanup = func() {
		_ = ln.Close()
		<-done
		_ = db.Close()
	}
	return ln.Addr().String(), cleanup
}

func connectPgx(t *testing.T, addr string, simple bool) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://duckpg@%s/duckpg?sslmode=disable", addr))
	assert.NoError(t, err)
	if simple {
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func connectAuth(t *testing.T, addr, user, password, sslmode string, roots *x509.CertPool) *pgx.Conn {
	t.Helper()
	conn, err := connectAuthErr(t, addr, user, password, sslmode, roots)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func connectAuthErr(t *testing.T, addr, user, password, sslmode string, roots *x509.CertPool) (*pgx.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	userinfo := user
	if password != "" {
		userinfo = user + ":" + password
	}
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s@%s/duckpg?sslmode=%s", userinfo, addr, sslmode))
	assert.NoError(t, err)
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	if roots != nil {
		cfg.TLSConfig.RootCAs = roots
	}
	return pgx.ConnectConfig(ctx, cfg)
}

func testServerTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	assert.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	server := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}
	return server, pool
}

func TestListenInvalidAddress(t *testing.T) {
	err := Listen(nil, "127.0.0.1:not-a-port", Config{})
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "listen 127.0.0.1:not-a-port"))
}

func TestStartupServerVersion(t *testing.T) {
	addr, cleanup := startConfiguredServer(t, Config{ServerVersion: "16.4"})
	defer cleanup()

	conn := connectPgx(t, addr, true)
	ctx := context.Background()

	assert.Equal(t, "16.4", conn.PgConn().ParameterStatus("server_version"))

	var version string
	err := conn.QueryRow(ctx, `SELECT version()`).Scan(&version)
	assert.NoError(t, err)
	assert.Equal(t, "PostgreSQL 16.4 (DuckDB "+duckdbVersion()+")", version)

	var serverVersion string
	err = conn.QueryRow(ctx, `SELECT current_setting('server_version')`).Scan(&serverVersion)
	assert.NoError(t, err)
	assert.Equal(t, "16.4", serverVersion)
}

func TestAuthenticatePassword(t *testing.T) {
	addr, cleanup := startConfiguredServer(t, Config{
		Users: map[string]string{"duckpg": "secret"},
	})
	defer cleanup()

	t.Run("matching password", func(t *testing.T) {
		ctx := context.Background()
		ok := connectAuth(t, addr, "duckpg", "secret", "disable", nil)
		var n int
		err := ok.QueryRow(ctx, `SELECT 1`).Scan(&n)
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("wrong password", func(t *testing.T) {
		_, err := connectAuthErr(t, addr, "duckpg", "wrong", "disable", nil)
		assert.Error(t, err)
	})

	t.Run("empty password", func(t *testing.T) {
		_, err := connectAuthErr(t, addr, "duckpg", "", "disable", nil)
		assert.Error(t, err)
	})

	t.Run("unknown user", func(t *testing.T) {
		_, err := connectAuthErr(t, addr, "other", "secret", "disable", nil)
		assert.Error(t, err)
	})

	t.Run("empty user map", func(t *testing.T) {
		emptyAddr, emptyCleanup := startConfiguredServer(t, Config{Users: map[string]string{}})
		defer emptyCleanup()
		_, err := connectAuthErr(t, emptyAddr, "duckpg", "secret", "disable", nil)
		assert.Error(t, err)
	})
}

func TestStartupTLS(t *testing.T) {
	serverTLS, pool := testServerTLS(t)
	addr, cleanup := startConfiguredServer(t, Config{TLS: serverTLS})
	defer cleanup()

	ctx := context.Background()

	t.Run("require", func(t *testing.T) {
		conn := connectAuth(t, addr, "duckpg", "", "require", pool)
		var n int
		err := conn.QueryRow(ctx, `SELECT 2`).Scan(&n)
		assert.NoError(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("disable", func(t *testing.T) {
		// Plaintext clients still work when TLS is optional.
		plain := connectAuth(t, addr, "duckpg", "", "disable", nil)
		var n int
		err := plain.QueryRow(ctx, `SELECT 3`).Scan(&n)
		assert.NoError(t, err)
		assert.Equal(t, 3, n)
	})
}

func TestStartupSSLRefused(t *testing.T) {
	addr, cleanup := startConfiguredServer(t, Config{})
	defer cleanup()

	_, err := connectAuthErr(t, addr, "duckpg", "", "require", nil)
	assert.Error(t, err)
}

func TestCancelQuery(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	ctx := context.Background()
	conn := connectPgx(t, addr, true)

	errCh := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, `CREATE TABLE slow AS SELECT range::VARCHAR AS v, random() AS k FROM range(100000000) ORDER BY k`)
		errCh <- err
	}()

	time.Sleep(200 * time.Millisecond)
	err := conn.PgConn().CancelRequest(ctx)
	assert.NoError(t, err)

	select {
	case err = <-errCh:
		assert.Error(t, err)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("expected PgError, got %v", err)
		}
		assert.Equal(t, "57014", pgErr.Code)
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "interrupt") && !strings.Contains(msg, "cancel") {
			t.Fatalf("expected query cancellation, got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("query was not canceled")
	}

	var n int
	err = conn.QueryRow(ctx, `SELECT 1`).Scan(&n)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
}
