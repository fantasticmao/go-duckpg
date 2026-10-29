package duckpg

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/zeebo/assert"
)

func TestCleartextPassword(t *testing.T) {
	addr, cleanup := startConfiguredServer(t, Config{Password: "secret"})
	defer cleanup()

	ctx := context.Background()
	ok := connectPgxPassword(t, addr, "secret", "disable", nil)
	var n int
	err := ok.QueryRow(ctx, `SELECT 1`).Scan(&n)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	_, err = connectPgxPasswordErr(t, addr, "wrong", "disable", nil)
	assert.Error(t, err)

	_, err = connectPgxPasswordErr(t, addr, "", "disable", nil)
	assert.Error(t, err)
}

func TestTLSHandshake(t *testing.T) {
	serverTLS, pool := testServerTLS(t)
	addr, cleanup := startConfiguredServer(t, Config{TLS: serverTLS})
	defer cleanup()

	ctx := context.Background()
	conn := connectPgxPassword(t, addr, "", "require", pool)
	var n int
	err := conn.QueryRow(ctx, `SELECT 2`).Scan(&n)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	// Plaintext clients still work when TLS is optional.
	plain := connectPgxPassword(t, addr, "", "disable", nil)
	err = plain.QueryRow(ctx, `SELECT 3`).Scan(&n)
	assert.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestSSLRequiredWithoutTLS(t *testing.T) {
	addr, cleanup := startConfiguredServer(t, Config{})
	defer cleanup()

	_, err := connectPgxPasswordErr(t, addr, "", "require", nil)
	assert.Error(t, err)
}

func TestCancelRequest(t *testing.T) {
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

func TestCancelWrongSecret(t *testing.T) {
	s := &session{processID: 4242, secretKey: []byte{1, 2, 3, 4}}
	registerSession(s)
	defer unregisterSession(s)

	called := false
	s.mu.Lock()
	s.cancelQuery = func() { called = true }
	s.mu.Unlock()

	cancelBackend(4242, []byte{9, 9, 9, 9})
	assert.False(t, called)
	cancelBackend(1, []byte{1, 2, 3, 4})
	assert.False(t, called)
	cancelBackend(4242, []byte{1, 2, 3, 4})
	assert.True(t, called)
}

func connectPgxPassword(t *testing.T, addr, password, sslmode string, roots *x509.CertPool) *pgx.Conn {
	t.Helper()
	conn, err := connectPgxPasswordErr(t, addr, password, sslmode, roots)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func connectPgxPasswordErr(t *testing.T, addr, password, sslmode string, roots *x509.CertPool) (*pgx.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	user := "duckpg"
	if password != "" {
		user = "duckpg:" + password
	}
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://%s@%s/duckpg?sslmode=%s", user, addr, sslmode))
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
