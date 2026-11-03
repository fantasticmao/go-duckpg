package duckpg

import (
	"errors"
	"testing"

	"github.com/zeebo/assert"
)

func TestCommandTag(t *testing.T) {
	cases := []struct {
		name         string
		query        string
		hasResultSet bool
		rowCnt       int64
		rowsAffected int64
		want         string
	}{
		{"select", "SELECT 1", true, 3, -1, "SELECT 3"},
		{"insert", "INSERT INTO t VALUES (1)", false, 0, 2, "INSERT 0 2"},
		{"insert returning", "INSERT INTO t VALUES (1)", true, 2, 9, "INSERT 0 2"},
		{"update", "UPDATE t SET i = 1", false, 0, 4, "UPDATE 4"},
		{"delete", "DELETE FROM t", false, 0, 5, "DELETE 5"},
		{"begin", "BEGIN", false, 0, 0, "BEGIN"},
		{"create", "CREATE TABLE t(i INT)", false, 0, 0, "CREATE"},
		{"rollback", "ROLLBACK", false, 0, 0, "ROLLBACK"},
		{"leading comment", "-- comment\nSELECT 1", true, 1, -1, "SELECT 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := commandTag(tc.query, tc.hasResultSet, tc.rowCnt, tc.rowsAffected)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestUpdateTxStatus(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name   string
		status byte
		query  string
		err    error
		want   byte
	}{
		{"begin", 'I', "BEGIN", nil, 'T'},
		{"commit", 'T', "COMMIT", nil, 'I'},
		{"commit error", 'T', "COMMIT", boom, 'E'},
		{"error in transaction", 'T', "SELECT 1", boom, 'E'},
		{"rollback", 'E', "ROLLBACK", nil, 'I'},
		{"error while idle", 'I', "SELECT 1", boom, 'I'},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := updateTxStatus(tc.status, tc.query, tc.err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCancelBackendSecret(t *testing.T) {
	s := &session{processID: 4242, secretKey: []byte{1, 2, 3, 4}}
	registerSession(s)
	defer unregisterSession(s)

	called := false
	s.mu.Lock()
	s.cancelQuery = func() { called = true }
	s.mu.Unlock()

	t.Run("wrong secret", func(t *testing.T) {
		cancelBackend(4242, []byte{9, 9, 9, 9})
		assert.False(t, called)
	})

	t.Run("unknown process", func(t *testing.T) {
		cancelBackend(1, []byte{1, 2, 3, 4})
		assert.False(t, called)
	})

	t.Run("matching secret", func(t *testing.T) {
		cancelBackend(4242, []byte{1, 2, 3, 4})
		assert.True(t, called)
	})
}
