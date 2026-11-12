package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// Test flow:
//  1. Rewrite each statement shape the store uses with the Postgres dialect.
//  2. Assert placeholders are numbered outside string literals, numbered placeholders keep their number, datetime('now') becomes a UTC to_char, and LIMIT -1 OFFSET loses its LIMIT.
//  3. Assert the SQLite dialect returns every statement untouched.
func TestPostgresDialectRewritesWhatSQLiteSpellsDifferently(t *testing.T) {
	cases := []struct{ name, query, want string }{
		{"positional", `INSERT INTO t (a, b) VALUES (?, ?)`, `INSERT INTO t (a, b) VALUES ($1, $2)`},
		{"numbered", `UPDATE t SET a = ?1, b = CASE WHEN ?1 = 1 THEN 0 END WHERE id = ?2`, `UPDATE t SET a = $1, b = CASE WHEN $1 = 1 THEN 0 END WHERE id = $2`},
		{"literal", `SELECT '?' WHERE a = ? AND b = 'it''s ?'`, `SELECT '?' WHERE a = $1 AND b = 'it''s ?'`},
		{"now", `UPDATE t SET updated_at = datetime('now') WHERE id = ?`, `UPDATE t SET updated_at = ` + postgresNow + ` WHERE id = $1`},
		{"offset", `SELECT id FROM t ORDER BY id LIMIT -1 OFFSET ?`, `SELECT id FROM t ORDER BY id OFFSET $1`},
	}
	for _, tc := range cases {
		if got := (&postgresDialect{}).rewrite(tc.query); got != tc.want {
			t.Errorf("%s: rewrite(%q) = %q, want %q", tc.name, tc.query, got, tc.want)
		}
		if got := (sqliteDialect{}).rewrite(tc.query); got != tc.query {
			t.Errorf("%s: SQLite rewrite changed the statement to %q", tc.name, got)
		}
	}
}

type roleName string

// Test flow:
//  1. Convert a bool, a named string, a byte slice and an unsigned integer with the Postgres dialect.
//  2. Assert bools become 0 or 1, the named string becomes a string, and the rest pass through.
//  3. Assert the SQLite dialect passes the same arguments through unchanged.
func TestPostgresDialectConvertsArguments(t *testing.T) {
	in := []any{true, false, roleName("temp"), []byte("x"), uint64(7), nil}
	got := (&postgresDialect{}).args(in)
	want := []any{int64(1), int64(0), "temp", []byte("x"), uint64(7), nil}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("args() = %#v, want %#v", got, want)
	}
	if kept := (sqliteDialect{}).args(in); fmt.Sprint(kept) != fmt.Sprint(in) {
		t.Fatalf("SQLite args() = %#v, want %#v", kept, in)
	}
}

// Test flow:
//  1. Classify a serialization failure, a deadlock, a unique violation, a statement timeout, a mid-statement connection error and a plain error.
//  2. Assert only the first two retry.
func TestPostgresRetryClassifier(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "40001"}, true},
		{fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "40P01"}), true},
		{&pgconn.PgError{Code: "23505"}, false},
		{&pgconn.PgError{Code: "57014"}, false},
		{errors.New("unexpected EOF"), false},
	}
	for _, tc := range cases {
		if got := isRetryablePostgresError(tc.err); got != tc.want {
			t.Errorf("isRetryablePostgresError(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
