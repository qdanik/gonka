package store

import (
	"context"
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// dialect holds what differs between the two backends; every statement in this package is written once, in SQLite's spelling.
type dialect interface {
	rewrite(query string) string
	args(args []any) []any
	retryable(err error) bool
	// walCheckpointOnCommit reports whether a durable write needs an explicit checkpoint to be in the main database file.
	walCheckpointOnCommit() bool
}

// database applies the dialect to every statement, so no call site knows which backend it runs on.
type database struct {
	*sql.DB
	dialect dialect
}

func (d *database) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.DB.ExecContext(ctx, d.dialect.rewrite(query), d.dialect.args(args)...)
}

func (d *database) Exec(query string, args ...any) (sql.Result, error) {
	return d.ExecContext(context.Background(), query, args...)
}

func (d *database) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.DB.QueryContext(ctx, d.dialect.rewrite(query), d.dialect.args(args)...)
}

func (d *database) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.DB.QueryRowContext(ctx, d.dialect.rewrite(query), d.dialect.args(args)...)
}

func (d *database) QueryRow(query string, args ...any) *sql.Row {
	return d.QueryRowContext(context.Background(), query, args...)
}

type sqliteDialect struct{}

func (sqliteDialect) rewrite(query string) string { return query }
func (sqliteDialect) args(args []any) []any       { return args }
func (sqliteDialect) walCheckpointOnCommit() bool { return true }
func (sqliteDialect) retryable(err error) bool    { return isLockedError(err) }

const postgresNow = `to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')`

type postgresDialect struct {
	rewritten sync.Map
}

func (p *postgresDialect) walCheckpointOnCommit() bool { return false }
func (p *postgresDialect) retryable(err error) bool    { return isRetryablePostgresError(err) }

func (p *postgresDialect) rewrite(query string) string {
	if cached, ok := p.rewritten.Load(query); ok {
		return cached.(string)
	}
	statement := strings.ReplaceAll(query, `datetime('now')`, postgresNow)
	statement = strings.ReplaceAll(statement, "LIMIT -1 OFFSET", "OFFSET")
	rewritten := rebind(statement)
	p.rewritten.Store(query, rewritten)
	return rewritten
}

// rebind turns `?` and `?N` into `$n` and `$N` outside string literals; a statement uses one form or the other.
func rebind(query string) string {
	var out strings.Builder
	out.Grow(len(query) + 8)
	inLiteral := false
	position := 0
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '\'':
			inLiteral = !inLiteral
			out.WriteByte(c)
		case c == '?' && !inLiteral:
			out.WriteByte('$')
			digits := i + 1
			for digits < len(query) && query[digits] >= '0' && query[digits] <= '9' {
				digits++
			}
			if digits > i+1 {
				out.WriteString(query[i+1 : digits])
				i = digits - 1
				continue
			}
			position++
			out.WriteString(strconv.Itoa(position))
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

// args gives Postgres what the SQLite driver coerces on its own: flag columns are BIGINT, and a named string type is a string.
func (p *postgresDialect) args(args []any) []any {
	converted := make([]any, len(args))
	for i, arg := range args {
		converted[i] = postgresArg(arg)
	}
	return converted
}

func postgresArg(arg any) any {
	switch value := arg.(type) {
	case nil, string, []byte, int, int64, uint64:
		return value
	case bool:
		if value {
			return int64(1)
		}
		return int64(0)
	}
	if reflected := reflect.ValueOf(arg); reflected.Kind() == reflect.String {
		return reflected.String()
	}
	return arg
}
