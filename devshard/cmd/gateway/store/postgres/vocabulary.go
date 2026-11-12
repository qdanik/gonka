package postgres

import (
	"errors"
	"time"
)

const (
	bootTimeout            = 30 * time.Second
	markerTable            = "gateway_database_marker"
	bootLockName           = "gateway_store_boot"
	nowExpression          = `to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')`
	synchronousCommitParam = "synchronous_commit"
	synchronousCommitOn    = "on"
	fsyncSetting           = "fsync"
	settingOff             = "off"
	serializationFailure   = "40001"
	deadlockDetected       = "40P01"
	flagSet                = int64(1)
	flagClear              = int64(0)
	loopbackHostName       = "localhost"
	unixSocketHostPrefix   = "/"
)

var (
	// ErrForeignDatabase reports a Postgres database this gateway did not create: it holds tables and no marker.
	ErrForeignDatabase = errors.New("postgres database holds tables this gateway did not create; point the gateway at a dedicated, empty database")
	// ErrEmptyDatabaseRefused reports an empty database that Options.RefuseEmptyDatabase kept Open from initialising.
	ErrEmptyDatabaseRefused = errors.New("postgres database is empty")
	// ErrPlaintextRemoteHost reports a connection that would send the store's traffic, credentials included, to a remote host without TLS.
	ErrPlaintextRemoteHost = errors.New("postgres connection to a remote host would not use TLS; set sslmode to require, verify-ca or verify-full")
	// ErrDurabilityOff reports a server that does not flush the write-ahead log before it answers a commit.
	ErrDurabilityOff = errors.New("postgres server does not flush commits to disk; a saved commitment would not survive a crash")
)
