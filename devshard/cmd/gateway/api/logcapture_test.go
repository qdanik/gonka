package api

import (
	"sync"
	"testing"

	"devshard/logging"
)

type logEntry struct {
	level  string
	msg    string
	fields []any
}

// logRecorder collects log lines a test asserts on. The gateway's logger is a package global, so a
// test that installs one cannot run in parallel with another that reads it.
type logRecorder struct {
	mu      sync.Mutex
	entries []logEntry
}

func (r *logRecorder) record(level, msg string, fields []any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, logEntry{level: level, msg: msg, fields: fields})
}

func (r *logRecorder) Info(msg string, fields ...any)  { r.record("info", msg, fields) }
func (r *logRecorder) Warn(msg string, fields ...any)  { r.record("warn", msg, fields) }
func (r *logRecorder) Error(msg string, fields ...any) { r.record("error", msg, fields) }
func (r *logRecorder) Debug(msg string, fields ...any) { r.record("debug", msg, fields) }

func (r *logRecorder) all() []logEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]logEntry(nil), r.entries...)
}

func (r *logRecorder) find(msg string) (logEntry, bool) {
	for _, entry := range r.all() {
		if entry.msg == msg {
			return entry, true
		}
	}
	return logEntry{}, false
}

func captureLogging(t *testing.T) *logRecorder {
	t.Helper()
	recorder := &logRecorder{}
	logging.SetLogger(recorder)
	t.Cleanup(func() { logging.SetLogger(logging.NewSlogAdapter()) })
	return recorder
}

func logField(entry logEntry, key string) any {
	for i := 0; i+1 < len(entry.fields); i += 2 {
		if name, ok := entry.fields[i].(string); ok && name == key {
			return entry.fields[i+1]
		}
	}
	return nil
}
