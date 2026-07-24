package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithRetry(t *testing.T) {
	testStore := openTestStore(t)
	testStore.retryBackoff = time.Millisecond

	t.Run("succeeds on first try", func(t *testing.T) {
		calls := 0
		err := testStore.WithRetry(context.Background(), func() error {
			calls++
			return nil
		})
		if err != nil {
			t.Fatalf("WithRetry() = %v, want nil", err)
		}
		if calls != 1 {
			t.Fatalf("fn called %d times, want 1", calls)
		}
	})

	t.Run("fails twice then succeeds", func(t *testing.T) {
		calls := 0
		err := testStore.WithRetry(context.Background(), func() error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("WithRetry() = %v, want nil", err)
		}
		if calls != 3 {
			t.Fatalf("fn called %d times, want 3", calls)
		}
	})

	t.Run("always fails returns last error after exhausting attempts", func(t *testing.T) {
		calls := 0
		sentinelErr := errors.New("permanent")
		err := testStore.WithRetry(context.Background(), func() error {
			calls++
			return sentinelErr
		})
		if !errors.Is(err, sentinelErr) {
			t.Fatalf("WithRetry() = %v, want %v", err, sentinelErr)
		}
		if calls != retryAttempts {
			t.Fatalf("fn called %d times, want exactly %d", calls, retryAttempts)
		}
	})

	t.Run("cancelled context aborts before exhausting attempts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		go func() {
			time.Sleep(testStore.retryBackoff / 2)
			cancel()
		}()
		err := testStore.WithRetry(ctx, func() error {
			calls++
			return errors.New("transient")
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WithRetry() = %v, want context.Canceled", err)
		}
		if calls >= retryAttempts {
			t.Fatalf("fn called %d times, want fewer than %d (must abort promptly, not exhaust attempts)", calls, retryAttempts)
		}
	})
}

func TestWithRetryAlreadyCancelledNeverCallsFn(t *testing.T) {
	testStore := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := testStore.WithRetry(ctx, func() error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WithRetry() = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("fn called %d times, want 0 (ctx was already cancelled)", calls)
	}
}
