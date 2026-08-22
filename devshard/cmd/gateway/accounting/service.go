package accounting

import (
	"context"
	"errors"
	"time"

	"devshard/logging"
)

const (
	pruneInterval           = time.Minute
	defaultSnapshotInterval = 5 * time.Minute
)

type Service struct {
	Book *Book

	store           *Store
	retentionEpochs uint64
	currentEpoch    CurrentEpochFunc
	cancel          context.CancelFunc
	stopped         chan struct{}
}

type CurrentEpochFunc func(context.Context) (uint64, error)

type Settings struct {
	Store            *Store
	SnapshotInterval time.Duration
	RetentionEpochs  uint64
	CurrentEpoch     CurrentEpochFunc
	Now              func() time.Time
}

// NewService restores any stored ledger and starts the snapshot and retention loops. A snapshot that
// cannot be read is reported and the ledger starts empty: refusing to start over an unreadable
// observability file would trade a gateway for a graph.
func NewService(settings Settings) (*Service, error) {
	if settings.SnapshotInterval <= 0 {
		settings.SnapshotInterval = defaultSnapshotInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{
		Book:            NewBook(settings.Now),
		store:           settings.Store,
		retentionEpochs: settings.RetentionEpochs,
		currentEpoch:    settings.CurrentEpoch,
		cancel:          cancel,
		stopped:         make(chan struct{}),
	}
	restoreErr := service.restore()
	go service.run(ctx, settings.SnapshotInterval)
	return service, restoreErr
}

func (s *Service) restore() error {
	if s.store == nil {
		return nil
	}
	snapshot, err := s.store.Load(context.Background())
	if err != nil {
		return err
	}
	return s.Book.Restore(snapshot)
}

func (s *Service) Flush() error {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.Save(context.Background(), s.Book)
}

func (s *Service) run(ctx context.Context, snapshotInterval time.Duration) {
	defer close(s.stopped)
	pruning := time.NewTicker(pruneInterval)
	defer pruning.Stop()
	snapshots := time.NewTicker(snapshotInterval)
	defer snapshots.Stop()
	for {
		select {
		case <-pruning.C:
			s.prune(ctx)
		case <-snapshots.C:
			if err := s.Flush(); err != nil {
				logging.Error("nonce accounting snapshot failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) prune(ctx context.Context) {
	if s.retentionEpochs == 0 || s.currentEpoch == nil {
		return
	}
	epoch, err := s.currentEpoch(ctx)
	if err != nil || epoch <= s.retentionEpochs {
		return
	}
	s.Book.PruneBefore(epoch - s.retentionEpochs)
}

// Reset empties the ledger and writes the empty one out, so a restart cannot restore what an
// operator just cleared.
func (s *Service) Reset() error {
	if s == nil || s.Book == nil {
		return nil
	}
	s.Book.Reset()
	return s.Flush()
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.cancel()
	<-s.stopped
	return errors.Join(s.Flush(), s.store.Close())
}

func (b *Book) PruneBefore(epoch uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for escrowID, escrow := range b.escrows {
		if escrow.retired && escrow.metadata.CreationEpoch < epoch {
			delete(b.escrows, escrowID)
		}
	}
}
