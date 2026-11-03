package peerload

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"devshard/cmd/gateway/chain"
)

var noScheduledWrite time.Time

// Registrar writes this instance's own registry entry.
type Registrar interface {
	RegisterInstance(ctx context.Context, registration chain.RegistryRegistration) error
}

// Settings configures the channel. Shadow is the only mode: the discount reaches /metrics and nothing else.
type Settings struct {
	Self          PeerID
	AdvertiseURL  string
	ListenAddress string
	ChainKey      []byte
	Rounding      Rounding
	CommitPoll    time.Duration
}

// Deps is what the plugin reads and writes outside itself.
type Deps struct {
	Registry    Registry
	Registrar   Registrar
	Samples     func() []LoadSample
	Concurrency func(participant, model string) int64
	Now         func() time.Time
	Listen      func(ctx context.Context, network, address string) (net.Listener, error)
	Dial        DialFunc
	Report      func(message string, err error)
}

// Plugin is the whole channel behind one hook. A nil Plugin is the switched-off feature: it listens on nothing, dials nothing and starts no goroutine.
type Plugin struct {
	settings Settings
	deps     Deps
	identity transportIdentity
	members  *membership
	verifier *verifier
	view     *View
	topic    *loadTopic
	server   *peerServer
	poller   *poller

	nudge    chan struct{}
	finished sync.WaitGroup
	writes   writeSchedule
}

type writeSchedule struct {
	mu      sync.Mutex
	backoff time.Duration
	next    time.Time
}

// New validates the settings and builds the plugin; it reads and dials nothing.
func New(settings Settings, deps Deps) (*Plugin, error) {
	if settings.Self.Address == emptyAddress || !validInstanceID(settings.Self.Instance) {
		return nil, fmt.Errorf("%w: instance id must be [a-z0-9-]+, at most %d bytes", ErrBadSettings, instanceIDLimit)
	}
	if !validEndpoint(settings.AdvertiseURL) {
		return nil, fmt.Errorf("%w: advertise URL must be https://host:port, at most %d bytes", ErrBadSettings, endpointLimit)
	}
	if deps.Registry == nil || deps.Registrar == nil {
		return nil, fmt.Errorf("%w: the channel needs a chain connection", ErrBadSettings)
	}
	identity, err := deriveTransportIdentity(settings.ChainKey, settings.Self)
	if err != nil {
		return nil, err
	}
	members := newMembership(ownIdentity{ID: settings.Self, Fingerprint: identity.Fingerprint, AdvertiseURL: settings.AdvertiseURL}, deps.Registry, deps.Now)
	verifier := newVerifier(members)
	view := NewView(loadMaxAge, deps.Concurrency, deps.Now)
	topic := newLoadTopic(deps.Samples, settings.Rounding, view)
	settings.ChainKey = nil
	return &Plugin{
		settings: settings,
		deps:     deps,
		identity: identity,
		members:  members,
		verifier: verifier,
		view:     view,
		topic:    topic,
		server:   newServer(settings.Self, verifier, identity, []peerTopic{topic}, deps.Now),
		poller:   newPoller(members, verifier, identity, view, deps.Dial, deps.Now, deps.Report),
		nudge:    make(chan struct{}, 1),
	}, nil
}

// ObserveChain takes one chain snapshot's allowlist and height; it never blocks the observer that calls it.
func (p *Plugin) ObserveChain(allowed []string, height int64) {
	if p == nil {
		return
	}
	if p.members.ObserveChain(allowed, height) {
		select {
		case p.nudge <- struct{}{}:
		default:
		}
	}
}

// Start opens the listener and the loops, which stop when ctx ends.
func (p *Plugin) Start(ctx context.Context) error {
	if p == nil {
		return nil
	}
	listener, err := p.deps.Listen(ctx, listenNetwork, p.settings.ListenAddress)
	if err != nil {
		return fmt.Errorf("peer listener %s: %w", p.settings.ListenAddress, err)
	}
	p.finished.Go(func() {
		if serveErr := p.server.Serve(listener); serveErr != nil {
			p.deps.Report("peer listener stopped", serveErr)
		}
	})
	p.finished.Go(func() { p.pollLoop(ctx) })
	p.finished.Go(func() { p.registryLoop(ctx) })
	p.finished.Go(func() {
		<-ctx.Done()
		_ = p.server.Close()
		p.poller.Close()
	})
	return nil
}

// Wait returns once a started plugin's loops have stopped.
func (p *Plugin) Wait() {
	if p != nil {
		p.finished.Wait()
	}
}

func (p *Plugin) refreshRegistry(ctx context.Context) {
	err := p.refreshMembers(ctx)
	if err == nil || errors.Is(err, errPartialRead) {
		if err := p.announce(ctx); err != nil {
			p.deps.Report("peer registration failed", err)
		}
	}
}

func (p *Plugin) refreshMembers(ctx context.Context) error {
	err := p.members.Refresh(ctx)
	if err != nil {
		p.deps.Report("peer registry read failed", err)
	}
	p.dropRemoved()
	return err
}

func (p *Plugin) dropRemoved() {
	p.server.CloseRemoved()
	p.poller.Sync()
}

func (p *Plugin) pollPeers(ctx context.Context) { p.poller.Round(ctx, p.topic) }

// View is the shadow view of peer load.
func (p *Plugin) View() *View { return p.view }

// HandshakeCounts copies the handshake results so far, by result.
func (p *Plugin) HandshakeCounts() map[string]uint64 { return p.verifier.HandshakeCounts() }

func (p *Plugin) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(p.topic.Interval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pollPeers(ctx)
		}
	}
}

func (p *Plugin) registryLoop(ctx context.Context) {
	p.refreshRegistry(ctx)
	wait := p.nextRegistryRead(firstReadRetryInterval)
	ticker := time.NewTicker(wait)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.members.HasAddresses() {
				wait = min(wait*retryWaitFactor, registryRefreshInterval)
			}
		case <-p.nudge:
		}
		p.refreshRegistry(ctx)
		wait = p.nextRegistryRead(wait)
		ticker.Reset(wait)
	}
}

func (p *Plugin) nextRegistryRead(wait time.Duration) time.Duration {
	if p.members.HasRead() {
		return registryRefreshInterval
	}
	return wait
}

func (p *Plugin) announce(ctx context.Context) error {
	self := p.settings.Self
	if !p.members.Allowed(self.Address) {
		return nil
	}
	entry, found := p.members.Lookup(self)
	if found && p.entryCurrent(entry) {
		p.writes.reset()
		return nil
	}
	if found && p.withinUpdateInterval(entry) {
		return nil
	}
	if !p.writes.due(p.deps.Now()) {
		return nil
	}
	writeCtx, cancel := context.WithTimeout(ctx, max(p.settings.CommitPoll, chain.DefaultPollTimeout)+registerBroadcastMargin)
	defer cancel()
	err := p.deps.Registrar.RegisterInstance(writeCtx, chain.RegistryRegistration{
		InstanceID: self.Instance,
		Endpoint:   p.settings.AdvertiseURL,
		SPKISHA256: hex.EncodeToString(p.identity.Fingerprint[:]),
		Protocol:   protocolVersion,
		Topics:     []string{topicLabel(p.topic)},
	})
	p.writes.record(err, p.deps.Now())
	if err != nil {
		return fmt.Errorf("register instance %s: %w", self, err)
	}
	_ = p.refreshMembers(ctx)
	return nil
}

func (p *Plugin) withinUpdateInterval(entry peerInstance) bool {
	return p.entryAgeBlocks(entry) < minUpdateIntervalBlocks
}

func (p *Plugin) entryAgeBlocks(entry peerInstance) int64 {
	return p.members.Height() - int64(entry.UpdatedHeight)
}

func (s *writeSchedule) due(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !now.Before(s.next)
}

func (s *writeSchedule) record(err error, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		s.resetLocked()
		return
	}
	s.backoff = min(max(s.backoff*retryWaitFactor, registryRefreshInterval), reregisterInterval)
	s.next = now.Add(s.backoff)
}

func (s *writeSchedule) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetLocked()
}

func (s *writeSchedule) resetLocked() {
	s.backoff = noWriteBackoff
	s.next = noScheduledWrite
}

func (p *Plugin) entryCurrent(entry peerInstance) bool {
	return entry.Endpoint == p.settings.AdvertiseURL &&
		entry.Fingerprint == p.identity.Fingerprint &&
		entry.SupportsTopic(p.topic.Name(), p.topic.Version()) &&
		p.entryAgeBlocks(entry) < reannounceBlocks
}
