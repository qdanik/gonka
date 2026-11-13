// Package affinity remembers which participant last won each conversation, so its next turn can be sent back to the host whose prefix cache holds it.
// See devshard/docs/proposals/gateway-prefix-affinity.md
package affinity

import (
	"container/list"
	"sync"
	"time"
)

const shareFloor = 8

// Limits bound the tracker; all are read on every use, so a change reaches a running tracker.
// ShareCap is a percentage of a participant's slot share of the pins; MinCachedTokens is the expected cached prefix below which a hit is not worth burning nonces for, and 0 turns it off.
type Limits struct {
	TTL             time.Duration
	MaxEntries      int
	MaxPinAge       time.Duration
	ShareCap        int
	MinCachedTokens uint64
}

// Weights answers how many slots each participant holds in the groups the gateway routes over, read-only.
type Weights func() map[string]int

type entry struct {
	key          Key
	participant  string
	session      string
	pinnedAt     time.Time
	recordedAt   time.Time
	promptTokens uint64
	capped       bool
}

// Tracker maps a key to the participant that last won it, newest first, within the bounds of Limits. See routing.md, "Host affinity".
type Tracker struct {
	limits  func() Limits
	weights Weights
	now     func() time.Time

	mu      sync.Mutex
	entries map[Key]*list.Element
	order   *list.List
	held    map[string]int
	pinned  int
}

// NewTracker builds a tracker; without weights the share cap is not applied.
func NewTracker(limits func() Limits, weights Weights, now func() time.Time) *Tracker {
	return &Tracker{limits: limits, weights: weights, now: now, entries: map[Key]*list.Element{}, order: list.New(), held: map[string]int{}}
}

// Lookup answers the participant that last won the key; an entry that is expired, aged out or a tombstone is dropped on the way.
func (t *Tracker) Lookup(key Key, promptTokens uint64) (string, Status) {
	t.mu.Lock()
	defer t.mu.Unlock()
	element, known := t.entries[key]
	if !known {
		return NoParticipant, StatusNew
	}
	found := element.Value.(*entry)
	bounds := t.limits()
	now := t.now()
	switch {
	case now.Sub(found.recordedAt) > bounds.TTL:
		t.remove(element)
		return NoParticipant, StatusExpired
	case found.capped:
		t.remove(element)
		return NoParticipant, StatusShareCapped
	case now.Sub(found.pinnedAt) > bounds.MaxPinAge:
		t.remove(element)
		return NoParticipant, StatusPinAged
	case min(found.promptTokens, promptTokens) < bounds.MinCachedTokens:
		return NoParticipant, StatusBelowMinCached
	}
	return found.participant, StatusHit
}

// Session answers the session key the gateway issued with the key's last win, NoSession when it holds none; it leaves an expired entry for Lookup to count.
func (t *Tracker) Session(key Key) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	element, known := t.entries[key]
	if !known || t.now().Sub(element.Value.(*entry).recordedAt) > t.limits().TTL {
		return NoSession
	}
	return element.Value.(*entry).session
}

// Record makes the participant the key's last winner under the session key the gateway issued for it and remembers the prompt it won with, unless that would take it past its share, and sheds what has expired or no longer fits.
func (t *Tracker) Record(key Key, participant, session string, promptTokens uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	bounds := t.limits()
	element := t.entries[key]
	if element == nil {
		t.pin(key, nil, participant, session, now, promptTokens, bounds)
	} else if found := element.Value.(*entry); found.participant == participant && now.Sub(found.pinnedAt) <= bounds.MaxPinAge {
		found.recordedAt, found.session = now, session
		if !found.holdsLongerPrefix(promptTokens, bounds) {
			found.promptTokens = promptTokens
		}
		t.order.MoveToFront(element)
	} else if !found.outweighs(participant, promptTokens, now, bounds) {
		t.pin(key, element, participant, session, now, promptTokens, bounds)
	}
	for oldest := t.order.Back(); oldest != nil; oldest = t.order.Back() {
		if t.order.Len() <= bounds.MaxEntries && now.Sub(oldest.Value.(*entry).recordedAt) <= bounds.TTL {
			return
		}
		t.remove(oldest)
	}
}

func (e *entry) outweighs(participant string, promptTokens uint64, now time.Time, bounds Limits) bool {
	return !e.capped && e.participant != participant && e.holdsLongerPrefix(promptTokens, bounds) &&
		now.Sub(e.recordedAt) <= bounds.TTL && now.Sub(e.pinnedAt) <= bounds.MaxPinAge
}

func (e *entry) holdsLongerPrefix(promptTokens uint64, bounds Limits) bool {
	return bounds.MinCachedTokens > 0 && e.promptTokens >= bounds.MinCachedTokens && e.promptTokens > promptTokens
}

func (t *Tracker) pin(key Key, previous *list.Element, participant, session string, now time.Time, promptTokens uint64, bounds Limits) {
	if previous != nil {
		t.remove(previous)
	}
	capped := t.pastShare(participant, bounds.ShareCap)
	if !capped {
		t.held[participant]++
		t.pinned++
	}
	t.entries[key] = t.order.PushFront(&entry{key: key, participant: participant, session: session, pinnedAt: now, recordedAt: now, promptTokens: promptTokens, capped: capped})
}

func (t *Tracker) pastShare(participant string, sharePercent int) bool {
	if t.weights == nil || t.held[participant]+1 <= shareFloor {
		return false
	}
	slots := t.weights()
	total := 0
	for _, count := range slots {
		total += count
	}
	if total == 0 {
		return false
	}
	return (t.held[participant]+1)*total*100 > sharePercent*(t.pinned+1)*slots[participant]
}

func (t *Tracker) remove(element *list.Element) {
	removed := element.Value.(*entry)
	delete(t.entries, removed.key)
	t.order.Remove(element)
	if removed.capped {
		return
	}
	t.pinned--
	if t.held[removed.participant]--; t.held[removed.participant] == 0 {
		delete(t.held, removed.participant)
	}
}

// Len is the number of entries held.
func (t *Tracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.order.Len()
}
