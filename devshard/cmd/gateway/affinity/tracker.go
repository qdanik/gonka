// Package affinity remembers which participant last won each conversation, so its next turn can be sent back to the host whose prefix cache holds it.
package affinity

import (
	"container/list"
	"sync"
	"time"
)

// Limits bound the tracker; both are read on every use, so a change reaches a running tracker.
type Limits struct {
	TTL        time.Duration
	MaxEntries int
}

type entry struct {
	key         Key
	participant string
	session     string
	recordedAt  time.Time
}

// Tracker maps a key to the participant that last won it, newest first; an entry older than the TTL is gone, and the oldest go first past the entry cap.
type Tracker struct {
	limits func() Limits
	now    func() time.Time

	mu      sync.Mutex
	entries map[Key]*list.Element
	order   *list.List
}

func NewTracker(limits func() Limits, now func() time.Time) *Tracker {
	return &Tracker{limits: limits, now: now, entries: map[Key]*list.Element{}, order: list.New()}
}

// Lookup answers the participant that last won the key; an expired entry is dropped on the way.
func (t *Tracker) Lookup(key Key) (string, Status) {
	t.mu.Lock()
	defer t.mu.Unlock()
	found, status := t.live(key)
	if status != StatusHit {
		return NoParticipant, status
	}
	return found.participant, StatusHit
}

// Session answers the session key the gateway issued with the key's last win, NoSession when it holds none; it leaves an expired entry for Lookup to count.
func (t *Tracker) Session(key Key) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	element, known := t.entries[key]
	if !known || t.expired(element.Value.(*entry)) {
		return NoSession
	}
	return element.Value.(*entry).session
}

func (t *Tracker) live(key Key) (*entry, Status) {
	element, known := t.entries[key]
	if !known {
		return nil, StatusNew
	}
	found := element.Value.(*entry)
	if t.expired(found) {
		t.remove(element)
		return nil, StatusExpired
	}
	return found, StatusHit
}

func (t *Tracker) expired(found *entry) bool {
	return t.now().Sub(found.recordedAt) > t.limits().TTL
}

// Record makes the participant the key's last winner under the session key the gateway issued for it, and sheds what has expired or no longer fits.
func (t *Tracker) Record(key Key, participant, session string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if element, known := t.entries[key]; known {
		found := element.Value.(*entry)
		found.participant, found.session, found.recordedAt = participant, session, now
		t.order.MoveToFront(element)
	} else {
		t.entries[key] = t.order.PushFront(&entry{key: key, participant: participant, session: session, recordedAt: now})
	}
	bounds := t.limits()
	for oldest := t.order.Back(); oldest != nil; oldest = t.order.Back() {
		if t.order.Len() <= bounds.MaxEntries && now.Sub(oldest.Value.(*entry).recordedAt) <= bounds.TTL {
			return
		}
		t.remove(oldest)
	}
}

func (t *Tracker) remove(element *list.Element) {
	delete(t.entries, element.Value.(*entry).key)
	t.order.Remove(element)
}

// Len is the number of entries held.
func (t *Tracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.order.Len()
}
