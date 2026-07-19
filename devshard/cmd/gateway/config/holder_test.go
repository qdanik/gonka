package config

import (
	"sync"
	"testing"
)

func TestHolderLoadReturnsInitialSnapshot(t *testing.T) {
	initial := Defaults()
	holder := NewHolder(&initial)
	if holder.Load() != &initial {
		t.Fatal("Load() must return the exact pointer given to NewHolder")
	}
}

func TestHolderSwapNotifiesSubscribersWithNewSnapshot(t *testing.T) {
	initial := Defaults()
	holder := NewHolder(&initial)

	var mutex sync.Mutex
	var seen []*Config
	cancel := holder.Subscribe(func(next *Config) {
		mutex.Lock()
		defer mutex.Unlock()
		seen = append(seen, next)
	})
	defer cancel()

	next := Defaults()
	next.Server.Port = 9999
	holder.Swap(&next)

	if holder.Load().Server.Port != 9999 {
		t.Fatalf("Load().Server.Port = %d, want 9999 after Swap", holder.Load().Server.Port)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(seen) != 1 || seen[0] != &next {
		t.Fatalf("subscriber saw %v, want exactly the swapped snapshot once", seen)
	}
}

func TestHolderCancelledSubscriberIsNotNotified(t *testing.T) {
	initial := Defaults()
	holder := NewHolder(&initial)

	notified := false
	cancel := holder.Subscribe(func(*Config) { notified = true })
	cancel()

	next := Defaults()
	holder.Swap(&next)
	if notified {
		t.Fatal("cancelled subscriber must not be notified")
	}
}

func TestHolderConcurrentLoadAndSwapIsRaceFree(t *testing.T) {
	initial := Defaults()
	holder := NewHolder(&initial)

	var waitGroup sync.WaitGroup
	for range 4 {
		waitGroup.Add(2)
		go func() {
			defer waitGroup.Done()
			for range 500 {
				snapshot := Defaults()
				holder.Swap(&snapshot)
			}
		}()
		go func() {
			defer waitGroup.Done()
			for range 500 {
				_ = holder.Load().Server.Port
			}
		}()
	}
	waitGroup.Wait()
}
