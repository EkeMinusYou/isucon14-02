package main

import (
	"sort"
	"sync"
	"sync/atomic"
)

// Directory locks protect only map entries. Mutable values are owned by the
// affected user/ride/chair locks, never by a directory or the WAL mutex.
type stateMap[T any] struct {
	shards [32]struct {
		mu     sync.RWMutex
		values map[string]T
	}
	count atomic.Int64
}

func newStateMap[T any](seed map[string]T) *stateMap[T] {
	m := &stateMap[T]{}
	for k, v := range seed {
		m.set(k, v)
	}
	return m
}
func (m *stateMap[T]) lookup(k string) (T, bool) {
	var zero T
	if m == nil {
		return zero, false
	}
	b := &m.shards[stateMapBucket(k)]
	b.mu.RLock()
	v, ok := b.values[k]
	b.mu.RUnlock()
	return v, ok
}
func (m *stateMap[T]) get(k string) T { v, _ := m.lookup(k); return v }
func (m *stateMap[T]) set(k string, v T) {
	b := &m.shards[stateMapBucket(k)]
	b.mu.Lock()
	if b.values == nil {
		b.values = make(map[string]T)
	}
	if _, ok := b.values[k]; !ok {
		m.count.Add(1)
	}
	b.values[k] = v
	b.mu.Unlock()
}
func (m *stateMap[T]) update(k string, change func(T) T) {
	b := &m.shards[stateMapBucket(k)]
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.values == nil {
		b.values = make(map[string]T)
	}
	v, exists := b.values[k]
	if !exists {
		m.count.Add(1)
	}
	b.values[k] = change(v)
}
func (m *stateMap[T]) len() int {
	if m == nil {
		return 0
	}
	return int(m.count.Load())
}

// Full directory iteration is only used during exclusive reconstruction.
func (m *stateMap[T]) all() map[string]T {
	out := make(map[string]T, m.len())
	if m == nil {
		return out
	}
	for i := range m.shards {
		b := &m.shards[i]
		b.mu.RLock()
		for k, v := range b.values {
			out[k] = v
		}
		b.mu.RUnlock()
	}
	return out
}
func stateMapBucket(k string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(k); i++ {
		h = (h ^ uint32(k[i])) * 16777619
	}
	return h % 32
}
func keyedStateMutex(directory *sync.Map, id string) *sync.RWMutex {
	if v, ok := directory.Load(id); ok {
		return v.(*sync.RWMutex)
	}
	v, _ := directory.LoadOrStore(id, &sync.RWMutex{})
	return v.(*sync.RWMutex)
}
func (s *rideState) userStateMutex(id string) *sync.RWMutex {
	return keyedStateMutex(&s.userStateLocks, id)
}
func (s *rideState) chairMutex(id string) *sync.RWMutex { return keyedStateMutex(&s.chairLocks, id) }
func lockStateKeys(directory *sync.Map, keys []string, read bool) func() {
	sort.Strings(keys)
	locks := make([]*sync.RWMutex, 0, len(keys))
	for i, k := range keys {
		if i > 0 && keys[i-1] == k {
			continue
		}
		l := keyedStateMutex(directory, k)
		if read {
			l.RLock()
		} else {
			l.Lock()
		}
		locks = append(locks, l)
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			if read {
				locks[i].RUnlock()
			} else {
				locks[i].Unlock()
			}
		}
	}
}

// Business writers use user ID -> ride ID -> chair ID, sorted within
// each domain. Owner registration/listing takes its owner lock before chairs.
// Assigned rides can also be read under their chair lock: every
// mutation of such a ride holds both its user and chair locks.
func (s *rideState) lockRide(id string, read bool) (*memoryRide, func()) {
	r := s.rides.get(id)
	if r == nil {
		return nil, func() {}
	}
	u := s.userStateMutex(r.ownerID)
	if read {
		u.RLock()
		r.mu.RLock()
	} else {
		u.Lock()
		r.mu.Lock()
	}
	var c *sync.RWMutex
	if r.ChairID.Valid {
		c = s.chairMutex(r.ChairID.String)
		if read {
			c.RLock()
		} else {
			c.Lock()
		}
	}
	return r, func() {
		if c != nil {
			if read {
				c.RUnlock()
			} else {
				c.Unlock()
			}
		}
		if read {
			r.mu.RUnlock()
			u.RUnlock()
		} else {
			r.mu.Unlock()
			u.Unlock()
		}
	}
}
