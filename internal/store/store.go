package store

import (
	"container/heap"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type Encoding int

const (
	StringEncoding Encoding = iota
	IntEncoding
	ListEncoding
)

type Value struct {
	encoding  Encoding
	strVal    string
	intVal    int64
	listVal   []string
	expiresAt int64 // stored in milliseconds
}

type Store struct {
	mu        sync.RWMutex
	data      map[string]Value
	evictHeap ExpirationHeap
	indexMap  map[string]*HeapItem
}

func NewStore() *Store {
	s := &Store{
		data:      make(map[string]Value),
		evictHeap: make(ExpirationHeap, 0),
		indexMap:  make(map[string]*HeapItem),
	}
	heap.Init(&s.evictHeap)

	go s.cleanupExpiredKeys()

	return s
}

func (s *Store) Set(key string, value string, ttlSeconds int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()

	var expires int64
	if ttlSeconds > 0 {
		expires = now + (ttlSeconds * 1000)
	}

	soonThreshold := now + 30000

	if item, ok := s.indexMap[key]; ok {
		if expires == 0 || expires > soonThreshold {
			heap.Remove(&s.evictHeap, item.index)
			delete(s.indexMap, key)
		} else {
			item.expiresAt = expires
			heap.Fix(&s.evictHeap, item.index)
		}
	} else {
		if expires > 0 && expires <= soonThreshold {
			item := &HeapItem{
				key:       key,
				expiresAt: expires,
			}
			heap.Push(&s.evictHeap, item)
			s.indexMap[key] = item
		}
	}

	s.data[key] = Value{
		encoding:  StringEncoding,
		strVal:    value,
		expiresAt: expires,
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return "", false
	}

	if val.expiresAt > 0 && time.Now().UnixMilli() > val.expiresAt {
		s.mu.Lock()
		delete(s.data, key)
		delete(s.indexMap, key)
		s.mu.Unlock()
		return "", false
	}

	if val.encoding == IntEncoding {
		return strconv.FormatInt(val.intVal, 10), true
	}

	return val.strVal, true
}

func (s *Store) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]
	if !ok {
		s.data[key] = Value{
			encoding: IntEncoding,
			intVal:   1,
		}
		return 1, nil
	}

	if val.expiresAt > 0 && time.Now().UnixMilli() > val.expiresAt {
		delete(s.data, key)
		delete(s.indexMap, key)
		s.data[key] = Value{
			encoding: IntEncoding,
			intVal:   1,
		}
		return 1, nil
	}

	switch val.encoding {
	case IntEncoding:
		val.intVal++
		s.data[key] = val
		return val.intVal, nil

	case StringEncoding:
		parsed, err := strconv.ParseInt(val.strVal, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		parsed++
		val.encoding = IntEncoding
		val.intVal = parsed
		val.strVal = ""
		s.data[key] = val
		return parsed, nil
	}
	return 0, nil
}

func (s *Store) Del(keys []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	now := time.Now().UnixMilli()

	for _, key := range keys {
		val, ok := s.data[key]
		if !ok {
			continue
		}

		if item, ok := s.indexMap[key]; ok {
			heap.Remove(&s.evictHeap, item.index)
			delete(s.indexMap, key)
		}

		if val.expiresAt > 0 && now > val.expiresAt {
			delete(s.data, key)
			continue
		}

		delete(s.data, key)
		count++
	}

	return count
}

func (s *Store) isExpired(val Value) bool {
	return val.expiresAt > 0 && time.Now().UnixMilli() > val.expiresAt
}

func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return -2
	}

	if s.isExpired(val) {
		s.mu.Lock()
		delete(s.data, key)
		delete(s.indexMap, key)
		s.mu.Unlock()
		return -2
	}

	if val.expiresAt == 0 {
		return -1
	}

	ttl := (val.expiresAt - time.Now().UnixMilli()) / 1000
	if ttl <= 0 {
		ttl = 0
	}
	return ttl
}

func (s *Store) LPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.data[key]
	// 1. Strict Type Check
	if exists && val.encoding != ListEncoding {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var list []string
	if exists {
		list = val.listVal
	}

	// 2. Prepend values (LPUSH)
	for _, v := range values {
		list = append([]string{v}, list...)
	}

	s.data[key] = Value{encoding: ListEncoding, listVal: list}
	return len(list), nil
}

func (s *Store) RPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.data[key]
	// 1. Strict Type Check
	if exists && val.encoding != ListEncoding {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var list []string
	if exists {
		list = val.listVal
	}

	// 2. Append values (RPUSH)
	list = append(list, values...)

	s.data[key] = Value{encoding: ListEncoding, listVal: list}
	return len(list), nil
}

func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]

	if !ok || val.encoding != ListEncoding || len(val.listVal) == 0 {
		return "", false
	}

	item := val.listVal[0]

	val.listVal = val.listVal[1:]

	if len(val.listVal) == 0 {
		delete(s.data, key)
	} else {
		s.data[key] = val
	}

	return item, true
}

func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.data[key]
	if !exists || val.encoding != ListEncoding || len(val.listVal) == 0 {
		return "", false
	}

	list := val.listVal
	lastIdx := len(list) - 1
	popped := list[lastIdx]

	// Update or delete if empty
	if len(list) == 1 {
		delete(s.data, key)
		delete(s.indexMap, key) // Cleanup heap/index if necessary
	} else {
		val.listVal = list[:lastIdx]
		s.data[key] = val
	}

	return popped, true
}

func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return []string{}
	}

	if val.encoding != ListEncoding {
		log.Printf("ERR WRONGTYPE Operation against a key holding the wrong kind of value")
		return []string{}
	}

	list := val.listVal
	length := len(list)

	if length == 0 {
		return []string{}
	}

	// handle negative indexes
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// clamp start
	if start < 0 {
		start = 0
	}

	// clamp stop
	if stop >= length {
		stop = length - 1
	}

	// if start is beyond list or start > stop → empty result
	if start >= length || start > stop {
		return []string{}
	}

	// Redis stop is inclusive, Go slices are exclusive
	return list[start : stop+1]
}

// --- Heap ---

type HeapItem struct {
	key       string
	expiresAt int64
	index     int
}

type ExpirationHeap []*HeapItem

func (h ExpirationHeap) Len() int           { return len(h) }
func (h ExpirationHeap) Less(i, j int) bool { return h[i].expiresAt < h[j].expiresAt }
func (h ExpirationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *ExpirationHeap) Push(x any) {
	data := x.(*HeapItem)
	data.index = len(*h)
	*h = append(*h, data)
}

func (h *ExpirationHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// --- Background cleanup ---

func (s *Store) cleanupExpiredKeys() {
	// runs ~10 times per second
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	randGen := rand.New(rand.NewSource(time.Now().UnixNano()))

	const (
		maxCycleTime = 25 * time.Millisecond // CPU budget per cycle
		sampleSize   = 20                    // keys to sample per loop
	)

	for range ticker.C {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// goroutine survives panics silently
				}
			}()

			start := time.Now()

			for {
				s.mu.Lock()

				now := time.Now().UnixMilli()
				soonThreshold := now + 30000

				// sweep heap first
				// these are keys we already flagged as expiring soon
				for s.evictHeap.Len() > 0 {
					item := s.evictHeap[0]
					if item.expiresAt <= now {
						heap.Pop(&s.evictHeap)
						delete(s.data, item.key)
						delete(s.indexMap, item.key)
					} else {
						break
					}
				}

				// randomly sample the store
				candidates := make([]string, 0, len(s.data))
				for k, v := range s.data {
					if v.expiresAt > 0 {
						candidates = append(candidates, k)
					}
				}

				if len(candidates) == 0 {
					s.mu.Unlock()
					break
				}

				n := sampleSize
				if len(candidates) < n {
					n = len(candidates)
				}

				expiredCount := 0

				for i := 0; i < n; i++ {
					idx := randGen.Intn(len(candidates))
					key := candidates[idx]
					candidates = append(candidates[:idx], candidates[idx+1:]...)

					val, ok := s.data[key]
					if !ok {
						continue
					}

					if val.expiresAt <= now {
						// expired — delete immediately
						delete(s.data, key)
						if item, ok := s.indexMap[key]; ok {
							heap.Remove(&s.evictHeap, item.index)
							delete(s.indexMap, key)
						}
						expiredCount++
					} else if val.expiresAt <= soonThreshold {
						// expiring soon — track in heap for next cycle
						if item, ok := s.indexMap[key]; ok {
							item.expiresAt = val.expiresAt
							heap.Fix(&s.evictHeap, item.index)
						} else {
							item := &HeapItem{
								key:       key,
								expiresAt: val.expiresAt,
							}
							heap.Push(&s.evictHeap, item)
							s.indexMap[key] = item
						}
					}
				}

				s.mu.Unlock()

				// Redis trick: if more than 25% of sampled keys were expired,
				// keyspace is dirty — loop again immediately
				if expiredCount < n/4 {
					break
				}

				// hard stop if we've burned too much CPU this cycle
				if time.Since(start) > maxCycleTime {
					break
				}
			}
		}()
	}
}
