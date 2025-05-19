/*
Copyright 2025 The Perkeep Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package index

import (
	"encoding"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"perkeep.org/pkg/sorted"
	"perkeep.org/pkg/sorted/kvfile"
)

type comparableMarshaler interface {
	encoding.BinaryMarshaler
	comparable
}
type marshalable[T encoding.BinaryMarshaler] interface {
	encoding.BinaryUnmarshaler
	*T
}

const defaultBatchSize = 16 << 10

// kvMap is a map[K]V, eventually (batched in batchSize chunks) stores
// the map in kvfile (sorted.KeyValue).
type kvMap[K comparableMarshaler, V encoding.BinaryMarshaler, PK marshalable[K], PV marshalable[V]] struct {
	kv     sorted.KeyValue
	mu     sync.RWMutex
	m      map[K]V
	length uint64
}

func newKvMap[K comparableMarshaler, V encoding.BinaryMarshaler, PK marshalable[K], PV marshalable[V]](file string) (*kvMap[K, V, PK, PV], error) {
	var isTemp bool
	if file == "" {
		fh, err := os.CreateTemp("", "perkeep-index-*.kvfile")
		if err != nil {
			return nil, err
		}
		file = fh.Name()
		fh.Close()
		os.Remove(file)
		isTemp = true
	}

	m, err := kvfile.NewStorage(file)
	if err != nil {
		return nil, err
	}
	if isTemp {
		os.Remove(file)
	}
	kvm := &kvMap[K, V, PK, PV]{
		kv: m, m: make(map[K]V),
	}
	sorted.Foreach(m, func(_, _ string) error {
		kvm.length++
		return nil
	})
	// Shovel the contents of kvm.m into the sorted.KeyValue
	return kvm, nil
}

// Start the goroutine that will slowly move data from the map[K]V
// to the sorted.KeyValue.
//
// It does it in small chunks with minimal locking.
func (kvm *kvMap[K, V, PK, PV]) Start() {
	go func() {
		m := make(map[K]V)
		errFinished := errors.New("finished")
		start := time.Now()
		var moved int
		for {
			if err := func() error {
				// Save map, avoid long locking
				kvm.mu.RLock()
				clear(m)
				for k, v := range kvm.m {
					m[k] = v
					if len(m) >= 16<<10 {
						break
					}
				}
				kvm.mu.RUnlock()
				if len(m) == 0 {
					return errFinished
				}
				// start = time.Now()
				batch := kvm.kv.BeginBatch()
				for k, v := range m {
					key, err := k.MarshalBinary()
					if err != nil {
						log.Printf("marshal %v: %+v", k, err)
						continue
					}
					val, err := v.MarshalBinary()
					if err != nil {
						log.Printf("marshal %v: %+v", v, err)
						continue
					}
					batch.Set(string(key), string(val))
				}
				if err := kvm.kv.CommitBatch(batch); err != nil {
					log.Printf("CommitBatch: %+v", err)
					return err
				}

				kvm.mu.Lock()
				for k := range m {
					delete(kvm.m, k)
				}
				kvm.mu.Unlock()
				moved += len(m)
				clear(m)
				time.Sleep(100 * time.Millisecond)
				return nil
			}(); err != nil {
				if err == errFinished {
					log.Printf("moved %d in %s", moved, time.Since(start))
					return
				}
				log.Println(err)
			}
		}
	}()
}

type kvPair[K, V any] struct {
	k K
	v V
}

func (m *kvMap[K, V, PK, PV]) Len() int {
	m.mu.RLock()
	n := int(m.length) + len(m.m)
	m.mu.RUnlock()
	return n
}

func (m *kvMap[K, V, PK, PV]) Get(k K) (V, bool, error) {
	m.mu.RLock()
	v, ok := m.m[k]
	m.mu.RUnlock()
	if ok {
		return v, true, nil
	}

	key, err := k.MarshalBinary()
	if err != nil {
		return v, false, err
	}
	val, err := m.kv.Get(string(key))
	if err != nil {
		if errors.Is(err, sorted.ErrNotFound) {
			return v, false, nil
		}
		return v, false, err
	}
	if err = (PV(&v)).UnmarshalBinary([]byte(val)); err == nil {
		ok = true
	} else if errors.Is(err, sorted.ErrNotFound) {
		err = nil
	}
	return v, ok, err
}

func (m *kvMap[K, V, PK, PV]) Set(k K, v V) error {
	m.mu.Lock()
	m.m[k] = v
	m.mu.Unlock()
	return nil
}

func (m *kvMap[K, V, PK, PV]) Foreach(fn func(K, V) error) error {
	m.mu.RUnlock()
	defer m.mu.RUnlock()
	for k, v := range m.m {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return sorted.Foreach(m.kv, func(key, value string) error {
		var k K
		if err := PK(&k).UnmarshalBinary([]byte(key)); err != nil {
			return err
		}
		var v V
		if err := PV(&v).UnmarshalBinary([]byte(value)); err != nil {
			return err
		}
		return fn(k, v)
	})
}
