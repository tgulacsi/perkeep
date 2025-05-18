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
	"os"

	"perkeep.org/internal/sieve"
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
	kv        sorted.KeyValue
	m         map[K]V
	cache     *sieve.Sieve[K, V]
	length    uint64
	batchSize int
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
		cache:     sieve.New[K, V](defaultBatchSize),
		batchSize: defaultBatchSize,
	}
	sorted.Foreach(m, func(_, _ string) error {
		kvm.length++
		return nil
	})
	return kvm, nil
}

func (m *kvMap[K, V, PK, PV]) Len() int { return int(m.length) + len(m.m) }

func (m *kvMap[K, V, PK, PV]) Get(k K) (V, bool, error) {
	if v, ok := m.m[k]; ok {
		return v, true, nil
	}

	if v, ok := m.cache.Get(k); ok {
		return v, true, nil
	}

	var v V
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
	var ok bool
	if err = (PV(&v)).UnmarshalBinary([]byte(val)); err == nil {
		ok = true
	} else if errors.Is(err, sorted.ErrNotFound) {
		err = nil
	}
	m.cache.Add(k, v)
	return v, ok, err
}

func (m *kvMap[K, V, PK, PV]) Commit() error {
	if len(m.m) == 0 {
		return nil
	}
	batch := m.kv.BeginBatch()
	for k, v := range m.m {
		key, err := k.MarshalBinary()
		if err != nil {
			return err
		}
		val, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		batch.Set(string(key), string(val))
	}
	clear(m.m)
	return m.kv.CommitBatch(batch)
}
func (m *kvMap[K, V, PK, PV]) Set(k K, v V) error {
	m.m[k] = v
	if len(m.m) < m.batchSize {
		return nil
	}
	return m.Commit()
}

func (m *kvMap[K, V, PK, PV]) Foreach(fn func(K, V) error) error {
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
