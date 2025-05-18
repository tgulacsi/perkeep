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

	"perkeep.org/pkg/sorted"
	"perkeep.org/pkg/sorted/kvfile"
)

type marshalable[T encoding.BinaryMarshaler] interface {
	encoding.BinaryUnmarshaler
	*T
}

const defaultBatchSize = 16 << 10

type kvMap[K encoding.BinaryMarshaler, V encoding.BinaryMarshaler, PK marshalable[K], PV marshalable[V]] struct {
	kv               sorted.KeyValue
	batch            sorted.BatchMutation
	batchKeys        map[string]struct{}
	length, batchLen uint64
	batchSize        int
}

func newKvMap[K, V encoding.BinaryMarshaler, PK marshalable[K], PV marshalable[V]](file string) (*kvMap[K, V, PK, PV], error) {
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
	kvm := &kvMap[K, V, PK, PV]{kv: m, batchSize: defaultBatchSize}
	sorted.Foreach(m, func(_, _ string) error {
		kvm.length++
		return nil
	})
	return kvm, nil
}

func (m *kvMap[K, V, PK, PV]) Len() int { return int(m.length) + len(m.batchKeys) }

func (m *kvMap[K, V, PK, PV]) Get(k K) (V, bool, error) {
	key, err := k.MarshalBinary()
	var v V
	if err != nil {
		return v, false, err
	}
	keyS := string(key)
	if m.batch != nil {
		if _, ok := m.batchKeys[keyS]; ok {
			if err = m.Begin(); err != nil {
				return v, true, err
			}
		}
	}

	val, err := m.kv.Get(keyS)
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
	return v, ok, err
}

func (m *kvMap[K, V, PK, PV]) Begin() error {
	if err := m.Commit(); err != nil {
		return err
	}
	if m.batchKeys == nil {
		m.batchKeys = make(map[string]struct{}, m.batchSize)
	}
	m.batch = m.kv.BeginBatch()
	return nil
}
func (m *kvMap[K, V, PK, PV]) Commit() error {
	m.batchLen = 0
	if m.batchKeys != nil {
		clear(m.batchKeys)
	}
	if m.batch == nil {
		return nil
	}
	err := m.kv.CommitBatch(m.batch)
	m.batch = nil
	return err
}
func (m *kvMap[K, V, PK, PV]) Set(k K, v V) error {
	key, err := k.MarshalBinary()
	if err != nil {
		return err
	}
	val, err := v.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err = m.kv.Get(string(key)); errors.Is(err, sorted.ErrNotFound) {
		m.length++
	}
	keyS, valS := string(key), string(val)
	if m.batch == nil {
		return m.kv.Set(keyS, valS)
	}
	m.batch.Set(keyS, valS)
	m.batchKeys[keyS] = struct{}{}
	if len(m.batchKeys) < m.batchSize {
		return nil
	}
	if err = m.Commit(); err != nil {
		return err
	}
	return m.Begin()
}

func (m *kvMap[K, V, PK, PV]) Foreach(fn func(K, V) error) error {
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
