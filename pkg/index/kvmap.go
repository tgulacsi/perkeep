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

	"perkeep.org/pkg/sorted"
	"perkeep.org/pkg/sorted/kvfile"
)

type marshalable[T encoding.BinaryMarshaler] interface {
	encoding.BinaryUnmarshaler
	*T
}

func newKvMap[K, V encoding.BinaryMarshaler, PV marshalable[V]](file string) (kvMap[K, V, PV], error) {
	if file == "" {
		fh, err := os.CreateTemp("", "perkeep-index-*.kvfile")
		if err != nil {
			return kvMap[K, V, PV]{}, err
		}
		file = fh.Name()
		fh.Close()
		os.Remove(file)
	}

	m, err := kvfile.NewStorage(file)
	if err != nil {
		return kvMap[K, V, PV]{}, err
	}
	return kvMap[K, V, PV]{KeyValue: m}, nil
}

type kvMap[K encoding.BinaryMarshaler, V encoding.BinaryMarshaler, PV marshalable[V]] struct {
	sorted.KeyValue
	length uint64
}

func (m *kvMap[K, V, PV]) Len() int { return int(m.length) }

func (m *kvMap[K, V, PV]) Get(k K) (V, bool, error) {
	key, err := k.MarshalBinary()
	var v V
	if err != nil {
		return v, false, err
	}
	val, err := m.KeyValue.Get(string(key))
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
	log.Println("unmarshaled", val, "to", v)
	return v, ok, err
}

func (m *kvMap[K, V, PV]) Set(k K, v V) error {
	key, err := k.MarshalBinary()
	if err != nil {
		return err
	}
	val, err := v.MarshalBinary()
	if err != nil {
		return err
	}
	if _, err = m.KeyValue.Get(string(key)); errors.Is(err, sorted.ErrNotFound) {
		m.length++
	}
	log.Println("set", key, "to", string(val))
	return m.KeyValue.Set(string(key), string(val))
}
