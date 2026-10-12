/*
Copyright 2026 The Perkeep Authors

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

package diskpacked

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// fdCache bounds the number of open files it owns.
// It uses SIEVE for eviction and pins active leases.
type fdCache struct {
	root     string
	mu       sync.Mutex
	entries  map[string]*entry
	queue    list.List     // front: newest, back: oldest
	hand     *list.Element // next candidate; nil means oldest
	maxOpen  int
	changed  chan struct{}
	closing  bool
	closeErr error
}

type entry struct {
	path    string
	file    *os.File
	refs    int
	visited bool
	node    *list.Element
}

type sectionReader struct {
	*io.SectionReader
	cache    *fdCache
	entry    *entry
	once     sync.Once
	closeErr error
}

func newFDCache(root string, maxOpen int) *fdCache {
	if maxOpen <= 0 {
		panic("maxOpen must be positive")
	}
	return &fdCache{
		root:    root,
		entries: make(map[string]*entry),
		maxOpen: maxOpen,
		changed: make(chan struct{}),
	}
}

// signalLocked wakes all waiters.
// Caller must hold c.mu.
func (c *fdCache) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// victimLocked selects an unpinned victim using SIEVE.
// Two passes handle a queue of visited entries and
// prevent infinite looping when every entry is pinned.
// Caller must hold c.mu.
func (c *fdCache) victimLocked() *entry {
	n := c.queue.Len()

	for scanned := 0; scanned < 2*n; scanned++ {
		if c.hand == nil {
			c.hand = c.queue.Back()
		}

		node := c.hand

		// Move toward newer entries.
		// Wrap to the oldest on the next iteration.
		c.hand = node.Prev()

		e := node.Value.(*entry)

		// Pinned: never evict an active reader.
		if e.refs != 0 {
			continue
		}

		// Second chance: clear the bit,
		// but do not move the entry.
		if e.visited {
			e.visited = false
			continue
		}

		// Unpinned and unvisited: victim found.
		return e
	}

	// Every entry is pinned.
	return nil
}

var _ io.ReadSeekCloser = (*sectionReader)(nil)

// OpenSection obtains a lease on a file.
// If all FD slots are pinned, it waits for capacity.
// The returned Section must be closed after its last read.
func (c *fdCache) OpenSection(
	ctx context.Context,
	path string,
	off, length int64,
) (*sectionReader, error) {
	if off < 0 || length < 0 {
		return nil, fmt.Errorf(
			"negative section offset or length",
		)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		c.mu.Lock()

		if c.closing {
			c.mu.Unlock()
			return nil, os.ErrClosed
		}

		// SIEVE cache hit:
		// Set visited, but do not reorder the queue.
		if e := c.entries[path]; e != nil {
			e.refs++
			e.visited = true
			c.mu.Unlock()

			return &sectionReader{
				SectionReader: io.NewSectionReader(
					e.file, off, length,
				),
				cache: c,
				entry: e,
			}, nil
		}

		// Cache miss with no free FD slot.
		if len(c.entries) == c.maxOpen {
			victim := c.victimLocked()

			if victim == nil {
				// All entries are pinned.
				// Release mutex while waiting.
				changed := c.changed
				c.mu.Unlock()

				select {
				case <-changed:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}

			// Evict the selected SIEVE victim.
			delete(c.entries, victim.path)
			c.queue.Remove(victim.node)

			openFdsVar.Add(c.root, -1)
			if err := victim.file.Close(); err != nil {
				c.mu.Unlock()
				return nil, fmt.Errorf(
					"evict %q: %w", victim.path, err,
				)
			}
		}

		// Open under mutex to enforce the FD limit
		// and prevent duplicate opens for the same path.
		f, err := os.Open(path)
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		openFdsVar.Add(c.root, 1)

		// SIEVE insertion:
		// Newest entry, visited initially false.
		e := &entry{
			path: path,
			file: f,
			refs: 1,
		}

		e.node = c.queue.PushFront(e)
		c.entries[path] = e

		c.mu.Unlock()

		return &sectionReader{
			SectionReader: io.NewSectionReader(
				f, off, length,
			),
			cache: c,
			entry: e,
		}, nil
	}
}

// Close releases the Section's lease.
// It does not normally close the underlying file:
// idle files remain cached until eviction or shutdown.
//
// Do not call Close while a read on this Section
// is still in progress.
func (s *sectionReader) Close() error {
	s.release()
	return s.closeErr
}

// Read reads from the Section and releases the lease once the
// Section is fully consumed (io.EOF), so an eagerly-read but
// never-closed Section does not pin an FD forever.
func (s *sectionReader) Read(p []byte) (int, error) {
	n, err := s.SectionReader.Read(p)
	if err == io.EOF {
		s.release()
	}
	return n, err
}

// release drops the Section's lease exactly once.
// It is called either by Close or when Read reaches io.EOF.
func (s *sectionReader) release() {
	s.once.Do(func() {
		c := s.cache

		c.mu.Lock()
		defer c.mu.Unlock()

		s.entry.refs--

		if s.entry.refs == 0 {
			if c.closing {
				// Cache is shutting down:
				// close the file on its last release.
				delete(c.entries, s.entry.path)
				c.queue.Remove(s.entry.node)

				openFdsVar.Add(c.root, -1)
				s.closeErr = s.entry.file.Close()
				c.closeErr = errors.Join(
					c.closeErr, s.closeErr,
				)
			}

			// An FD may now be evictable.
			c.signalLocked()
		}
	})
}

// open opens path for reading and caches it, without taking a lease.
// It is used to eagerly open the current pack file for reading.
func (c *fdCache) open(ctx context.Context, path string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		c.mu.Lock()

		if c.closing {
			c.mu.Unlock()
			return os.ErrClosed
		}
		if _, ok := c.entries[path]; ok {
			c.mu.Unlock()
			return nil
		}

		if len(c.entries) == c.maxOpen {
			victim := c.victimLocked()
			if victim == nil {
				changed := c.changed
				c.mu.Unlock()

				select {
				case <-changed:
				case <-ctx.Done():
					return ctx.Err()
				}
				continue
			}

			delete(c.entries, victim.path)
			c.queue.Remove(victim.node)

			openFdsVar.Add(c.root, -1)
			if err := victim.file.Close(); err != nil {
				c.mu.Unlock()
				return fmt.Errorf("evict %q: %w", victim.path, err)
			}
		}

		f, err := os.Open(path)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		openFdsVar.Add(c.root, 1)

		e := &entry{path: path, file: f}
		e.node = c.queue.PushFront(e)
		c.entries[path] = e

		c.mu.Unlock()
		return nil
	}
}

// Close rejects new leases and immediately closes
// idle files. It waits for active leases to finish,
// unless the context expires.
//
// If the context expires, active files remain open
// until their respective Section.Close() calls.
func (c *fdCache) Close(ctx context.Context) error {
	c.mu.Lock()

	if !c.closing {
		c.closing = true
		c.hand = nil

		for path, e := range c.entries {
			if e.refs != 0 {
				continue
			}

			delete(c.entries, path)
			c.queue.Remove(e.node)

			openFdsVar.Add(c.root, -1)
			c.closeErr = errors.Join(
				c.closeErr, e.file.Close(),
			)
		}

		c.signalLocked()
	}

	for len(c.entries) != 0 {
		if err := ctx.Err(); err != nil {
			result := errors.Join(c.closeErr, err)
			c.mu.Unlock()
			return result
		}

		changed := c.changed
		c.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
		}

		c.mu.Lock()
	}

	result := c.closeErr
	c.mu.Unlock()

	return result
}
