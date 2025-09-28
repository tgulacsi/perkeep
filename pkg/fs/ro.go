/*
Copyright 2013 The Perkeep Authors

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

package fs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go4.org/types"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/search"
)

// roDir is a read-only directory.
// Its permanode is the permanode with camliPath:entname attributes.
type roDir struct {
	fs        *PkFileSystem
	permanode blob.Ref
	parent    *roDir // or nil, if the root within its roots.go root.
	name      string // ent name (base name within parent)
	at        time.Time

	mu       sync.Mutex
	children map[string]roFileOrDir
	// xattrs   map[string][]byte
}

var (
	_ fs.FS        = (*roDir)(nil)
	_ fs.ReadDirFS = (*roDir)(nil)
	_ roFileOrDir  = (*roDir)(nil)
)

func newRODir(fs *PkFileSystem, permanode blob.Ref, name string, at time.Time) *roDir {
	return &roDir{
		fs:        fs,
		permanode: permanode,
		name:      name,
		at:        at,
	}
}

// for debugging
func (n *roDir) fullPath() string {
	if n == nil {
		return ""
	}
	return filepath.Join(n.parent.fullPath(), n.name)
}

func (n *roDir) Close() error  { return nil }
func (n *roDir) Ref() blob.Ref { return n.permanode }
func (n *roDir) Stat() (fs.FileInfo, error) {
	return fileInfo{
		mode: os.ModeDir | 0500,
	}, nil
}
func (n *roDir) Read(_ []byte) (int, error) { return 0, fs.ErrInvalid }

// ReadDir reads the named directory
// and returns a list of directory entries sorted by filename.
func (n *roDir) ReadDir(name string) ([]fs.DirEntry, error) {
	ctx, cancel := context.WithTimeout(n.fs.Context(), time.Minute)
	defer cancel()
	err := n.populate(ctx)
	n.mu.Lock()
	defer n.mu.Unlock()
	var ents []fs.DirEntry
	for _, childNode := range n.children {
		fi, err := childNode.Stat()
		if err != nil {
			return ents, err
		}
		ents = append(ents, fi.(fileInfo))
	}
	return ents, err
}

// populate hits the blobstore to populate map of child nodes.
func (n *roDir) populate(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// Things never change here, so if we've ever populated, we're
	// populated.
	if n.children != nil {
		return nil
	}

	Logger.Printf("roDir.populate(%q) - Sending request At %v", n.fullPath(), n.at)

	res, err := n.fs.client.Describe(ctx, &search.DescribeRequest{
		BlobRef: n.permanode,
		Depth:   3,
		At:      types.Time3339(n.at),
	})
	if err != nil {
		Logger.Println("roDir.paths:", err)
		return fmt.Errorf("error while describing permanode: %w", err)
	}
	db := res.Meta[n.permanode.String()]
	if db == nil {
		return errors.New("dir blobref not described")
	}

	// Find all child permanodes and stick them in n.children
	n.children = make(map[string]roFileOrDir)
	for k, v := range db.Permanode.Attr {
		const p = "camliPath:"
		if !strings.HasPrefix(k, p) || len(v) < 1 {
			continue
		}
		name := k[len(p):]
		childRef := v[0]
		child := res.Meta[childRef]
		if child == nil {
			Logger.Printf("child not described: %v", childRef)
			continue
		}
		if target := child.Permanode.Attr.Get("camliSymlinkTarget"); target != "" {
			// This is a symlink.
			n.children[name] = &roFile{
				fs:        n.fs,
				permanode: blob.ParseOrZero(childRef),
				parent:    n,
				name:      name,
				symLink:   true,
				target:    target,
			}
		} else if isDir(child.Permanode) {
			// This is a directory.
			n.children[name] = &roDir{
				fs:        n.fs,
				permanode: blob.ParseOrZero(childRef),
				parent:    n,
				name:      name,
				at:        n.at,
			}
		} else if contentRef := child.Permanode.Attr.Get("camliContent"); contentRef != "" {
			// This is a file.
			content := res.Meta[contentRef]
			if content == nil {
				Logger.Printf("child content not described: %v", childRef)
				continue
			}
			if content.CamliType != "file" {
				Logger.Printf("child not a file: %v", childRef)
				continue
			}
			n.children[name] = &roFile{
				fs:        n.fs,
				permanode: blob.ParseOrZero(childRef),
				parent:    n,
				name:      name,
				content:   blob.ParseOrZero(contentRef),
				size:      content.File.Size,
			}
		} else {
			// unknown type
			continue
		}
		// n.children[name].xattr().load(child.Permanode)
	}
	return nil
}

// Open opens the named file.
// [File.Close] must be called to release any associated resources.
//
// When Open returns an error, it should be of type *PathError
// with the Op field set to "open", the Path field set to name,
// and the Err field describing the problem.
//
// Open should reject attempts to open names that do not satisfy
// ValidPath(name), returning a *PathError with Err set to
// ErrInvalid or ErrNotExist.
func (n *roDir) Open(name string) (ret fs.File, err error) {
	defer func() {
		Logger.Printf("roDir(%q).Lookup(%q) = %#v, %v", n.fullPath(), name, ret, err)
	}()
	ctx, cancel := context.WithTimeout(n.fs.Context(), time.Minute)
	defer cancel()
	if err := n.populate(ctx); err != nil {
		Logger.Println("populate:", err)
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n2 := n.children[name]; n2 != nil {
		if f, ok := n2.(*roFile); ok {
			if f.reader == nil {
				if f.reader, err = schema.NewFileReader(ctx, f.fs.fetcher, f.content); err != nil {
					return nil, &fs.PathError{Op: "open", Path: name, Err: err}
				}
			}
		}

		return n2, nil
	}
	return nil, &fs.PathError{Op: "open", Err: fs.ErrNotExist, Path: name}
}

// roFile is a read-only file, or symlink.
type roFile struct {
	fs        *PkFileSystem
	permanode blob.Ref
	parent    *roDir
	name      string // ent name (base name within parent)

	mu      sync.Mutex // protects all following fields
	reader  *schema.FileReader
	symLink bool     // if true, is a symlink
	target  string   // if a symlink
	content blob.Ref // if a regular file
	size    int64
	mtime   time.Time // if zero, use serverStart
	// atime time.Time
	// xattrs       map[string][]byte
}

var _ fs.File = (*roFile)(nil)

func (n *roFile) Close() error { return nil }

// for debugging
func (n *roFile) fullPath() string {
	if n == nil {
		return ""
	}
	return filepath.Join(n.parent.fullPath(), n.name)
}

func (n *roFile) Stat() (fs.FileInfo, error) {
	var mode os.FileMode = 0400 // read-only

	n.mu.Lock()
	size := n.size
	// inode := n.permanode.Sum64()
	if n.symLink {
		mode |= os.ModeSymlink
	}
	n.mu.Unlock()

	return fileInfo{
		name:    n.name,
		mode:    mode,
		size:    int64(size),
		modTime: n.modTime(),
	}, nil
}

func (n *roFile) Read(p []byte) (int, error) {
	return n.reader.Read(p)
}

type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
}

func (fi fileInfo) Name() string       { return fi.name }
func (fi fileInfo) Size() int64        { return fi.size }
func (fi fileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi fileInfo) ModTime() time.Time { return fi.modTime }
func (fi fileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi fileInfo) Sys() any           { return nil }

// Type returns the type bits for the entry.
// The type bits are a subset of the usual FileMode bits, those returned by the FileMode.Type method.
func (fi fileInfo) Type() fs.FileMode { return fi.mode.Type() }

// Info returns the FileInfo for the file or subdirectory described by the entry.
// The returned FileInfo may be from the time of the original directory read
// or from the time of the call to Info. If the file has been removed or renamed
// since the directory read, Info may return an error satisfying errors.Is(err, ErrNotExist).
// If the entry denotes a symbolic link, Info reports the information about the link itself,
// not the link's target.
func (fi fileInfo) Info() (fs.FileInfo, error) { return fi, nil }

func (n *roFile) modTime() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.mtime.IsZero() {
		return n.mtime
	}
	return serverStart
}
func (n *roFile) Ref() blob.Ref { return n.permanode }

type roFileOrDir interface {
	fs.File
	Ref() blob.Ref
}

func isDir(d *search.DescribedPermanode) bool {
	// Explicit
	if d.Attr.Get("camliNodeType") == "directory" {
		return true
	}
	// Implied
	for k := range d.Attr {
		if strings.HasPrefix(k, "camliPath:") {
			return true
		}
	}
	return false
}
