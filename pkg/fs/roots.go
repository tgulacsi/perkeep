/*
Copyright 2012 The Perkeep Authors

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
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"go4.org/syncutil"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/search"
)

const refreshTime = 1 * time.Minute

type rootsDir struct {
	fs *PkFileSystem
	at time.Time

	mu        sync.Mutex // guards following
	lastQuery time.Time
	m         map[string]blob.Ref // ent name => permanode
	children  map[string]fs.File  // ent name => child node
}

var (
	_ fs.FS     = (*rootsDir)(nil)
	_ fs.StatFS = (*rootsDir)(nil)
)

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
func (n *rootsDir) Open(name string) (fs.File, error) {
	Logger.Printf("fs.roots: Lookup(%q)", name)
	n.mu.Lock()
	defer n.mu.Unlock()
	ctx, cancel := context.WithTimeout(n.fs.Context(), time.Minute)
	defer cancel()
	if err := n.condRefresh(ctx); err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	br := n.m[name]
	if !br.Valid() {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}

	nod, ok := n.children[name]
	if ok {
		return nod, nil
	}

	nod = newRODir(n.fs, br, name, n.at)
	n.children[name] = nod

	return nod, nil
}

// Stat returns a FileInfo describing the file.
// If there is an error, it should be of type *PathError.
func (n *rootsDir) Stat(name string) (fs.FileInfo, error) {
	return n.children[name]
}

func (n *rootsDir) dirMode() os.FileMode { return 0500 }

// requires n.mu is held
func (n *rootsDir) condRefresh(ctx context.Context) error {
	if n.lastQuery.After(time.Now().Add(-refreshTime)) {
		return nil
	}
	Logger.Printf("fs.roots: querying")

	var rootRes, impRes *search.WithAttrResponse
	var grp syncutil.Group
	grp.Go(func() (err error) {
		// TODO(mpl): use a search query instead.
		rootRes, err = n.fs.client.GetPermanodesWithAttr(ctx, &search.WithAttrRequest{N: 100, Attr: "camliRoot", At: n.at})
		return
	})
	grp.Go(func() (err error) {
		impRes, err = n.fs.client.GetPermanodesWithAttr(ctx, &search.WithAttrRequest{N: 100, Attr: "camliImportRoot", At: n.at})
		return
	})
	if err := grp.Err(); err != nil {
		Logger.Printf("fs.roots: error refreshing permanodes: %v", err)
		return err
	}

	n.m = make(map[string]blob.Ref)
	if n.children == nil {
		n.children = make(map[string]fs.File)
	}

	dr := &search.DescribeRequest{
		Depth: 1,
	}
	for _, wi := range rootRes.WithAttr {
		dr.BlobRefs = append(dr.BlobRefs, wi.Permanode)
	}
	for _, wi := range impRes.WithAttr {
		dr.BlobRefs = append(dr.BlobRefs, wi.Permanode)
	}
	if len(dr.BlobRefs) == 0 {
		return nil
	}

	dres, err := n.fs.client.Describe(ctx, dr)
	if err != nil {
		Logger.Printf("Describe failure: %v", err)
		return err
	}

	// Roots
	currentRoots := map[string]bool{}
	for _, wi := range rootRes.WithAttr {
		pn := wi.Permanode
		db := dres.Meta[pn.String()]
		if db != nil && db.Permanode != nil {
			name := db.Permanode.Attr.Get("camliRoot")
			if name != "" {
				currentRoots[name] = true
				n.m[name] = pn
			}
		}
	}

	// Remove any children objects we have mapped that are no
	// longer relevant.
	for name := range n.children {
		if !currentRoots[name] {
			delete(n.children, name)
		}
	}

	// Importers (mapped as roots for now)
	for _, wi := range impRes.WithAttr {
		pn := wi.Permanode
		db := dres.Meta[pn.String()]
		if db != nil && db.Permanode != nil {
			name := db.Permanode.Attr.Get("camliImportRoot")
			if name != "" {
				name = strings.Replace(name, ":", "-", -1)
				name = strings.Replace(name, "/", "-", -1)
				n.m["importer-"+name] = pn
			}
		}
	}

	n.lastQuery = time.Now()
	return nil
}
