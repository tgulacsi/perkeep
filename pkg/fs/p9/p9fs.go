/*
Copyright 2025 The Perkeep Authors.

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

// Package p9 implements a 9P filesystem for Perkeep
package p9 // import "perkeep.org/pkg/fs/p9"

import (
	"context"
	"fmt"
	"hash"
	"hash/maphash"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"perkeep.org/internal/lru"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"

	"github.com/hugelgupf/p9/fsimpl/qids"
	"github.com/hugelgupf/p9/fsimpl/templatefs"
	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"
)

var (
	serverStart = time.Now()
	// Logger is used by the package to print all sorts of debugging statements. It
	// is up to the user of the package to SetOutput the Logger to reduce verbosity.
	Logger = log.New(os.Stderr, "PerkeepP9FS: ", log.LstdFlags)
)

const Version uint32 = 1

type pkP9FS struct {
	fetcher blob.Fetcher
	client  *client.Client // or nil, if not doing search queries
	paths   *qids.PathGenerator

	// IgnoreOwners, if true, collapses all file ownership to the
	// uid/gid running the fuse filesystem, and sets all the
	// permissions to 0600/0700.
	IgnoreOwners bool

	blobToSchema *lru.Cache // ~map[blobstring]*schema.Blob
	nameToBlob   *lru.Cache // ~map[string]blob.Ref
	nameToAttr   *lru.Cache // ~map[string]*fuse.Attr
}

var _ p9.Attacher = (*pkP9FS)(nil)

func newPkP9FS(fetcher blob.Fetcher) *pkP9FS {
	return &pkP9FS{
		fetcher:      fetcher,
		blobToSchema: lru.New(1024), // arbitrary; TODO: tunable/smarter?
		nameToBlob:   lru.New(1024), // arbitrary: TODO: tunable/smarter?
		nameToAttr:   lru.New(1024), // arbitrary: TODO: tunable/smarter?
		paths:        &qids.PathGenerator{},
	}
}

// NewDefaultpkP9FS returns a filesystem with a generic base, from which
// users can navigate by blobref, tag, date, etc.
func NewDefaultPkP9FS(client *client.Client, fetcher blob.Fetcher) *pkP9FS {
	if client == nil || fetcher == nil {
		panic("nil argument")
	}
	fs := newPkP9FS(fetcher)
	fs.root = &node{fs: fs} // root.go
	fs.client = client
	return fs
}

// NewRootedpkP9FS returns a pkP9FS with a node based on a blobref
// as its base.
func NewRootedPkP9FS(cli *client.Client, fetcher blob.Fetcher, root blob.Ref) (*pkP9FS, error) {
	fs := newPkP9FS(fetcher)
	fs.client = cli

	n, err := fs.newNodeFromBlobRef(root)

	if err != nil {
		return nil, err
	}

	fs.root = n

	return fs, nil
}

func (fs *pkP9FS) Attach() (p9.File, error) {
	return &node{fs: fs}, nil
}

var _ p9.File = (*node)(nil)

// node implements fuse.Node with a read-only Camli "file" or
// "directory" blob.
type node struct {
	p9.DefaultWalkGetAttr
	// templatefs.ReadOnlyFile
	templatefs.ReadOnlyDir
	templatefs.NotLockable

	fs      *pkP9FS
	blobref blob.Ref

	pnodeModTime time.Time // optionally set by recent.go; modtime of permanode

	dmu     sync.Mutex // guards dirents. acquire before mu.
	dirents []*node    // nil until populated once

	initOnce    sync.Once
	mu          sync.Mutex // guards rest
	attr        p9.Attr
	meta        *schema.Blob
	lookMap     map[string]blob.Ref
	parentInode uint64
	qid         p9.QID
	reader      *schema.FileReader
}

// Walk walks to the path components given in names.
//
// Walk returns QIDs in the same order that the names were passed in.
//
// An empty list of arguments should return a copy of the current file.
//
// On the server, Walk has a read concurrency guarantee.
func (n *node) Walk(names []string) ([]p9.QID, p9.File, error) {
	if err := n.init(); err != nil {
		return nil, nil, err
	}
	if len(names) == 0 {
		return nil, n, nil
	}
	n.dmu.Lock()
	err := n.readDirAll()
	dirents := n.dirents
	n.dmu.Unlock()
	if len(dirents) == 0 {
		return nil, nil, err
	}
	qids := make([]p9.QID, len(n.dirents))
	for i, f := range n.dirents {
		qids[i] = f.qid
	}
	return qids, n.dirents[len(n.dirents)-1], err
}

// StatFS returns information about the file system associated with this file.
//
// On the server, StatFS has no concurrency guarantee.
func (n *node) StatFS() (p9.FSStat, error) {
	return p9.FSStat{
		Type:      0x01021997, /* V9FS_MAGIC */
		BlockSize: 4096,       /* whatever */
		// Make some stuff up, just to see if it makes "lsof" happy.
		Blocks:  1 << 35,
		Bfree:   1 << 34,
		Bavail:  1 << 34,
		Files:   1 << 29,
		Ffree:   1 << 28,
		Namelen: 2048,
	}, nil
}

// GetAttr returns attributes of this node.
//
// On the server, GetAttr has a read concurrency guarantee.
func (n *node) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	err := n.init()
	return n.qid, req, n.attr, err
}

// Open must be called prior to using ReadAt, WriteAt, or Readdir. Once
// Open is called, some operations, such as Walk, will no longer work.
//
// On the client, Open should be called only once. The fd return is
// optional, and may be nil.
//
// On the server, Open has a read concurrency guarantee.  Open is
// guaranteed to be called only once.
//
// N.B. The server must resolve any lazy paths when open is called.
// After this point, read and write may be called on files with no
// deletion check, so resolving in the data path is not viable.
func (n *node) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	if mode.Mode() != p9.ReadOnly {
		return p9.QID{}, 0, linux.EROFS
	}

	if err := n.init(); err != nil {
		return n.qid, 0, err
	}
	Logger.Printf("CAMLI Open on %v: %#v", n.blobref, mode)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.meta.Type() == schema.TypeDirectory {
		return n.qid, 0, nil
	}
	if n.reader != nil {
		return n.qid, 0, nil
	}
	var err error
	if n.reader, err = ss.NewFileReader(n.fs.fetcher); err != nil {
		// Will only happen if ss.Type != "file" or "bytes"
		Logger.Printf("NewFileReader(%s) = %v", n.blobref, err)
	}
	return n.qid, 0, err
}

// ReadAt reads from this file. Open must be called first.
//
// This may return io.EOF in addition to linux.Errno values.
//
// On the server, ReadAt has a read concurrency guarantee. See Open for
// additional requirements regarding lazy path resolution.
func (n *node) ReadAt(p []byte, offset int64) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reader.ReadAt(p, offset)
}

// Readdir reads directory entries.
//
// offset is the entry offset, and count the number of entries to
// return.
//
// This may return io.EOF in addition to linux.Errno values.
//
// On the server, Readdir has a read concurrency guarantee.
func (n *node) Readdir(offset uint64, count uint32) (p9.Dirents, error) {

}

// Readlink reads the link target.
//
// On the server, Readlink has a read concurrency guarantee.
func (n *node) Readlink() (string, error) { return "", nil }

// Renamed is called when this node is renamed.
//
// This may not fail. The file will hold a reference to its parent
// within the p9 package, and is therefore safe to use for the lifetime
// of this File (until Close is called).
//
// This method should not be called by clients, who should use the
// relevant Rename methods. (Although the method will be a no-op.)
//
// On the server, Renamed has a global concurrency guarantee.
func (n *node) Renamed(newDir File, newName string) {}

func (n *node) readDirAll(ctx context.Context) error {
	Logger.Printf("CAMLI ReadDirAll on %v", n.blobref)
	if n.dirents != nil {
		return nil
	}
	if err := n.initCtx(ctx); err != nil {
		return err
	}

	ss := n.meta
	dr, err := schema.NewDirReader(ctx, n.fs.fetcher, ss.BlobRef())
	if err != nil {
		Logger.Printf("camli.ReadDirAll error on %v: %v", n.blobref, err)
		return handleEIOorEINTR(err)
	}
	schemaEnts, err := dr.Readdir(ctx, -1)
	if err != nil {
		Logger.Printf("camli.ReadDirAll error on %v: %v", n.blobref, err)
		return handleEIOorEINTR(err)
	}
	n.dirents = make([]*node, 0, len(schemaEnts))
	if n.lookMap == nil {
		n.lookMap = make(map[string]blob.Ref)
	}
	for _, sent := range schemaEnts {
		if name := sent.FileName(); name != "" {
			n.lookMap[name] = sent.BlobRef()
			c, err := n.lookup(name)
			if err != nil {
				return err
			}
			n.dirents = append(n.dirents, c)
		}
	}
	return nil
}

func (n *node) init() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return n.initCtx(ctx)
}

func (n *node) initCtx(ctx context.Context) error {
	var err error
	n.initOnce.Do(func() {
		if n.meta != nil {
			return nil
		}
		var blob *schema.Blob
		if blob, err = n.fs.fetchSchemaMeta(ctx, n.blobref); err == nil {
			n.meta = blob
			n.populateAttr()
		}
	})
	return err
}

// FSync implements p9.File.FSync.
func (n *node) FSync() error { return nil }

// Close implements p9.File.Close.
func (n *node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.reader != nil {
		n.reader.Close()
	}
	return nil
}

func (n *node) lookup(name string) (*node, error) {
	if name == ".quitquitquit" {
		// TODO: only in dev mode
		log.Fatalf("Shutting down due to .quitquitquit lookup.")
	}

	// If we haven't done Readdir yet (dirents isn't set), then force a Readdir
	// call to populate lookMap.
	n.dmu.Lock()
	loaded := n.dirents != nil
	n.dmu.Unlock()
	if !loaded {
		n.readDirAll()
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	ref, ok := n.lookMap[name]
	if !ok {
		return nil, fuse.ENOENT
	}
	return &node{fs: n.fs, blobref: ref, parentInode: n.qid.Path}, nil
}

// populateAttr should only be called once n.ss is known to be set and non-nil
func (n *node) populateAttr() error {
	meta := n.meta

	n.attr.Mode = meta.FileMode()

	if n.fs.IgnoreOwners {
		n.attr.Uid = uint32(os.Getuid())
		n.attr.Gid = uint32(os.Getgid())
		executeBit := n.attr.Mode & 0100
		n.attr.Mode = (n.attr.Mode ^ n.attr.Mode.Perm()) | 0400 | executeBit
	} else {
		n.attr.Uid = uint32(meta.MapUid())
		n.attr.Gid = uint32(meta.MapGid())
	}

	if mt := meta.ModTime(); !mt.IsZero() {
		n.attr.Mtime = mt
	} else {
		n.attr.Mtime = n.pnodeModTime
	}

	switch meta.Type() {
	case schema.TypeFile:
		n.attr.Size = uint64(meta.PartsSize())
		n.attr.Blocks = 0 // TODO: set?
		n.attr.Mode |= 0400
	case schema.TypeDirectory:
		n.attr.Mode |= 0500
	case schema.TypeSymlink:
		n.attr.Mode |= 0400
	default:
		Logger.Printf("unknown attr ss.Type %q in populateAttr", meta.Type())
	}

	n.qid = p9.QID{
		Type:    n.attr.Mode & p9.FileModeMask,
		Version: Version,
		Path:    mkInode(n.parentInode, meta.FileName()),
	}

	return nil
}

var inodeHashPool = sync.Pool{New: func() any { return new(maphash.Hash) }}

func mkInode(parent uint64, name string) uint64 {
	hsh := inodeHashPool.Get().(*maphash.Hash)
	hsh.Reset()
	binary.Write(hsh, binary.BigEndian, parent)
	hsh.WriteString(name)
	u := hsh.Sum64()
	inodeHashPool.Put(hsh)
	return u
}

// Errors returned are:
//
//	os.ErrNotExist -- blob not found
//	os.ErrInvalid -- not JSON or a camli schema blob
func (fs *pkP9FS) fetchSchemaMeta(ctx context.Context, br blob.Ref) (*schema.Blob, error) {
	blobStr := br.String()
	if blob, ok := fs.blobToSchema.Get(blobStr); ok {
		return blob.(*schema.Blob), nil
	}

	rc, _, err := fs.fetcher.Fetch(ctx, br)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	blob, err := schema.BlobFromReader(br, rc)
	if err != nil {
		Logger.Printf("Error parsing %s as schema blob: %v", br, err)
		return nil, os.ErrInvalid
	}
	if blob.Type() == "" {
		Logger.Printf("blob %s is JSON but lacks camliType", br)
		return nil, os.ErrInvalid
	}
	fs.blobToSchema.Add(blobStr, blob)
	return blob, nil
}

// consolated logic for determining a node to mount based on an arbitrary blobref
func (fs *pkP9FS) newNodeFromBlobRef(root blob.Ref) (fusefs.Node, error) {
	blob, err := fs.fetchSchemaMeta(context.TODO(), root)
	if err != nil {
		return nil, err
	}

	switch blob.Type() {
	case schema.TypeDirectory:
		n := &node{fs: fs, blobref: root, meta: blob}
		n.populateAttr()
		return n, nil

	case schema.TypePermanode:
		// other mutDirs listed in the default filesystem have names and are displayed
		return &mutDir{fs: fs, permanode: root, name: "-", children: make(map[string]mutFileOrDir)}, nil
	}

	return nil, fmt.Errorf("Blobref must be of a directory or permanode got a %v", blob.Type())
}

type notImplementDirNode struct{}

var _ fusefs.Node = (*notImplementDirNode)(nil)

func (notImplementDirNode) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0000
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	return nil
}

type staticFileNode string

var _ fusefs.Node = (*notImplementDirNode)(nil)

func (s staticFileNode) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0400
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	a.Size = uint64(len(s))
	a.Mtime = serverStart
	a.Ctime = serverStart
	return nil
}

var _ fusefs.HandleReader = (*staticFileNode)(nil)

func (s staticFileNode) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	if req.Offset > int64(len(s)) {
		return nil
	}
	s = s[req.Offset:]
	size := req.Size
	if size > len(s) {
		size = len(s)
	}
	res.Data = make([]byte, size)
	copy(res.Data, s)
	return nil
}
