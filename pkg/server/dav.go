/*
Copyright 2021 The Perkeep Authors

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

package server

import (
	"context"
	"fmt"
	"io"
	stdfs "io/fs"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"path"
	"strings"
	"time"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"
	"golang.org/x/net/webdav"

	"go4.org/jsonconfig"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/fs"
)

func init() {
	blobserver.RegisterHandlerConstructor("dav", davFromConfig)
}

type DAVHandler struct {
	*webdav.Handler
}

func davFromConfig(ld blobserver.Loader, conf jsonconfig.Obj) (h http.Handler, err error) {
	sto, err := ld.GetStorage("/bs/")
	if err != nil {
		return nil, err
	}
	dav := &DAVHandler{
		Handler: &webdav.Handler{
			Prefix: ld.MyPrefix(),
			FileSystem: &davFS{
				CamliFileSystem: fs.NewDefaultCamliFileSystem(client.NewOrFail(), sto),
			},
			LockSystem: webdav.NewMemLS(),
			Logger: func(req *http.Request, err error) {
				if err != nil {
					log.Println("DAV ERROR:", err, "request:", req)
				}
			},
		},
	}
	log.Println("Prefix:", dav.Handler.Prefix)
	if err = conf.Validate(); err != nil {
		return nil, err
	}

	return dav, nil
}
func (d *DAVHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := httputil.DumpRequest(r, true)
	log.Println(string(b))
	d.Handler.ServeHTTP(w, r)
}

type davFS struct {
	blobserver.Storage
	*fs.CamliFileSystem
}

var _ webdav.FileSystem = (*davFS)(nil)

func (d *davFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return os.ErrPermission
}
func (d *davFS) RemoveAll(ctx context.Context, name string) error {
	return os.ErrPermission
}
func (d *davFS) Rename(ctx context.Context, oldName, newName string) error {
	return os.ErrPermission
}
func (d *davFS) lookup(ctx context.Context, name string) (fusefs.Node, error) {
	log.Println("lookup", name)
	n, err := d.CamliFileSystem.Root()
	if err != nil {
		return nil, err
	}
	nl := n.(fusefs.NodeStringLookuper)
	for _, part := range strings.Split(strings.TrimPrefix(strings.TrimSuffix(name, "/"), "/"), "/") {
		if part == "" {
			continue
		}
		sn, err := nl.Lookup(ctx, part)
		if err != nil {
			return nil, err
		}
		var ok bool
		if nl, ok = sn.(fusefs.NodeStringLookuper); !ok {
			if n, ok := sn.(fusefs.Node); ok {
				return n, nil
			}
			break
		}
	}
	return nl.(fusefs.Node), nil
}
func (d *davFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	n, err := d.lookup(ctx, name)
	log.Println("OpenFile", name, n, err)
	if err != nil {
		return nil, err
	}
	var resp fuse.OpenResponse
	if op, ok := n.(fusefs.NodeOpener); ok {
		h, err := op.Open(ctx, &fuse.OpenRequest{Flags: fuse.OpenFlags(os.FileMode(flag) | perm)}, &resp)
		if err != nil {
			return nil, err
		}
		if h == op { // directory
			return &davFile{name: path.Base(name), n: h.(fusefs.Node)}, nil
		}
		// nodeReader or n itself
		return &davFile{name: path.Base(name), n: h.(fusefs.Node)}, nil
	}
	return &davFile{name: path.Base(name), n: n.(fusefs.Node)}, nil
}
func (d *davFS) Stat(ctx context.Context, name string) (stdfs.FileInfo, error) {
	n, err := d.lookup(ctx, name)
	log.Println("Stat", name, n, err)
	if err != nil {
		return nil, err
	}
	var a fuse.Attr
	err = n.Attr(ctx, &a)
	da := davAttr{name: name, Attr: a}
	log.Printf("Stat(%q): %v dir=%t (err=%+v)", name, da, da.IsDir(), err)
	return da, err
}

type davFile struct {
	n      fusefs.Node
	name   string
	offset int64
}

func (d *davFile) Readdir(count int) ([]stdfs.FileInfo, error) {
	log.Println("Readdir", d.name)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var ents []fuse.Dirent
	var err error
	if rd, ok := d.n.(interface {
		ReadDir(context.Context) ([]fuse.Dirent, error)
	}); ok {
		ents, err = rd.ReadDir(ctx)
	} else if rd, ok := d.n.(fusefs.HandleReadDirAller); ok {
		ents, err = rd.ReadDirAll(ctx)
	} else {
		log.Printf("Non-readdir %T", d.n)
		return nil, nil
	}
	fis := make([]stdfs.FileInfo, len(ents))
	for i, ent := range ents {
		n, err := d.n.(fusefs.NodeStringLookuper).Lookup(ctx, path.Join(d.name, ent.Name))
		if err != nil {
			return fis, err
		}
		var a fuse.Attr
		if err = n.Attr(ctx, &a); err != nil {
			return fis, err
		}
		fis[i] = davAttr{name: ent.Name, Attr: a}
	}
	log.Printf("Readdir(%q): %v (err=%+v)", d.name, fis, err)
	return fis, err
}
func (d *davFile) Stat() (stdfs.FileInfo, error) {
	var a fuse.Attr
	err := d.n.(fusefs.Node).Attr(context.Background(), &a)
	return davAttr{name: d.name, Attr: a}, err
}
func (d *davFile) Read(p []byte) (int, error) {
	log.Println("Read", d.name)
	var resp fuse.ReadResponse
	err := d.n.(fusefs.HandleReader).Read(context.Background(), &fuse.ReadRequest{Offset: d.offset, Size: len(p)}, &resp)
	n := copy(p, resp.Data)
	d.offset += int64(n)
	return n, err
}

func (d *davFile) Write(_ []byte) (int, error) { return 0, os.ErrPermission }
func (d *davFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		d.offset = offset
	case io.SeekCurrent:
		d.offset += offset
	case io.SeekEnd:
		fi, err := d.Stat()
		if err != nil {
			return d.offset, err
		}
		d.offset = fi.Size() - offset
	}
	return d.offset, nil
}
func (d *davFile) Close() error {
	n := d.n
	d.n = nil
	if n != nil {
		if r, ok := n.(fusefs.HandleReleaser); ok {
			return r.Release(context.Background(), nil)
		}
	}
	return nil
}

type davAttr struct {
	name string
	fuse.Attr
}

func (d davAttr) IsDir() bool        { return d.Attr.Mode.IsDir() }
func (d davAttr) ModTime() time.Time { return d.Attr.Mtime }
func (d davAttr) Mode() os.FileMode  { return d.Attr.Mode }
func (d davAttr) Name() string       { return d.name }
func (d davAttr) Size() int64        { return int64(d.Attr.Size) }
func (d davAttr) Sys() interface{}   { return nil }
func (d davAttr) String() string     { return fmt.Sprintf("%q\t%t\t%d\t%s", d.name, d.IsDir(), d.Mode()) }
