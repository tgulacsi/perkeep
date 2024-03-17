//go:build linux

/*
Copyright 2011 The Perkeep Authors

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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/buildinfo"
	"perkeep.org/pkg/cacher"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/cmdmain"
	"perkeep.org/pkg/fs"
	"perkeep.org/pkg/search"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"
)

var (
	debug = flag.Bool("debug", false, "print debugging messages.")
	xterm = flag.Bool("xterm", false, "Run an xterm in the mounted directory. Shut down when xterm ends.")
)

func usage() {
	fmt.Fprint(os.Stderr, "usage: pk-mount [opts] [<mountpoint> [<root-blobref>|<share URL>|<root-name>]]\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func init() {
	// So we can simply use log.Printf and log.Fatalf.
	// For logging that depends on verbosity (cmdmain.FlagVerbose), use cmdmain.Logf/Printf.
	log.SetOutput(cmdmain.Stderr)
}

var ctxbg = context.Background()

func main() {
	var conn *fuse.Conn

	// Scans the arg list and sets up flags
	client.AddFlags()
	flag.Usage = usage
	flagRO := flag.Bool("read-only", true, "mount read-only")
	flag.Parse()

	if *cmdmain.FlagLegal {
		cmdmain.PrintLicenses()
		return
	}
	if *cmdmain.FlagVersion {
		fmt.Fprintf(cmdmain.Stderr, "%s version: %s\n", os.Args[0], buildinfo.Summary())
		return
	}
	if *cmdmain.FlagHelp {
		usage()
	}

	narg := flag.NArg()
	if narg > 2 {
		usage()
	}

	var mountPoint string
	var err error
	if narg > 0 {
		mountPoint = flag.Arg(0)
	} else {
		if fi, err := os.Stat("/pk"); err == nil && fi.IsDir() {
			log.Printf("no mount point given; using /pk")
			mountPoint = "/pk"
		} else {
			mountPoint, err = os.MkdirTemp("", "pk-mount")
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("no mount point given and recommended directory /pk doesn't exist; using temp directory %s", mountPoint)
			defer os.Remove(mountPoint)
		}
	}

	errorf := func(msg string, args ...interface{}) {
		fmt.Fprintf(os.Stderr, msg, args...)
		fmt.Fprint(os.Stderr, "\n")
		usage()
	}

	var (
		cl    *client.Client
		root  blob.Ref // nil if only one arg
		camfs *fs.CamliFileSystem
	)
	if narg == 2 {
		rootArg := flag.Arg(1)
		// not trying very hard since NewFromShareRoot will do it better with a regex
		if strings.HasPrefix(rootArg, "http://") ||
			strings.HasPrefix(rootArg, "https://") {
			if client.ExplicitServer() != "" {
				errorf("Can't use an explicit blobserver with a share URL; the blobserver is implicit from the share URL.")
			}
			var err error
			cl, root, err = client.NewFromShareRoot(ctxbg, rootArg)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			cl = client.NewOrFail() // automatic from flags

			var ok bool
			root, ok = blob.Parse(rootArg)

			if !ok {
				// not a blobref, check for root name instead
				req := &search.WithAttrRequest{N: 1, Attr: "camliRoot", Value: rootArg}
				wres, err := cl.GetPermanodesWithAttr(ctxbg, req)

				if err != nil {
					log.Fatal("could not query search")
				}

				if wres.WithAttr != nil {
					root = wres.WithAttr[0].Permanode
				} else {
					log.Fatalf("root specified is not a blobref or name of a root: %q\n", rootArg)
				}
			}
		}
	} else {
		cl = client.NewOrFail() // automatic from flags
	}

	diskCacheFetcher, err := cacher.NewDiskCache(cl)
	if err != nil {
		log.Fatalf("Error setting up local disk cache: %v", err)
	}
	defer diskCacheFetcher.Clean()
	if root.Valid() {
		var err error
		camfs, err = fs.NewRootedCamliFileSystem(cl, diskCacheFetcher, root)
		if err != nil {
			log.Fatalf("Error creating root with %v: %v", root, err)
		}
	} else {
		camfs = fs.NewDefaultCamliFileSystem(cl, diskCacheFetcher)
	}

	if *debug {
		fuse.Debug = func(msg interface{}) { log.Print(msg) }
	} else {
		fs.Logger.SetOutput(io.Discard)
	}

	sigc := make(chan os.Signal, 1)

	opts := []fuse.MountOption{
		fuse.FSName("perkeep"), fuse.CacheSymlinks(), fuse.ReadOnly(),
	}
	if !*flagRO {
		opts = opts[:len(opts)-1]
	}
	conn, err = fuse.Mount(mountPoint, opts...)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}

	xtermDone := make(chan bool, 1)
	if *xterm {
		cmd := exec.Command("xterm")
		cmd.Dir = mountPoint
		if err := cmd.Start(); err != nil {
			log.Printf("Error starting xterm: %v", err)
		} else {
			go func() {
				cmd.Wait()
				xtermDone <- true
			}()
			defer cmd.Process.Kill()
		}
	}

	signal.Notify(sigc, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	doneServe := make(chan error, 1)
	go func() {
		doneServe <- fusefs.Serve(conn, camfs)
	}()

	quitKey := make(chan bool, 1)
	go awaitQuitKey(quitKey)

	select {
	case err := <-doneServe:
		log.Printf("conn.Serve returned %v", err)
	case sig := <-sigc:
		log.Printf("Signal %s received, shutting down.", sig)
	case <-quitKey:
		log.Printf("Quit key pressed. Shutting down.")
	case <-xtermDone:
		log.Printf("xterm done")
	}
	time.AfterFunc(2*time.Second, func() {
		log.Printf("shutdown timer elapsed; calling os.Exit(1)")
		os.Exit(1)
	})

	log.Printf("Unmounting...")
	err = fs.Unmount(mountPoint)
	log.Printf("Unmount = %v", err)

	log.Printf("pk-mount FUSE process ending.")
}

func awaitQuitKey(done chan<- bool) {
	var buf [1]byte
	for {
		_, err := os.Stdin.Read(buf[:])
		if err != nil {
			return
		}
		if buf[0] == 'q' {
			if *debug {
				stacks := make([]byte, 1<<20)
				stacks = stacks[:runtime.Stack(stacks, true)]
				os.Stderr.Write(stacks)
			}
			done <- true
			return
		}
	}
}
