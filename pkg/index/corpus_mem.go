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

package index

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"log"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"perkeep.org/internal/osutil"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/schema/nodeattr"
	"perkeep.org/pkg/sorted"
	"perkeep.org/pkg/types/camtypes"

	"go4.org/strutil"
	"go4.org/syncutil"
)

var _ Corpus = (*corpusMem)(nil)

func newMemCorpusFromStorage(s sorted.KeyValue) (*corpusMem, error) {
	c := newCorpus()
	return c, c.scanFromStorage(s)
}

// corpusMem is the in-memory implementation of Corpus. It is an in-memory
// summary of all of a user's blobs' metadata.
type corpusMem struct {
	// building is true at start while scanning all rows in the
	// index.  While building, certain invariants (like things
	// being sorted) can be temporarily violated and fixed at the
	// end of scan.
	building bool

	// hasLegacySHA1 reports whether some SHA-1 blobs are indexed. It is set while
	//building the corpus from the initial index scan.
	hasLegacySHA1 bool

	// gen is incremented on every blob received.
	// It's used as a query cache invalidator.
	gen int64

	strs      map[string]string   // interned strings
	brOfStr   map[string]blob.Ref // blob.Parse fast path
	brInterns int64               // blob.Ref -> blob.Ref, via br method

	blobs        map[blob.Ref]*camtypes.BlobMeta
	sumBlobBytes int64

	// camBlobs maps from camliType ("file") to blobref to the meta.
	// The value is the same one in blobs.
	camBlobs map[schema.CamliType]map[blob.Ref]*camtypes.BlobMeta

	// TODO: add GoLLRB to vendor; keep sorted BlobMeta
	keyId signerFromBlobrefMap

	// signerRefs maps a signer GPG ID to all its signer blobs (because different hashes).
	signerRefs   map[string]SignerRefSet
	files        map[blob.Ref]camtypes.FileInfo // keyed by file or directory schema blob
	permanodes   map[blob.Ref]*PermanodeMeta
	imageInfo    map[blob.Ref]camtypes.ImageInfo // keyed by fileref (not wholeref)
	fileWholeRef map[blob.Ref]blob.Ref           // fileref -> its wholeref (TODO: multi-valued?)
	gps          map[blob.Ref]latLong            // wholeRef -> GPS coordinates
	// dirChildren maps a directory to its (direct) children (static-set entries).
	dirChildren map[blob.Ref]map[blob.Ref]struct{}
	// fileParents maps a file or directory to its (direct) parents.
	fileParents map[blob.Ref]map[blob.Ref]struct{}

	// Lack of edge tracking implementation is issue #707
	// (https://github.com/perkeep/perkeep/issues/707)

	// claimBack allows hopping backwards from a Claim's Value
	// when the Value is a blobref.  It allows, for example,
	// finding the parents of camliMember claims.  If a permanode
	// parent set A has a camliMembers B and C, it allows finding
	// A from either B and C.
	// The slice is not sorted.
	claimBack map[blob.Ref][]*camtypes.Claim

	// TODO: use deletedCache instead?
	deletedBy map[blob.Ref]blob.Ref // key is deleted by value
	// deletes tracks deletions of claims and permanodes. The key is
	// the blobref of a claim or permanode. The values, sorted newest first,
	// contain the blobref of the claim responsible for the deletion, as well
	// as the date when that deletion happened.
	deletes map[blob.Ref][]deletion

	mediaTags map[blob.Ref]map[string]string // wholeref -> "album" -> "foo"

	permanodesByTime    *lazySortedPermanodes // cache of permanodes sorted by creation time.
	permanodesByModtime *lazySortedPermanodes // cache of permanodes sorted by modtime.

	// permanodesSetByNodeType maps from a camliNodeType attribute
	// value to the set of permanodes that ever had that
	// value. The bool is always true.
	permanodesSetByNodeType map[string]map[blob.Ref]bool

	// scratch string slice
	ss []string
}

func (c *corpusMem) logf(format string, args ...any) {
	log.Printf("index/corpus: "+format, args...)
}

// blobMatches reports whether br is in the set.
func (srs SignerRefSet) blobMatches(br blob.Ref) bool {
	return slices.ContainsFunc(srs, br.EqualString)
}

// signerFromBlobrefMap maps a signer blobRef to the signer's GPG ID (e.g.
// 2931A67C26F5ABDA). It is needed because the signer on a claim is represented by
// its blobRef, but the same signer could have created claims with different hashes
// (e.g. with sha1 and with sha224), so these claims would look as if created by
// different signers (because different blobRefs). signerID thus allows the
// algorithms to rely on the unique GPG ID of a signer instead of the different
// blobRef representations of it. Its value is usually the corpus keyId.
type signerFromBlobrefMap map[blob.Ref]string

type latLong struct {
	lat, long float64
}

// IsDeleted reports whether the provided blobref (of a permanode or claim) should be considered deleted.
func (c *corpusMem) IsDeleted(br blob.Ref) bool {
	for _, v := range c.deletes[br] {
		if !c.IsDeleted(v.deleter) {
			return true
		}
	}
	return false
}

// HasLegacySHA1 reports whether some SHA-1 blobs are indexed.
func (c *corpusMem) HasLegacySHA1() bool {
	return c.hasLegacySHA1
}

// SignerRefs returns the set of blobRefs that represent the same signer
// GPG identity as the given keyID (e.g. "2931A67C26F5ABDA").
func (c *corpusMem) SignerRefs(keyID string) SignerRefSet {
	return c.signerRefs[keyID]
}

func (c *corpusMem) Generation() int64 { return c.gen }

// *********** Updating the corpus

var corpusMergeFunc = map[string]func(c *corpusMem, k, v []byte) error{
	"have":                 nil, // redundant with "meta"
	"recpn":                nil, // unneeded.
	"meta":                 (*corpusMem).mergeMetaRow,
	keySignerKeyID.name:    (*corpusMem).mergeSignerKeyIdRow,
	"claim":                (*corpusMem).mergeClaimRow,
	"fileinfo":             (*corpusMem).mergeFileInfoRow,
	keyFileTimes.name:      (*corpusMem).mergeFileTimesRow,
	"imagesize":            (*corpusMem).mergeImageSizeRow,
	"wholetofile":          (*corpusMem).mergeWholeToFileRow,
	"exifgps":              (*corpusMem).mergeEXIFGPSRow,
	"exiftag":              nil, // not using any for now
	"signerattrvalue":      nil, // ignoring for now
	"mediatag":             (*corpusMem).mergeMediaTag,
	keyStaticDirChild.name: (*corpusMem).mergeStaticDirChildRow,
}

func (c *corpusMem) scanFromStorage(s sorted.KeyValue) error {
	c.building = true

	var ms0 *runtime.MemStats
	if logCorpusStats {
		ms0 = memstats()
		c.logf("loading into memory...")
		c.logf("loading into memory... (1/%d: meta rows)", len(slurpPrefixes))
	}

	scanmu := new(sync.Mutex)

	// We do the "meta" rows first, before the prefixes below, because it
	// populates the blobs map (used for blobref interning) and the camBlobs
	// map (used for hinting the size of other maps)
	if err := c.scanPrefix(scanmu, s, "meta:"); err != nil {
		return err
	}

	// we do the keyIDs first, because they're necessary to properly merge claims
	if err := c.scanPrefix(scanmu, s, keySignerKeyID.name+":"); err != nil {
		return err
	}

	c.files = make(map[blob.Ref]camtypes.FileInfo, len(c.camBlobs[schema.TypeFile]))
	c.permanodes = make(map[blob.Ref]*PermanodeMeta, len(c.camBlobs[schema.TypePermanode]))
	cpu0 := osutil.CPUUsage()

	var grp syncutil.Group
	for i, prefix := range slurpPrefixes[2:] {
		if logCorpusStats {
			c.logf("loading into memory... (%d/%d: prefix %q)", i+2, len(slurpPrefixes),
				prefix[:len(prefix)-1])
		}
		prefix := prefix
		grp.Go(func() error { return c.scanPrefix(scanmu, s, prefix) })
	}
	if err := grp.Err(); err != nil {
		return err
	}

	// Post-load optimizations and restoration of invariants.
	for _, pm := range c.permanodes {
		// Restore invariants violated during building:
		if err := pm.restoreInvariants(c.keyId); err != nil {
			return err
		}

		// And intern some stuff.
		for _, cl := range pm.Claims {
			cl.BlobRef = c.br(cl.BlobRef)
			cl.Signer = c.br(cl.Signer)
			cl.Permanode = c.br(cl.Permanode)
			cl.Target = c.br(cl.Target)
		}
	}
	c.brOfStr = nil // drop this now.
	c.building = false
	// log.V(1).Printf("interned blob.Ref = %d", c.brInterns)

	if err := c.initDeletes(s); err != nil {
		return fmt.Errorf("Could not populate the corpus deletes: %w", err)
	}

	if logCorpusStats {
		cpu := osutil.CPUUsage() - cpu0
		ms1 := memstats()
		memUsed := ms1.Alloc - ms0.Alloc
		if ms1.Alloc < ms0.Alloc {
			memUsed = 0
		}
		c.logf("stats: %.3f MiB mem: %d blobs (%.3f GiB) (%d schema (%d permanode, %d file (%d image), ...)",
			float64(memUsed)/(1<<20),
			len(c.blobs),
			float64(c.sumBlobBytes)/(1<<30),
			c.numSchemaBlobs(),
			len(c.permanodes),
			len(c.files),
			len(c.imageInfo))
		c.logf("scanning CPU usage: %v", cpu)
	}

	return nil
}

// initDeletes populates the corpus deletes from the delete entries in s.
func (c *corpusMem) initDeletes(s sorted.KeyValue) (err error) {
	it := queryPrefix(s, keyDeleted)
	defer closeIterator(it, &err)
	for it.Next() {
		cl, ok := kvDeleted(it.Key())
		if !ok {
			return fmt.Errorf("Bogus keyDeleted entry key: want |\"deleted\"|<deleted blobref>|<reverse claimdate>|<deleter claim>|, got %q", it.Key())
		}
		targetDeletions := append(c.deletes[cl.Target],
			deletion{
				deleter: cl.BlobRef,
				when:    cl.Date,
			})
		sort.Sort(sort.Reverse(byDeletionDate(targetDeletions)))
		c.deletes[cl.Target] = targetDeletions
	}
	return err
}

func (c *corpusMem) numSchemaBlobs() (n int64) {
	for _, m := range c.camBlobs {
		n += int64(len(m))
	}
	return
}

func (c *corpusMem) scanPrefix(mu *sync.Mutex, s sorted.KeyValue, prefix string) (err error) {
	typeKey := typeOfKey(prefix)
	fn, ok := corpusMergeFunc[typeKey]
	if !ok {
		panic("No registered merge func for prefix " + prefix)
	}

	n, t0 := 0, time.Now()
	it := queryPrefixString(s, prefix)
	defer closeIterator(it, &err)
	for it.Next() {
		n++
		if n == 1 {
			mu.Lock()
			defer mu.Unlock()
		}
		if typeKey == keySignerKeyID.name {
			signerBlobRef, ok := blob.Parse(strings.TrimPrefix(it.Key(), keySignerKeyID.name+":"))
			if !ok {
				c.logf("WARNING: bogus signer blob in %v row: %q", keySignerKeyID.name, it.Key())
				continue
			}
			if err := c.addKeyID(&mutationMap{
				signerBlobRef: signerBlobRef,
				signerID:      it.Value(),
			}); err != nil {
				return err
			}
		} else {
			if err := fn(c, it.KeyBytes(), it.ValueBytes()); err != nil {
				return err
			}
		}
	}
	if logCorpusStats {
		d := time.Since(t0)
		c.logf("loaded prefix %q: %d rows, %v", prefix[:len(prefix)-1], n, d)
	}
	return nil
}

func (c *corpusMem) addKeyID(mm *mutationMap) error {
	if mm.signerID == "" || !mm.signerBlobRef.Valid() {
		return nil
	}
	id, ok := c.keyId[mm.signerBlobRef]
	// only add it if we don't already have it, to save on allocs.
	if ok {
		if id != mm.signerID {
			return fmt.Errorf("GPG ID mismatch for signer %q: refusing to overwrite %v with %v", mm.signerBlobRef, id, mm.signerID)
		}
		return nil
	}
	c.signerRefs[mm.signerID] = append(c.signerRefs[mm.signerID], mm.signerBlobRef.String())
	return c.mergeSignerKeyIdRow([]byte("signerkeyid:"+mm.signerBlobRef.String()), []byte(mm.signerID))
}

func (c *corpusMem) AddBlob(ctx context.Context, br blob.Ref, mm *mutationMap) error {
	if _, dup := c.blobs[br]; dup {
		return nil
	}
	c.gen++
	// make sure keySignerKeyID is done first before the actual mutations, even
	// though it's also going to be done in the loop below.
	if err := c.addKeyID(mm); err != nil {
		return err
	}
	for k, v := range mm.kv {
		kt := typeOfKey(k)
		if kt == keySignerKeyID.name {
			// because we already took care of it in addKeyID
			continue
		}
		if !slurpedKeyType[kt] {
			continue
		}
		if err := corpusMergeFunc[kt](c, []byte(k), []byte(v)); err != nil {
			return err
		}
	}
	for _, cl := range mm.deletes {
		if err := c.updateDeletes(cl); err != nil {
			return fmt.Errorf("Could not update the deletes cache after deletion from %v: %w", cl, err)
		}
	}
	return nil
}

// updateDeletes updates the corpus deletes with the delete claim deleteClaim.
// deleteClaim is trusted to be a valid delete Claim.
func (c *corpusMem) updateDeletes(deleteClaim schema.Claim) error {
	target := c.br(deleteClaim.Target())
	deleter := deleteClaim.Blob()
	when, err := deleter.ClaimDate()
	if err != nil {
		return fmt.Errorf("Could not get date of delete claim %v: %w", deleteClaim, err)
	}
	del := deletion{
		deleter: c.br(deleter.BlobRef()),
		when:    when,
	}
	if slices.Contains(c.deletes[target], del) {
		return nil
	}
	targetDeletions := append(c.deletes[target], del)
	sort.Sort(sort.Reverse(byDeletionDate(targetDeletions)))
	c.deletes[target] = targetDeletions
	return nil
}

func (c *corpusMem) mergeMetaRow(k, v []byte) error {
	bm, ok := kvBlobMeta_bytes(k, v)
	if !ok {
		return fmt.Errorf("bogus meta row: %q -> %q", k, v)
	}
	return c.mergeBlobMeta(bm)
}

func (c *corpusMem) mergeBlobMeta(bm camtypes.BlobMeta) error {
	if _, dup := c.blobs[bm.Ref]; dup {
		panic("dup blob seen")
	}
	bm.CamliType = schema.CamliType((c.str(string(bm.CamliType))))

	c.blobs[bm.Ref] = &bm
	c.sumBlobBytes += int64(bm.Size)
	if bm.CamliType != "" {
		m, ok := c.camBlobs[bm.CamliType]
		if !ok {
			m = make(map[blob.Ref]*camtypes.BlobMeta)
			c.camBlobs[bm.CamliType] = m
		}
		m[bm.Ref] = &bm
	}
	return nil
}

func (c *corpusMem) mergeSignerKeyIdRow(k, v []byte) error {
	br, ok := blob.ParseBytes(k[len("signerkeyid:"):])
	if !ok {
		return fmt.Errorf("bogus signerid row: %q -> %q", k, v)
	}
	c.keyId[br] = string(v)
	return nil
}

func (c *corpusMem) mergeClaimRow(k, v []byte) error {
	cl, ok := c.kvClaimBytes(k, v)
	if !ok || !cl.Permanode.Valid() {
		return fmt.Errorf("bogus claim row: %q -> %q", k, v)
	}

	pn := c.br(cl.Permanode)
	pm, ok := c.permanodes[pn]
	if !ok {
		pm = new(PermanodeMeta)
		c.permanodes[pn] = pm
	}
	pm.Claims = append(pm.Claims, &cl)
	if !c.building {
		// Unless we're still starting up (at which we sort at
		// the end instead), keep claims sorted and attrs in sync.
		if err := pm.fixupLastClaim(c.keyId); err != nil {
			return err
		}
	}

	if vbr, ok := blob.Parse(cl.Value); ok {
		c.claimBack[vbr] = append(c.claimBack[vbr], &cl)
	}
	if cl.Attr == "camliNodeType" {
		set := c.permanodesSetByNodeType[cl.Value]
		if set == nil {
			set = make(map[blob.Ref]bool)
			c.permanodesSetByNodeType[cl.Value] = set
		}
		set[pn] = true
	}
	return nil
}

func (c *corpusMem) mergeFileInfoRow(k, v []byte) error {
	// fileinfo|sha1-579f7f246bd420d486ddeb0dadbb256cfaf8bf6b" "5|some-stuff.txt|"
	pipe := bytes.IndexByte(k, '|')
	if pipe < 0 {
		return fmt.Errorf("unexpected fileinfo key %q", k)
	}
	br, ok := blob.ParseBytes(k[pipe+1:])
	if !ok {
		return fmt.Errorf("unexpected fileinfo blobref in key %q", k)
	}

	// TODO: could at least use strutil.ParseUintBytes to not stringify and retain
	// the length bytes of v.
	c.ss = strutil.AppendSplitN(c.ss[:0], string(v), "|", 4)
	if len(c.ss) != 3 && len(c.ss) != 4 {
		return fmt.Errorf("unexpected fileinfo value %q", v)
	}
	size, err := strconv.ParseInt(c.ss[0], 10, 64)
	if err != nil {
		return fmt.Errorf("unexpected fileinfo value %q", v)
	}
	var wholeRef blob.Ref
	if len(c.ss) == 4 && c.ss[3] != "" { // checking for "" because of special files such as symlinks.
		var ok bool
		wholeRef, ok = blob.Parse(urld(c.ss[3]))
		if !ok {
			return fmt.Errorf("invalid wholeRef blobref in value %q for fileinfo key %q", v, k)
		}
	}
	c.mutateFileInfo(br, func(fi *camtypes.FileInfo) {
		fi.Size = size
		fi.FileName = c.str(urld(c.ss[1]))
		fi.MIMEType = c.str(urld(c.ss[2]))
		fi.WholeRef = wholeRef
	})
	return nil
}

func (c *corpusMem) mergeStaticDirChildRow(k, v []byte) error {
	// dirchild|sha1-dir|sha1-child" "1"
	// strip the key name
	sk := k[len(keyStaticDirChild.name)+1:]
	pipe := bytes.IndexByte(sk, '|')
	if pipe < 0 {
		return fmt.Errorf("invalid dirchild key %q, missing second pipe", k)
	}
	parent, ok := blob.ParseBytes(sk[:pipe])
	if !ok {
		return fmt.Errorf("invalid dirchild parent blobref in key %q", k)
	}
	child, ok := blob.ParseBytes(sk[pipe+1:])
	if !ok {
		return fmt.Errorf("invalid dirchild child blobref in key %q", k)
	}
	parent = c.br(parent)
	child = c.br(child)
	children, ok := c.dirChildren[parent]
	if !ok {
		children = make(map[blob.Ref]struct{})
	}
	children[child] = struct{}{}
	c.dirChildren[parent] = children
	parents, ok := c.fileParents[child]
	if !ok {
		parents = make(map[blob.Ref]struct{})
	}
	parents[parent] = struct{}{}
	c.fileParents[child] = parents
	return nil
}

func (c *corpusMem) mergeFileTimesRow(k, v []byte) error {
	if len(v) == 0 {
		return nil
	}
	// "filetimes|sha1-579f7f246bd420d486ddeb0dadbb256cfaf8bf6b" "1970-01-01T00%3A02%3A03Z"
	pipe := bytes.IndexByte(k, '|')
	if pipe < 0 {
		return fmt.Errorf("unexpected fileinfo key %q", k)
	}
	br, ok := blob.ParseBytes(k[pipe+1:])
	if !ok {
		return fmt.Errorf("unexpected filetimes blobref in key %q", k)
	}
	c.ss = strutil.AppendSplitN(c.ss[:0], urld(string(v)), ",", -1)
	times := c.ss
	c.mutateFileInfo(br, func(fi *camtypes.FileInfo) {
		updateFileInfoTimes(fi, times)
	})
	return nil
}

func (c *corpusMem) mutateFileInfo(br blob.Ref, fn func(*camtypes.FileInfo)) {
	br = c.br(br)
	fi := c.files[br] // use zero value if not present
	fn(&fi)
	c.files[br] = fi
}

func (c *corpusMem) mergeImageSizeRow(k, v []byte) error {
	br, okk := blob.ParseBytes(k[len("imagesize|"):])
	ii, okv := kvImageInfo(v)
	if !okk || !okv {
		return fmt.Errorf("bogus row %q = %q", k, v)
	}
	br = c.br(br)
	c.imageInfo[br] = ii
	return nil
}

var sha1Prefix = []byte("sha1-")

// "wholetofile|sha1-17b53c7c3e664d3613dfdce50ef1f2a09e8f04b5|sha1-fb88f3eab3acfcf3cfc8cd77ae4366f6f975d227" -> "1"
func (c *corpusMem) mergeWholeToFileRow(k, v []byte) error {
	pair := k[len("wholetofile|"):]
	pipe := bytes.IndexByte(pair, '|')
	if pipe < 0 {
		return fmt.Errorf("bogus row %q = %q", k, v)
	}
	wholeRef, ok1 := blob.ParseBytes(pair[:pipe])
	fileRef, ok2 := blob.ParseBytes(pair[pipe+1:])
	if !ok1 || !ok2 {
		return fmt.Errorf("bogus row %q = %q", k, v)
	}
	c.fileWholeRef[fileRef] = wholeRef
	if c.building && !c.hasLegacySHA1 {
		if bytes.HasPrefix(pair, sha1Prefix) {
			c.hasLegacySHA1 = true
		}
	}
	return nil
}

// "mediatag|sha1-2b219be9d9691b4f8090e7ee2690098097f59566|album" = "Some+Album+Name"
func (c *corpusMem) mergeMediaTag(k, v []byte) error {
	f := strings.Split(string(k), "|")
	if len(f) != 3 {
		return fmt.Errorf("unexpected key %q", k)
	}
	wholeRef, ok := blob.Parse(f[1])
	if !ok {
		return fmt.Errorf("failed to parse wholeref from key %q", k)
	}
	tm, ok := c.mediaTags[wholeRef]
	if !ok {
		tm = make(map[string]string)
		c.mediaTags[wholeRef] = tm
	}
	tm[c.str(f[2])] = c.str(urld(string(v)))
	return nil
}

// "exifgps|sha1-17b53c7c3e664d3613dfdce50ef1f2a09e8f04b5" -> "-122.39897155555556|37.61952208333334"
func (c *corpusMem) mergeEXIFGPSRow(k, v []byte) error {
	wholeRef, ok := blob.ParseBytes(k[len("exifgps|"):])
	pipe := bytes.IndexByte(v, '|')
	if pipe < 0 || !ok {
		return fmt.Errorf("bogus row %q = %q", k, v)
	}
	lat, err := strconv.ParseFloat(string(v[:pipe]), 64)
	long, err1 := strconv.ParseFloat(string(v[pipe+1:]), 64)
	if err != nil || err1 != nil {
		if err != nil {
			log.Printf("index: bogus latitude in value of row %q = %q", k, v)
		} else {
			log.Printf("index: bogus longitude in value of row %q = %q", k, v)
		}
		return nil
	}
	c.gps[wholeRef] = latLong{lat, long}
	return nil
}

func (c *corpusMem) blobParse(v []byte) (br blob.Ref, ok bool) {
	br, ok = c.brOfStr[string(v)]
	if ok {
		return
	}
	br, ok = blob.ParseBytes(v)
	if !ok {
		return
	}
	if c.brOfStr == nil {
		c.brOfStr = make(map[string]blob.Ref)
	}
	c.brOfStr[string(v)] = br
	return br, true
}

// str returns s, interned.
func (c *corpusMem) str(s string) string {
	if s == "" {
		return ""
	}
	if s, ok := c.strs[s]; ok {
		return s
	}
	if c.strs == nil {
		c.strs = make(map[string]string)
	}
	c.strs[s] = s
	return s
}

// strB returns string(b), interned.
func (c *corpusMem) strB(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if s, ok := c.strs[string(b)]; ok {
		return s
	}
	if c.strs == nil {
		c.strs = make(map[string]string)
	}
	s := string(b)
	c.strs[s] = s
	return s
}

// br returns br, interned.
func (c *corpusMem) br(br blob.Ref) blob.Ref {
	if bm, ok := c.blobs[br]; ok {
		c.brInterns++
		return bm.Ref
	}
	return br
}

// *********** Reading from the corpus

// EnumerateCamliBlobs calls fn for all known meta blobs.
//
// If camType is not empty, it specifies a filter for which meta blob
// types to call fn for. If empty, all are emitted.
//
// If fn returns false, iteration ends.
func (c *corpusMem) EnumerateCamliBlobs(camType schema.CamliType, fn func(camtypes.BlobMeta) bool) {
	if camType != "" {
		for _, bm := range c.camBlobs[camType] {
			if !fn(*bm) {
				return
			}
		}
		return
	}
	for _, m := range c.camBlobs {
		for _, bm := range m {
			if !fn(*bm) {
				return
			}
		}
	}
}

// EnumerateBlobMeta calls fn for all known meta blobs in an undefined
// order.
// If fn returns false, iteration ends.
func (c *corpusMem) EnumerateBlobMeta(fn func(camtypes.BlobMeta) bool) {
	for _, bm := range c.blobs {
		if !fn(*bm) {
			return
		}
	}
}

func (c *corpusMem) enumeratePermanodes(fn func(camtypes.BlobMeta) bool, pns []pnAndTime) {
	for _, cand := range pns {
		bm := c.blobs[cand.pn]
		if bm == nil {
			continue
		}
		if !fn(*bm) {
			return
		}
	}
}

// EnumeratePermanodesLastModified calls fn for all permanodes, sorted by most recently modified first.
// Iteration ends prematurely if fn returns false.
func (c *corpusMem) EnumeratePermanodesLastModified(fn func(camtypes.BlobMeta) bool) {
	c.enumeratePermanodes(fn, c.permanodesByModtime.sorted(true))
}

// EnumeratePermanodesCreated calls fn for all permanodes.
// They are sorted using the contents creation date if any, the permanode modtime
// otherwise, and in the order specified by newestFirst.
// Iteration ends prematurely if fn returns false.
func (c *corpusMem) EnumeratePermanodesCreated(fn func(camtypes.BlobMeta) bool, newestFirst bool) {
	c.enumeratePermanodes(fn, c.permanodesByTime.sorted(newestFirst))
}

// EnumerateSingleBlob calls fn with br's BlobMeta if br exists in the corpus.
func (c *corpusMem) EnumerateSingleBlob(fn func(camtypes.BlobMeta) bool, br blob.Ref) {
	if bm := c.blobs[br]; bm != nil {
		fn(*bm)
	}
}

// EnumeratePermanodesByNodeTypes enumerates over all permanodes that might
// have one of the provided camliNodeType values, calling fn for each. If fn returns false,
// enumeration ends.
func (c *corpusMem) EnumeratePermanodesByNodeTypes(fn func(camtypes.BlobMeta) bool, camliNodeTypes []string) {
	for _, t := range camliNodeTypes {
		set := c.permanodesSetByNodeType[t]
		for br := range set {
			if bm := c.blobs[br]; bm != nil {
				if !fn(*bm) {
					return
				}
			}
		}
	}
}

func (c *corpusMem) IterPermanodes() iter.Seq[blob.Ref] {
	return func(yield func(blob.Ref) bool) {
		for ref := range c.permanodes {
			if !yield(ref) {
				return
			}
		}
	}
}

func (c *corpusMem) GetBlobMeta(ctx context.Context, br blob.Ref) (camtypes.BlobMeta, error) {
	bm, ok := c.blobs[br]
	if !ok {
		return camtypes.BlobMeta{}, os.ErrNotExist
	}
	return *bm, nil
}

func (c *corpusMem) KeyId(ctx context.Context, signer blob.Ref) (string, error) {
	if v, ok := c.keyId[signer]; ok {
		return v, nil
	}
	return "", sorted.ErrNotFound
}

func (c *corpusMem) pnTimeAttr(pn blob.Ref, attr string) (t time.Time, ok bool) {
	if v := c.PermanodeAttrValue(pn, attr, time.Time{}, ""); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, true
		}
	}
	return
}

// PermanodeTime returns the time of the content in permanode.
func (c *corpusMem) PermanodeTime(pn blob.Ref) (t time.Time, ok bool) {
	// TODO(bradfitz): keep this time property cached on the permanode / files
	// TODO(bradfitz): finish implementing all these

	// Priorities:
	// -- Permanode explicit "camliTime" property
	// -- EXIF GPS time
	// -- Exif camera time - this one is actually already in the FileInfo,
	// because we use schema.FileTime (which returns the EXIF time, if available)
	// to index the time when receiving a file.
	// -- File time
	// -- File modtime
	// -- camliContent claim set time

	if t, ok = c.pnTimeAttr(pn, nodeattr.PaymentDueDate); ok {
		return
	}
	if t, ok = c.pnTimeAttr(pn, nodeattr.StartDate); ok {
		return
	}
	if t, ok = c.pnTimeAttr(pn, nodeattr.DateCreated); ok {
		return
	}
	var fi camtypes.FileInfo
	ccRef, ccTime, ok := c.pnCamliContent(pn)
	if ok {
		fi = c.files[ccRef]
	}
	if fi.Time != nil {
		return time.Time(*fi.Time), true
	}

	if t, ok = c.pnTimeAttr(pn, nodeattr.DatePublished); ok {
		return
	}
	if t, ok = c.pnTimeAttr(pn, nodeattr.DateModified); ok {
		return
	}
	if fi.ModTime != nil {
		return time.Time(*fi.ModTime), true
	}
	if ok {
		return ccTime, true
	}
	return time.Time{}, false
}

// PermanodeAnyTime returns the time that best qualifies the permanode.
// It tries content-specific times first, the permanode modtime otherwise.
func (c *corpusMem) PermanodeAnyTime(pn blob.Ref) (t time.Time, ok bool) {
	if t, ok := c.PermanodeTime(pn); ok {
		return t, ok
	}
	return c.PermanodeModtime(pn)
}

func (c *corpusMem) pnCamliContent(pn blob.Ref) (cc blob.Ref, t time.Time, ok bool) {
	// TODO(bradfitz): keep this property cached
	pm, ok := c.permanodes[pn]
	if !ok {
		return
	}
	for _, cl := range pm.Claims {
		if cl.Attr != "camliContent" {
			continue
		}
		// TODO: pass down the 'PermanodeConstraint.At' parameter, and then do: if cl.Date.After(at) { continue }
		switch cl.Type {
		case string(schema.DelAttributeClaim):
			cc = blob.Ref{}
			t = time.Time{}
		case string(schema.SetAttributeClaim):
			cc = blob.ParseOrZero(cl.Value)
			t = cl.Date
		}
	}
	return cc, t, cc.Valid()

}

// PermanodeModtime returns the latest modification time of the given
// permanode.
//
// The ok value is true only if the permanode is known and has any
// non-deleted claims. A deleted claim is ignored and neither its
// claim date nor the date of the delete claim affect the modtime of
// the permanode.
func (c *corpusMem) PermanodeModtime(pn blob.Ref) (t time.Time, ok bool) {
	pm, ok := c.permanodes[pn]
	if !ok {
		return
	}

	// Note: We intentionally don't try to derive any information
	// (except the owner, elsewhere) from the permanode blob
	// itself. Even though the permanode blob sometimes has the
	// GPG signature time, we intentionally ignore it.
	for _, cl := range pm.Claims {
		if c.IsDeleted(cl.BlobRef) {
			continue
		}
		if cl.Date.After(t) {
			t = cl.Date
		}
	}
	return t, !t.IsZero()
}

// PermanodeAttrValue returns a single-valued attribute or "".
// signerFilter, if set, should be the GPG ID of a signer
// (e.g. 2931A67C26F5ABDA).
func (c *corpusMem) PermanodeAttrValue(permaNode blob.Ref,
	attr string,
	at time.Time,
	signerFilter string) string {
	pm, ok := c.permanodes[permaNode]
	if !ok {
		return ""
	}
	var signerRefs SignerRefSet
	if signerFilter != "" {
		signerRefs, ok = c.signerRefs[signerFilter]
		if !ok {
			return ""
		}
	}

	if values, ok := pm.valuesAtSigner(at, signerFilter); ok {
		v := values[attr]
		if len(v) == 0 {
			return ""
		}
		return v[0]
	}

	return claimPtrsAttrValue(pm.Claims, attr, at, signerRefs)
}

// permanodeAttrsOrClaims returns the best available source
// to query attr values of permaNode at the given time
// for the signerID, which is either:
// a. m that represents attr values for the parameters, or
// b. all claims of the permanode.
// Only one of m or claims will be non-nil.
//
// (m, nil) is returned if m represents attrValues
// valid for the specified parameters.
//
// (nil, claims) is returned if
// no cached attribute map is valid for the given time,
// because e.g. some claims are more recent than this time. In which
// case the caller should resort to query claims directly.
//
// (nil, nil) is returned if the permaNode does not exist,
// or permaNode exists and signerID is valid,
// but permaNode has no attributes for it.
//
// The returned values must not be changed by the caller.
func (c *corpusMem) PermanodeAttrsOrClaims(permaNode blob.Ref,
	at time.Time, signerID string) (m map[string][]string, claims []*camtypes.Claim) {

	pm, ok := c.permanodes[permaNode]
	if !ok {
		return nil, nil
	}

	m, ok = pm.valuesAtSigner(at, signerID)
	if ok {
		return m, nil
	}
	return nil, pm.Claims
}

// AppendPermanodeAttrValues appends to dst all the values for the attribute
// attr set on permaNode.
// signerFilter, if set, should be the GPG ID of a signer (e.g. 2931A67C26F5ABDA).
// dst must start with length 0 (laziness, mostly)
func (c *corpusMem) AppendPermanodeAttrValues(dst []string,
	permaNode blob.Ref,
	attr string,
	at time.Time,
	signerFilter string) []string {
	if len(dst) > 0 {
		panic("len(dst) must be 0")
	}
	pm, ok := c.permanodes[permaNode]
	if !ok {
		return dst
	}
	var signerRefs SignerRefSet
	if signerFilter != "" {
		signerRefs, ok = c.signerRefs[signerFilter]
		if !ok {
			return dst
		}
	}
	if values, ok := pm.valuesAtSigner(at, signerFilter); ok {
		return append(dst, values[attr]...)
	}
	if at.IsZero() {
		at = time.Now()
	}
	for _, cl := range pm.Claims {
		if cl.Attr != attr || cl.Date.After(at) {
			continue
		}
		if len(signerRefs) > 0 && !signerRefs.blobMatches(cl.Signer) {
			continue
		}
		switch cl.Type {
		case string(schema.DelAttributeClaim):
			if cl.Value == "" {
				dst = dst[:0] // delete all
			} else {
				for i := 0; i < len(dst); i++ {
					v := dst[i]
					if v == cl.Value {
						copy(dst[i:], dst[i+1:])
						dst = dst[:len(dst)-1]
						i--
					}
				}
			}
		case string(schema.SetAttributeClaim):
			dst = append(dst[:0], cl.Value)
		case string(schema.AddAttributeClaim):
			dst = append(dst, cl.Value)
		}
	}
	return dst
}

func (c *corpusMem) AppendClaims(ctx context.Context, dst []camtypes.Claim, permaNode blob.Ref,
	signerFilter string,
	attrFilter string) ([]camtypes.Claim, error) {
	pm, ok := c.permanodes[permaNode]
	if !ok {
		return nil, nil
	}

	var signerRefs SignerRefSet
	if signerFilter != "" {
		signerRefs, ok = c.signerRefs[signerFilter]
		if !ok {
			return dst, nil
		}
	}

	for _, cl := range pm.Claims {
		if c.IsDeleted(cl.BlobRef) {
			continue
		}

		if len(signerRefs) > 0 && !signerRefs.blobMatches(cl.Signer) {
			continue
		}

		if attrFilter != "" && cl.Attr != attrFilter {
			continue
		}
		dst = append(dst, *cl)
	}
	return dst, nil
}

func (c *corpusMem) GetFileInfo(ctx context.Context, fileRef blob.Ref) (fi camtypes.FileInfo, err error) {
	fi, ok := c.files[fileRef]
	if !ok {
		err = os.ErrNotExist
	}
	return
}

// GetDirChildren returns the direct children (static-set entries) of the directory dirRef.
// It only returns an error if dirRef does not exist.
func (c *corpusMem) GetDirChildren(ctx context.Context, dirRef blob.Ref) (map[blob.Ref]struct{}, error) {
	children, ok := c.dirChildren[dirRef]
	if !ok {
		if _, ok := c.files[dirRef]; !ok {
			return nil, os.ErrNotExist
		}
		return nil, nil
	}
	return children, nil
}

// GetParentDirs returns the direct parents (directories) of the file or directory childRef.
// It only returns an error if childRef does not exist.
func (c *corpusMem) GetParentDirs(ctx context.Context, childRef blob.Ref) (map[blob.Ref]struct{}, error) {
	parents, ok := c.fileParents[childRef]
	if !ok {
		if _, ok := c.files[childRef]; !ok {
			return nil, os.ErrNotExist
		}
		return nil, nil
	}
	return parents, nil
}

func (c *corpusMem) GetImageInfo(ctx context.Context, fileRef blob.Ref) (ii camtypes.ImageInfo, err error) {
	ii, ok := c.imageInfo[fileRef]
	if !ok {
		err = os.ErrNotExist
	}
	return
}

func (c *corpusMem) GetMediaTags(ctx context.Context, fileRef blob.Ref) (map[string]string, error) {
	wholeRef, ok := c.fileWholeRef[fileRef]
	if !ok {
		return nil, os.ErrNotExist
	}
	tags, ok := c.mediaTags[wholeRef]
	if !ok {
		return nil, os.ErrNotExist
	}
	return tags, nil
}

func (c *corpusMem) GetWholeRef(ctx context.Context, fileRef blob.Ref) (wholeRef blob.Ref, ok bool) {
	wholeRef, ok = c.fileWholeRef[fileRef]
	return
}

func (c *corpusMem) FileLatLong(fileRef blob.Ref) (lat, long float64, ok bool) {
	wholeRef, ok := c.fileWholeRef[fileRef]
	if !ok {
		return
	}
	ll, ok := c.gps[wholeRef]
	if !ok {
		return
	}
	return ll.lat, ll.long, true
}

// ForeachClaim calls fn for each claim of permaNode.
// If at is zero, all claims are yielded.
// If at is non-zero, claims after that point are skipped.
// If fn returns false, iteration ends.
// Iteration is in an undefined order.
func (c *corpusMem) ForeachClaim(permaNode blob.Ref, at time.Time, fn func(*camtypes.Claim) bool) {
	pm, ok := c.permanodes[permaNode]
	if !ok {
		return
	}
	for _, cl := range pm.Claims {
		if !at.IsZero() && cl.Date.After(at) {
			continue
		}
		if !fn(cl) {
			return
		}
	}
}

// ForeachClaimBack calls fn for each claim with a value referencing br.
// If at is zero, all claims are yielded.
// If at is non-zero, claims after that point are skipped.
// If fn returns false, iteration ends.
// Iteration is in an undefined order.
func (c *corpusMem) ForeachClaimBack(value blob.Ref, at time.Time, fn func(*camtypes.Claim) bool) {
	for _, cl := range c.claimBack[value] {
		if !at.IsZero() && cl.Date.After(at) {
			continue
		}
		if !fn(cl) {
			return
		}
	}
}

// PermanodeHasAttrValue reports whether the permanode pn at
// time at (zero means now) has the given attribute with the given
// value. If the attribute is multi-valued, any may match.
func (c *corpusMem) PermanodeHasAttrValue(pn blob.Ref, at time.Time, attr, val string) bool {
	pm, ok := c.permanodes[pn]
	if !ok {
		return false
	}
	if values, ok := pm.valuesAtSigner(at, ""); ok {
		return slices.Contains(values[attr], val)
	}
	if at.IsZero() {
		at = time.Now()
	}
	ret := false
	for _, cl := range pm.Claims {
		if cl.Attr != attr {
			continue
		}
		if cl.Date.After(at) {
			break
		}
		switch cl.Type {
		case string(schema.DelAttributeClaim):
			if cl.Value == "" || cl.Value == val {
				ret = false
			}
		case string(schema.SetAttributeClaim):
			ret = (cl.Value == val)
		case string(schema.AddAttributeClaim):
			if cl.Value == val {
				ret = true
			}
		}
	}
	return ret
}
