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
	"context"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"sort"
	"sync"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/sorted"
	"perkeep.org/pkg/types/camtypes"
)

// Corpus is a summary of all of a user's blobs' metadata, as used by the
// search machinery. The index package provides an in-memory implementation
// (the unexported corpusMem type, obtained with Index.KeepInMemory or
// NewCorpusFromStorage), and perkeep.org/pkg/index/dbcorpus provides an
// implementation backed by a database/sql (SQLite) database.
//
// A Corpus is not safe for concurrent use. Callers should use Lock or RLock
// on the parent index instead.
//
// Unless documented otherwise, a method returning a map or slice returns a
// value that must not be changed by the caller, and a not-found blob is
// reported with os.ErrNotExist.
type Corpus interface {
	// AddBlob applies to the corpus the mutations that were committed to
	// the index when the blob br was received.
	AddBlob(ctx context.Context, br blob.Ref, mm *mutationMap) error

	// HasLegacySHA1 reports whether some SHA-1 blobs are indexed.
	HasLegacySHA1() bool

	// SignerRefs returns the set of blobRefs (of different hashes) that
	// represent the same signer GPG identity as the given GPG key ID
	// (e.g. "2931A67C26F5ABDA").
	SignerRefs(keyID string) SignerRefSet

	// IsDeleted reports whether the provided blobref (of a permanode or
	// claim) should be considered deleted.
	IsDeleted(br blob.Ref) bool

	// KeyId returns the GPG key ID (e.g. "2931A67C26F5ABDA") of the
	// signer blob signer. It returns sorted.ErrNotFound if unknown.
	KeyId(ctx context.Context, signer blob.Ref) (string, error)

	// GetBlobMeta returns the metadata of br, or os.ErrNotExist if the
	// blob is not known.
	GetBlobMeta(ctx context.Context, br blob.Ref) (camtypes.BlobMeta, error)

	// GetFileInfo returns the info of fileRef (a file or directory schema
	// blob), or os.ErrNotExist if not known.
	GetFileInfo(ctx context.Context, fileRef blob.Ref) (camtypes.FileInfo, error)

	// GetImageInfo returns the dimensions of the image file fileRef, or
	// os.ErrNotExist if not known.
	GetImageInfo(ctx context.Context, fileRef blob.Ref) (camtypes.ImageInfo, error)

	// GetMediaTags returns the media tags (e.g. ID3) of the whole file
	// referenced by fileRef, or os.ErrNotExist if none are known.
	GetMediaTags(ctx context.Context, fileRef blob.Ref) (map[string]string, error)

	// GetWholeRef returns the wholeRef of the file fileRef, if any.
	GetWholeRef(ctx context.Context, fileRef blob.Ref) (wholeRef blob.Ref, ok bool)

	// FileLatLong returns the GPS coordinates of the contents of fileRef.
	FileLatLong(fileRef blob.Ref) (lat, long float64, ok bool)

	// GetDirChildren returns the direct children (static-set entries) of
	// the directory dirRef. It only returns an error if dirRef does not
	// exist.
	GetDirChildren(ctx context.Context, dirRef blob.Ref) (map[blob.Ref]struct{}, error)

	// GetParentDirs returns the direct parents (directories) of the file
	// or directory childRef. It only returns an error if childRef does
	// not exist.
	GetParentDirs(ctx context.Context, childRef blob.Ref) (map[blob.Ref]struct{}, error)

	// AppendClaims appends to dst the claims on permaNode. The
	// signerFilter (a GPG key ID) and attrFilter are both optional: if
	// set, they filter the returned claims to only those made by the
	// given signer, or about the given attribute, respectively. Deleted
	// claims are never returned.
	AppendClaims(ctx context.Context, dst []camtypes.Claim, permaNode blob.Ref,
		signerFilter string,
		attrFilter string) ([]camtypes.Claim, error)

	// AppendPermanodeAttrValues appends to dst all the values for the
	// attribute attr set on permaNode. signerFilter, if set, should be
	// the GPG ID of a signer. dst must start with length 0.
	AppendPermanodeAttrValues(dst []string,
		permaNode blob.Ref,
		attr string,
		at time.Time,
		signerFilter string) []string

	// PermanodeAttrValue returns a single-valued attribute of permaNode,
	// or "". signerFilter, if set, should be the GPG ID of a signer.
	PermanodeAttrValue(permaNode blob.Ref, attr string, at time.Time, signerFilter string) string

	// PermanodeHasAttrValue reports whether the permanode pn at time at
	// (zero means now) has the given attribute with the given value. If
	// the attribute is multi-valued, any may match.
	PermanodeHasAttrValue(pn blob.Ref, at time.Time, attr, val string) bool

	// ForeachClaim calls fn for each claim of permaNode. If at is zero,
	// all claims are yielded; otherwise claims after that point are
	// skipped. If fn returns false, iteration ends. Iteration is in an
	// undefined order.
	ForeachClaim(permaNode blob.Ref, at time.Time, fn func(*camtypes.Claim) bool)

	// ForeachClaimBack calls fn for each claim with a value referencing
	// value. The at and fn semantics are the same as for ForeachClaim.
	ForeachClaimBack(value blob.Ref, at time.Time, fn func(*camtypes.Claim) bool)

	// PermanodeModtime returns the latest modification time of the given
	// permanode. The ok value is true only if the permanode is known and
	// has any non-deleted claims.
	PermanodeModtime(pn blob.Ref) (t time.Time, ok bool)

	// PermanodeAnyTime returns the time that best qualifies the
	// permanode. It tries content-specific times first, the permanode
	// modtime otherwise.
	PermanodeAnyTime(pn blob.Ref) (t time.Time, ok bool)

	// PermanodeAttrsOrClaims returns the best available source to query
	// attr values of permaNode at the given time for the signerID, which
	// is either the attribute values (m), or all the claims of the
	// permanode. Only one of m or claims is non-nil. Both are nil if the
	// permanode does not exist, or if it has no attributes for signerID.
	// The returned values must not be changed by the caller.
	PermanodeAttrsOrClaims(permaNode blob.Ref,
		at time.Time, signerID string) (m map[string][]string, claims []*camtypes.Claim)

	// EnumerateBlobMeta calls fn for all known meta blobs in an undefined
	// order. If fn returns false, iteration ends.
	EnumerateBlobMeta(fn func(camtypes.BlobMeta) bool)

	// EnumerateCamliBlobs calls fn for all known meta blobs. If camType
	// is not empty, it specifies a filter for which meta blob types to
	// call fn for; if empty, all are emitted. If fn returns false,
	// iteration ends.
	EnumerateCamliBlobs(camType schema.CamliType, fn func(camtypes.BlobMeta) bool)

	// EnumeratePermanodesLastModified calls fn for all permanodes, sorted
	// by most recently modified first. Iteration ends prematurely if fn
	// returns false.
	EnumeratePermanodesLastModified(fn func(camtypes.BlobMeta) bool)

	// EnumeratePermanodesCreated calls fn for all permanodes. They are
	// sorted using the contents creation date if any, the permanode
	// modtime otherwise, and in the order specified by newestFirst.
	// Iteration ends prematurely if fn returns false.
	EnumeratePermanodesCreated(fn func(camtypes.BlobMeta) bool, newestFirst bool)

	// EnumeratePermanodesByNodeTypes enumerates over all permanodes that
	// might have one of the provided camliNodeType values, calling fn for
	// each. If fn returns false, enumeration ends.
	EnumeratePermanodesByNodeTypes(fn func(camtypes.BlobMeta) bool, camliNodeTypes []string)

	// EnumerateSingleBlob calls fn with br's BlobMeta if br exists in the
	// corpus.
	EnumerateSingleBlob(fn func(camtypes.BlobMeta) bool, br blob.Ref)

	// IterPermanodes iterates over all permanodes, without any specific order
	IterPermanodes() iter.Seq[blob.Ref]

	Generation() int64
}

type PermanodeMeta struct {
	Claims []*camtypes.Claim // sorted by camtypes.ClaimsByDate

	attr attrValues // attributes from all signers

	// signer maps a signer's GPG ID (e.g. 2931A67C26F5ABDA) to the attrs for this
	// signer.
	signer map[string]attrValues
}

type attrValues map[string][]string

// cacheAttrClaim applies attribute changes from cl.
func (m attrValues) cacheAttrClaim(cl *camtypes.Claim) {
	switch cl.Type {
	case string(schema.SetAttributeClaim):
		m[cl.Attr] = []string{cl.Value}
	case string(schema.AddAttributeClaim):
		m[cl.Attr] = append(m[cl.Attr], cl.Value)
	case string(schema.DelAttributeClaim):
		if cl.Value == "" {
			delete(m, cl.Attr)
		} else {
			a, i := m[cl.Attr], 0
			for _, v := range a {
				if v != cl.Value {
					a[i] = v
					i++
				}
			}
			m[cl.Attr] = a[:i]
		}
	}
}

// restoreInvariants sorts claims by date and
// recalculates latest attributes.
func (pm *PermanodeMeta) restoreInvariants(signers signerFromBlobrefMap) error {
	sort.Sort(camtypes.ClaimPtrsByDate(pm.Claims))
	pm.attr = make(attrValues)
	pm.signer = make(map[string]attrValues)
	for _, cl := range pm.Claims {
		if err := pm.appendAttrClaim(cl, signers); err != nil {
			return err
		}
	}
	return nil
}

// fixupLastClaim fixes invariants on the assumption
// that the all but the last element in Claims are sorted by date
// and the last element is the only one not yet included in Attrs.
func (pm *PermanodeMeta) fixupLastClaim(signers signerFromBlobrefMap) error {
	if pm.attr != nil {
		n := len(pm.Claims)
		if n < 2 || camtypes.ClaimPtrsByDate(pm.Claims).Less(n-2, n-1) {
			// already sorted, update Attrs from new Claim
			return pm.appendAttrClaim(pm.Claims[n-1], signers)
		}
	}
	return pm.restoreInvariants(signers)
}

// appendAttrClaim stores permanode attributes
// from cl in pm.attr and pm.signer[signerID[cl.Signer]].
// The caller of appendAttrClaim is responsible for calling
// it with claims sorted in camtypes.ClaimPtrsByDate order.
func (pm *PermanodeMeta) appendAttrClaim(cl *camtypes.Claim, signers signerFromBlobrefMap) error {
	signer, ok := signers[cl.Signer]
	if !ok {
		return fmt.Errorf("claim %v has unknown signer %q", cl.BlobRef, cl.Signer)
	}
	sc, ok := pm.signer[signer]
	if !ok {
		// Optimize for the case where cl.Signer of all claims are the same.
		// Instead of having two identical attrValues copies in
		// pm.attr and pm.signer[cl.Signer],
		// use a single attrValues
		// until there is at least a second signer.
		switch len(pm.signer) {
		case 0:
			// Set up signer cache to reference
			// the existing attrValues.
			pm.attr.cacheAttrClaim(cl)
			pm.signer[signer] = pm.attr
			return nil

		case 1:
			// pm.signer has exactly one other signer,
			// and its attrValues entry references pm.attr.
			// Make a copy of pm.attr
			// for this other signer now.
			m := make(attrValues)
			for a, v := range pm.attr {
				xv := make([]string, len(v))
				copy(xv, v)
				m[a] = xv
			}

			for sig := range pm.signer {
				pm.signer[sig] = m
				break
			}
		}
		sc = make(attrValues)
		pm.signer[signer] = sc
	}

	pm.attr.cacheAttrClaim(cl)

	// Cache claim in sc only if sc != pm.attr.
	if len(pm.signer) > 1 {
		sc.cacheAttrClaim(cl)
	}
	return nil
}

// valuesAtSigner returns an attrValues to query permanode attr values at the
// given time for the signerFilter, which is the GPG ID of a signer (e.g. 2931A67C26F5ABDA).
// It returns (nil, true) if signerFilter is not empty but pm has no
// attributes for it (including if signerFilter is unknown).
// It returns ok == true if v represents attrValues valid for the specified
// parameters.
// It returns (nil, false) if neither pm.attr nor pm.signer should be used for
// the given time, because e.g. some claims are more recent than this time. In
// which case, the caller should resort to querying another source, such as pm.Claims.
// The returned map must not be changed by the caller.
func (pm *PermanodeMeta) valuesAtSigner(at time.Time,
	signerFilter string) (v attrValues, ok bool) {

	if pm.attr == nil {
		return nil, false
	}

	var m attrValues
	if signerFilter != "" {
		m = pm.signer[signerFilter]
		if m == nil {
			return nil, true
		}
	} else {
		m = pm.attr
	}
	if at.IsZero() {
		return m, true
	}
	if n := len(pm.Claims); n == 0 || !pm.Claims[n-1].Date.After(at) {
		return m, true
	}
	return nil, false
}

func newCorpus() *corpusMem {
	c := &corpusMem{
		blobs:                   make(map[blob.Ref]*camtypes.BlobMeta),
		camBlobs:                make(map[schema.CamliType]map[blob.Ref]*camtypes.BlobMeta),
		files:                   make(map[blob.Ref]camtypes.FileInfo),
		permanodes:              make(map[blob.Ref]*PermanodeMeta),
		imageInfo:               make(map[blob.Ref]camtypes.ImageInfo),
		deletedBy:               make(map[blob.Ref]blob.Ref),
		keyId:                   make(map[blob.Ref]string),
		signerRefs:              make(map[string]SignerRefSet),
		brOfStr:                 make(map[string]blob.Ref),
		fileWholeRef:            make(map[blob.Ref]blob.Ref),
		gps:                     make(map[blob.Ref]latLong),
		mediaTags:               make(map[blob.Ref]map[string]string),
		deletes:                 make(map[blob.Ref][]deletion),
		claimBack:               make(map[blob.Ref][]*camtypes.Claim),
		permanodesSetByNodeType: make(map[string]map[blob.Ref]bool),
		dirChildren:             make(map[blob.Ref]map[blob.Ref]struct{}),
		fileParents:             make(map[blob.Ref]map[blob.Ref]struct{}),
	}
	c.permanodesByModtime = &lazySortedPermanodes{
		c:      c,
		pnTime: c.PermanodeModtime,
	}
	c.permanodesByTime = &lazySortedPermanodes{
		c:      c,
		pnTime: c.PermanodeAnyTime,
	}
	return c
}

func NewCorpusFromStorage(s sorted.KeyValue) (Corpus, error) {
	if s == nil {
		return nil, errors.New("storage is nil")
	}
	return newMemCorpusFromStorage(s)
}

func (x *Index) KeepInMemory() (Corpus, error) {
	var err error
	x.corpus, err = NewCorpusFromStorage(x.s)
	return x.corpus, err
}

// SetCorpus sets c as the corpus used by the index to answer queries and
// to apply blob mutations. It must be called before the index is used for
// serving requests (and it is not safe to call concurrently with other
// index methods).
func (x *Index) SetCorpus(c Corpus) {
	x.corpus = c
}

// PreventStorageAccessForTesting causes any access to the index's underlying
// Storage interface to panic.
func (x *Index) PreventStorageAccessForTesting() {
	x.s = crashStorage{}
}

type crashStorage struct {
	sorted.KeyValue
}

func (crashStorage) Get(key string) (string, error) {
	panic(fmt.Sprintf("unexpected KeyValue.Get(%q) called", key))
}

func (crashStorage) Find(start, end string) sorted.Iterator {
	panic(fmt.Sprintf("unexpected KeyValue.Find(%q, %q) called", start, end))
}

func memstats() *runtime.MemStats {
	ms := new(runtime.MemStats)
	runtime.GC()
	runtime.ReadMemStats(ms)
	return ms
}

var logCorpusStats = true // set to false in tests

var slurpPrefixes = []string{
	"meta:", // must be first
	keySignerKeyID.name + ":",

	// the first two above are loaded serially first for dependency reasons, whereas
	// the others below are loaded concurrently afterwards.
	"claim|",
	"fileinfo|",
	keyFileTimes.name + "|",
	"imagesize|",
	"wholetofile|",
	"exifgps|",
	"mediatag|",
	keyStaticDirChild.name + "|",
}

// Key types (without trailing punctuation) that we slurp to memory at start.
var slurpedKeyType = make(map[string]bool)

func init() {
	for _, prefix := range slurpPrefixes {
		slurpedKeyType[typeOfKey(prefix)] = true
	}
}

// pnAndTime is a value type wrapping a permanode blobref and its modtime.
// It's used by EnumeratePermanodesLastModified and EnumeratePermanodesCreated.
type pnAndTime struct {
	pn blob.Ref
	t  time.Time
}

type byPermanodeTime []pnAndTime

func (s byPermanodeTime) Len() int      { return len(s) }
func (s byPermanodeTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byPermanodeTime) Less(i, j int) bool {
	if s[i].t.Equal(s[j].t) {
		return s[i].pn.Less(s[j].pn)
	}
	return s[i].t.Before(s[j].t)
}

type lazySortedPermanodes struct {
	c      Corpus
	pnTime func(blob.Ref) (time.Time, bool) // returns permanode's time (if any) to sort on

	mu                  sync.Mutex  // guards sortedCache and ofGen
	sortedCache         []pnAndTime // nil if invalidated
	sortedCacheReversed []pnAndTime // nil if invalidated
	ofGen               int64       // the Corpus.gen from which sortedCache was built
}

func reversedCopy(original []pnAndTime) []pnAndTime {
	l := len(original)
	reversed := make([]pnAndTime, l)
	for k, v := range original {
		reversed[l-1-k] = v
	}
	return reversed
}

func (lsp *lazySortedPermanodes) sorted(reverse bool) []pnAndTime {
	lsp.mu.Lock()
	defer lsp.mu.Unlock()
	if lsp.ofGen == lsp.c.Generation() {
		// corpus hasn't changed -> caches are still valid, if they exist.
		if reverse {
			if lsp.sortedCacheReversed != nil {
				return lsp.sortedCacheReversed
			}
			if lsp.sortedCache != nil {
				// using sortedCache to quickly build sortedCacheReversed
				lsp.sortedCacheReversed = reversedCopy(lsp.sortedCache)
				return lsp.sortedCacheReversed
			}
		}
		if !reverse {
			if lsp.sortedCache != nil {
				return lsp.sortedCache
			}
			if lsp.sortedCacheReversed != nil {
				// using sortedCacheReversed to quickly build sortedCache
				lsp.sortedCache = reversedCopy(lsp.sortedCacheReversed)
				return lsp.sortedCache
			}
		}
	}
	// invalidate the caches
	lsp.sortedCache = nil
	lsp.sortedCacheReversed = nil
	var pns []pnAndTime
	for pn := range lsp.c.IterPermanodes() {
		if lsp.c.IsDeleted(pn) {
			continue
		}
		if pt, ok := lsp.pnTime(pn); ok {
			pns = append(pns, pnAndTime{pn, pt})
		}
	}
	// and rebuild one of them
	if reverse {
		sort.Sort(sort.Reverse(byPermanodeTime(pns)))
		lsp.sortedCacheReversed = pns
	} else {
		sort.Sort(byPermanodeTime(pns))
		lsp.sortedCache = pns
	}
	lsp.ofGen = lsp.c.Generation()
	return pns
}

// SetVerboseCorpusLogging controls corpus setup verbosity. It's on by default
// but used to disable verbose logging in tests.
func SetVerboseCorpusLogging(v bool) {
	logCorpusStats = v
}
