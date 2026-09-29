/*
Copyright 2024 The Perkeep Authors

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

package index_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"go4.org/types"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/index"
	"perkeep.org/pkg/index/indextest"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/sorted"
	"perkeep.org/pkg/types/camtypes"
)

func TestDBCorpusFromStorage(t *testing.T) {
	kv := sorted.NewMemoryKeyValue()
	setRows(t, kv)
	mem, err := index.NewCorpusFromStorage(kv)
	if err != nil {
		t.Fatal(err)
	}
	dc, err := index.NewDBCorpusFromStorage(filepath.Join(t.TempDir(), "corpus.db"), kv)
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	compare(t, mem, dc)
}

// TestDBCorpus
// Receive checks that a dbcorpus attached to an index gets
// populated incrementally via AddBlob, and stays equivalent to the
// in-memory corpus built from the same index rows.
func TestDBCorpusReceive(t *testing.T) {
	kv := sorted.NewMemoryKeyValue()
	idx, err := index.New(kv)
	if err != nil {
		t.Fatal(err)
	}
	db, err := index.NewCorpusDB(filepath.Join(t.TempDir(), "corpus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	idx.SetCorpus(db)

	id := indextest.NewIndexDeps(idx)
	id.Fataler = t
	foopn := id.NewPlannedPermanode("foo")
	id.SetAttribute(foopn, "tag", "foo")
	id.SetAttribute(foopn, "camliNodeType", "testType")
	fileRef, _ := id.UploadFile("foo.txt", "some content", time.Unix(1382073153, 0).UTC())
	barpn := id.NewPlannedPermanode("bar")
	id.SetAttribute(barpn, "camliContent", fileRef.String())
	id.SetAttribute(barpn, "title", "bar title")
	bazpn := id.NewPlannedPermanode("baz")
	id.SetAttribute(bazpn, "tag", "baz")

	mem, err := index.NewCorpusFromStorage(kv)
	if err != nil {
		t.Fatal(err)
	}
	compare(t, mem, db)
}

func setRows(t *testing.T, kv sorted.KeyValue) {
	t.Helper()
	rows := map[string]string{
		// blobs
		"meta:" + pn.String():       "10|application/json; camliType=permanode",
		"meta:" + pn2.String():      "10|application/json; camliType=permanode",
		"meta:" + file.String():     "12|application/json; camliType=file",
		"meta:" + dir.String():      "5|application/json; camliType=directory",
		"meta:" + child.String():    "3|application/json; camliType=file",
		"meta:" + signer.String():   "4|application/pgp-keys",
		"meta:" + signer2.String():  "4|application/pgp-keys",
		"meta:" + c1.String():       "1|application/json; camliType=claim",
		"meta:" + c2.String():       "1|application/json; camliType=claim",
		"meta:" + c3.String():       "1|application/json; camliType=claim",
		"meta:" + c4.String():       "1|application/json; camliType=claim",
		"meta:" + c5.String():       "1|application/json; camliType=claim",
		"meta:" + c6.String():       "1|application/json; camliType=claim",
		"meta:" + c7.String():       "1|application/json; camliType=claim",
		"meta:" + delClaim.String(): "1|application/json; camliType=claim",
		"meta:" + whole.String():    "6|image/gif",

		// signer key IDs
		"signerkeyid:" + signer.String():  keyID,
		"signerkeyid:" + signer2.String(): keyID2,

		// file rows
		"fileinfo|" + file.String():                           "12|foo%2etxt|text%2fplain|" + whole.String(),
		"wholetofile|" + whole.String() + "|" + file.String(): "1",
		"filetimes|" + file.String():                          url.QueryEscape("1970-01-01T00:02:03Z,1970-01-01T00:04:05Z"),
		"imagesize|" + file.String():                          "100|200",
		"exifgps|" + whole.String():                           "-122.5|37.5",
		"mediatag|" + whole.String() + "|album":               "Some+Album+Name",

		// directory rows
		"fileinfo|" + dir.String():                        "1|dir|",
		"fileinfo|" + child.String():                      "1|child|",
		"dirchild|" + dir.String() + "|" + child.String(): "1",
	}
	add := func(pairs ...[2]string) {
		for _, p := range pairs {
			rows[p[0]] = p[1]
		}
	}
	add(
		// claims on pn
		claimRow(pn, signer, c1, 100, "set-attribute", "foo", "foov"),
		claimRow(pn, signer, c2, 101, "add-attribute", "tag", "a"),
		claimRow(pn, signer, c3, 102, "add-attribute", "tag", "b"),
		claimRow(pn, signer, c4, 103, "del-attribute", "tag", "a"),
		claimRow(pn, signer, c5, 104, "set-attribute", "camliNodeType", "testType"),
		claimRow(pn, signer, c6, 105, "set-attribute", "camliContent", file.String()),
		// claim on pn2, and pn2's delete
		claimRow(pn2, signer, c7, 150, "set-attribute", "tag", "x"),
		deletedRow(pn2, delClaim, 200),
	)
	for k, v := range rows {
		if err := kv.Set(k, v); err != nil {
			t.Fatalf("Set(%q): %v", k, err)
		}
	}
}

var (
	pn       = blob.MustParse("abc-11")
	pn2      = blob.MustParse("abc-12")
	file     = blob.MustParse("abc-21")
	dir      = blob.MustParse("abc-22")
	child    = blob.MustParse("abc-23")
	signer   = blob.MustParse("abc-aa")
	signer2  = blob.MustParse("abc-bb")
	c1       = blob.MustParse("abc-31")
	c2       = blob.MustParse("abc-32")
	c3       = blob.MustParse("abc-33")
	c4       = blob.MustParse("abc-34")
	c5       = blob.MustParse("abc-35")
	c6       = blob.MustParse("abc-36")
	c7       = blob.MustParse("abc-37")
	delClaim = blob.MustParse("abc-41")
	whole    = blob.MustParse("sha1-" + strings.Repeat("a", 40))

	keyID  = "2931A67C26F5ABDA"
	keyID2 = "0123456789ABCDEF"
)

func claimRow(pn, signer, claim blob.Ref, sec int64, typ, attr, value string) [2]string {
	k := "claim|" + pn.String() + "|" + keyID + "|" +
		time.Unix(sec, 0).UTC().Format(time.RFC3339) + "|" + claim.String()
	v := url.QueryEscape(typ) + "|" + url.QueryEscape(attr) + "|" + url.QueryEscape(value) + "|" + signer.String()
	return [2]string{k, v}
}

func deletedRow(target, deleter blob.Ref, sec int64) [2]string {
	t := time.Unix(sec, 0).UTC().Format(time.RFC3339)
	return [2]string{"deleted|" + target.String() + "|" + reverseTimeString(t) + "|" + deleter.String(), ""}
}

func reverseTimeString(s string) string {
	var b strings.Builder
	b.WriteString("rt")
	for i := 0; i < len(s); i++ {
		a := s[i]
		if a >= '0' && a <= '9' {
			b.WriteByte(byte('0' + ('9' - a)))
		} else {
			b.WriteByte(a)
		}
	}
	return b.String()
}

// compare checks that the db corpus is equivalent to the in-memory
// corpus mem for all the data reachable from mem.
func compare(t *testing.T, mem, db index.Corpus) {
	t.Helper()
	ctx := context.Background()

	refs := collectRefs(mem.EnumerateBlobMeta)
	if got := collectRefs(db.EnumerateBlobMeta); !reflect.DeepEqual(refs, got) {
		t.Errorf("EnumerateBlobMeta: mem=%v db=%v", refs, got)
	}

	camliTypes := map[string]bool{"": true}
	for _, br := range refs {
		bm, err := mem.GetBlobMeta(ctx, br)
		if err != nil {
			t.Fatalf("mem.GetBlobMeta(%v): %v", br, err)
		}
		if bm.CamliType != "" {
			camliTypes[string(bm.CamliType)] = true
		}
	}
	for typ := range camliTypes {
		m := func(fn func(camtypes.BlobMeta) bool) { mem.EnumerateCamliBlobs(schema.CamliType(typ), fn) }
		d := func(fn func(camtypes.BlobMeta) bool) { db.EnumerateCamliBlobs(schema.CamliType(typ), fn) }
		if got, want := collectRefs(d), collectRefs(m); !reflect.DeepEqual(got, want) {
			t.Errorf("EnumerateCamliBlobs(%q): mem[%d]=%v db[%d]=%v", typ, len(want), want, len(got), got)
		}
	}

	for _, br := range refs {
		m, merr := mem.GetBlobMeta(ctx, br)
		d, derr := db.GetBlobMeta(ctx, br)
		if !sameErr(merr, derr) || !reflect.DeepEqual(m, d) {
			t.Errorf("GetBlobMeta(%v): mem=(%+v,%v) db=(%+v,%v)", br, m, merr, d, derr)
		}
		if got, want := db.IsDeleted(br), mem.IsDeleted(br); got != want {
			t.Errorf("IsDeleted(%v): mem=%v db=%v", br, want, got)
		}
	}

	old := time.Time{}
	future := time.Unix(1<<40, 0).UTC()
	signers := map[blob.Ref]bool{}
	nodeTypes := map[string]bool{}
	for _, pn := range refsOfType(ctx, t, mem, refs, "permanode") {
		if got, want := collectClaims(db, pn), collectClaims(mem, pn); !reflect.DeepEqual(got, want) {
			t.Errorf("AppendClaims(%v): mem=%v db=%v", pn, want, got)
		}
		for _, at := range []time.Time{old, time.Unix(102, 0).UTC(), time.Unix(200, 0).UTC(), future} {
			mm, mc := mem.PermanodeAttrsOrClaims(pn, at, "")
			dm, dcl := db.PermanodeAttrsOrClaims(pn, at, "")
			if !reflect.DeepEqual(mm, dm) || !reflect.DeepEqual(claimRefs(mc), claimRefs(dcl)) {
				t.Errorf("PermanodeAttrsOrClaims(%v,%v): mem=(%v,%v) db=(%v,%v)",
					pn, at, mm, claimRefs(mc), dm, claimRefs(dcl))
			}
		}
		mt, mok := mem.PermanodeModtime(pn)
		dt, dok := db.PermanodeModtime(pn)
		if !mt.Equal(dt) || mok != dok {
			t.Errorf("PermanodeModtime(%v): mem=(%v,%v) db=(%v,%v)", pn, mt, mok, dt, dok)
		}
		mt, mok = mem.PermanodeAnyTime(pn)
		dt, dok = db.PermanodeAnyTime(pn)
		if !mt.Equal(dt) || mok != dok {
			t.Errorf("PermanodeAnyTime(%v): mem=(%v,%v) db=(%v,%v)", pn, mt, mok, dt, dok)
		}

		attrs := map[string]bool{}
		for _, cl := range collectClaims(mem, pn) {
			attrs[cl.Attr] = true
			signers[cl.Signer] = true
			if cl.Attr == "camliNodeType" {
				nodeTypes[cl.Value] = true
			}
		}
		for attr := range attrs {
			for _, at := range []time.Time{old, time.Unix(102, 0).UTC(), time.Unix(200, 0).UTC(), future} {
				if got, want := mem.PermanodeAttrValue(pn, attr, at, ""), db.PermanodeAttrValue(pn, attr, at, ""); got != want {
					t.Errorf("PermanodeAttrValue(%v,%q,%v): mem=%q db=%q", pn, attr, at, want, got)
				}
				if got, want := mem.PermanodeHasAttrValue(pn, at, attr, "a"), db.PermanodeHasAttrValue(pn, at, attr, "a"); got != want {
					t.Errorf("PermanodeHasAttrValue(%v,%q,%v): mem=%v db=%v", pn, attr, at, want, got)
				}
				mg := mem.AppendPermanodeAttrValues(nil, pn, attr, at, "")
				dg := db.AppendPermanodeAttrValues(nil, pn, attr, at, "")
				if !reflect.DeepEqual(mg, dg) {
					t.Errorf("AppendPermanodeAttrValues(%v,%q,%v): mem=%v db=%v", pn, attr, at, mg, dg)
				}
			}
		}
		if got, want := collectForeachClaim(db, pn), collectForeachClaim(mem, pn); !reflect.DeepEqual(got, want) {
			t.Errorf("ForeachClaim(%v): mem=%v db=%v", pn, want, got)
		}
	}

	// ForeachClaimBack and KeyId for every claim value/signer.
	for _, br := range refs {
		if got, want := collectForeachBack(db, br), collectForeachBack(mem, br); !reflect.DeepEqual(got, want) {
			t.Errorf("ForeachClaimBack(%v): mem=%v db=%v", br, want, got)
		}
	}
	for s := range signers {
		mk, merr := mem.KeyId(ctx, s)
		dk, derr := db.KeyId(ctx, s)
		if !sameErr(merr, derr) || mk != dk {
			t.Errorf("KeyId(%v): mem=(%q,%v) db=(%q,%v)", s, mk, merr, dk, derr)
		}
		if got, want := db.SignerRefs(mk), mem.SignerRefs(mk); !reflect.DeepEqual(got, want) {
			t.Errorf("SignerRefs(%q): mem=%v db=%v", mk, want, got)
		}
	}
	if got, want := db.HasLegacySHA1(), mem.HasLegacySHA1(); got != want {
		t.Errorf("HasLegacySHA1: mem=%v db=%v", want, got)
	}

	// File and directory lookups.
	files := refsOfType(ctx, t, mem, refs, "file")
	files = append(files, refsOfType(ctx, t, mem, refs, "directory")...)
	for _, f := range files {
		mfi, merr := mem.GetFileInfo(ctx, f)
		dfi, derr := db.GetFileInfo(ctx, f)
		if !sameErr(merr, derr) || !sameFileInfo(mfi, dfi) {
			t.Errorf("GetFileInfo(%v): mem=(%+v,%v) db=(%+v,%v)", f, mfi, merr, dfi, derr)
		}
		mii, merr := mem.GetImageInfo(ctx, f)
		dii, derr := db.GetImageInfo(ctx, f)
		if !sameErr(merr, derr) || !reflect.DeepEqual(mii, dii) {
			t.Errorf("GetImageInfo(%v): mem=(%+v,%v) db=(%+v,%v)", f, mii, merr, dii, derr)
		}
		mtags, merr := mem.GetMediaTags(ctx, f)
		dtags, derr := db.GetMediaTags(ctx, f)
		if !sameErr(merr, derr) || !reflect.DeepEqual(mtags, dtags) {
			t.Errorf("GetMediaTags(%v): mem=(%v,%v) db=(%v,%v)", f, mtags, merr, dtags, derr)
		}
		mwr, mok := mem.GetWholeRef(ctx, f)
		dwr, dok := db.GetWholeRef(ctx, f)
		if mwr != dwr || mok != dok {
			t.Errorf("GetWholeRef(%v): mem=(%v,%v) db=(%v,%v)", f, mwr, mok, dwr, dok)
		}
		mlat, mlong, mok := mem.FileLatLong(f)
		dlat, dlong, dok := db.FileLatLong(f)
		if mlat != dlat || mlong != dlong || mok != dok {
			t.Errorf("FileLatLong(%v): mem=(%v,%v,%v) db=(%v,%v,%v)", f, mlat, mlong, mok, dlat, dlong, dok)
		}
		md, merr := mem.GetDirChildren(ctx, f)
		dd, derr := db.GetDirChildren(ctx, f)
		if !sameErr(merr, derr) || !reflect.DeepEqual(md, dd) {
			t.Errorf("GetDirChildren(%v): mem=(%v,%v) db=(%v,%v)", f, md, merr, dd, derr)
		}
		mp, merr := mem.GetParentDirs(ctx, f)
		dp, derr := db.GetParentDirs(ctx, f)
		if !sameErr(merr, derr) || !reflect.DeepEqual(mp, dp) {
			t.Errorf("GetParentDirs(%v): mem=(%v,%v) db=(%v,%v)", f, mp, merr, dp, derr)
		}
	}

	// Permanode enumerations.
	if got, want := collectRefs(mem.EnumeratePermanodesLastModified), collectRefs(db.EnumeratePermanodesLastModified); !reflect.DeepEqual(got, want) {
		t.Errorf("EnumeratePermanodesLastModified: mem=%v db=%v", got, want)
	}
	mCreated := func(fn func(camtypes.BlobMeta) bool) { mem.EnumeratePermanodesCreated(fn, true) }
	dCreated := func(fn func(camtypes.BlobMeta) bool) { db.EnumeratePermanodesCreated(fn, true) }
	if got, want := collectRefs(mCreated), collectRefs(dCreated); !reflect.DeepEqual(got, want) {
		t.Errorf("EnumeratePermanodesCreated: mem=%v db=%v", got, want)
	}
	for typ := range nodeTypes {
		m := func(fn func(camtypes.BlobMeta) bool) { mem.EnumeratePermanodesByNodeTypes(fn, []string{typ}) }
		d := func(fn func(camtypes.BlobMeta) bool) { db.EnumeratePermanodesByNodeTypes(fn, []string{typ}) }
		if got, want := collectRefs(d), collectRefs(m); !reflect.DeepEqual(got, want) {
			t.Errorf("EnumeratePermanodesByNodeTypes(%q): mem=%v db=%v", typ, want, got)
		}
	}
}

func refsOfType(ctx context.Context, t *testing.T, c index.Corpus, refs []blob.Ref, camliType string) []blob.Ref {
	t.Helper()
	var out []blob.Ref
	for _, br := range refs {
		bm, err := c.GetBlobMeta(ctx, br)
		if err != nil {
			t.Fatalf("GetBlobMeta(%v): %v", br, err)
		}
		if string(bm.CamliType) == camliType {
			out = append(out, br)
		}
	}
	return out
}

func collectRefs(enum func(func(camtypes.BlobMeta) bool)) []blob.Ref {
	var refs []blob.Ref
	enum(func(bm camtypes.BlobMeta) bool {
		refs = append(refs, bm.Ref)
		return true
	})
	sortRefs(refs)
	return refs
}

func collectClaims(c index.Corpus, pn blob.Ref) []camtypes.Claim {
	claims, err := c.AppendClaims(context.Background(), nil, pn, "", "")
	if err != nil {
		return nil
	}
	for i := range claims {
		claims[i].Date = claims[i].Date.UTC()
	}
	sort.Slice(claims, func(i, j int) bool { return claims[i].BlobRef.Less(claims[j].BlobRef) })
	return claims
}

func collectForeachClaim(c index.Corpus, pn blob.Ref) []blob.Ref {
	var refs []blob.Ref
	c.ForeachClaim(pn, time.Time{}, func(cl *camtypes.Claim) bool {
		refs = append(refs, cl.BlobRef)
		return true
	})
	sortRefs(refs)
	return refs
}

func collectForeachBack(c index.Corpus, value blob.Ref) []blob.Ref {
	var refs []blob.Ref
	c.ForeachClaimBack(value, time.Time{}, func(cl *camtypes.Claim) bool {
		refs = append(refs, cl.BlobRef)
		return true
	})
	sortRefs(refs)
	return refs
}

func claimRefs(claims []*camtypes.Claim) []blob.Ref {
	var refs []blob.Ref
	for _, cl := range claims {
		refs = append(refs, cl.BlobRef)
	}
	sortRefs(refs)
	return refs
}

func sortRefs(refs []blob.Ref) {
	sort.Slice(refs, func(i, j int) bool { return refs[i].Less(refs[j]) })
}

func sameFileInfo(a, b camtypes.FileInfo) bool {
	if a.FileName != b.FileName || a.Size != b.Size || a.MIMEType != b.MIMEType || a.WholeRef != b.WholeRef {
		return false
	}
	return sameTimePtr(a.Time, b.Time) && sameTimePtr(a.ModTime, b.ModTime)
}

func sameTimePtr(a, b *types.Time3339) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return time.Time(*a).Equal(time.Time(*b))
}

func sameErr(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return os.IsNotExist(a) == os.IsNotExist(b) && a.Error() == b.Error()
}
