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

// Package dbcorpus implements Corpus, storing all of a user's blobs'
// metadata in a database/sql (SQLite) database instead of in memory.
package index // import "perkeep.org/pkg/index/dbcorpus"

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"go4.org/types"
	"modernc.org/sqlite"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/schema/nodeattr"
	"perkeep.org/pkg/sorted"
	"perkeep.org/pkg/types/camtypes"
)

// NewDBCorpusFromStorage returns a Corpus backed by a new SQLite database at
// file, populated from all the index rows in s.
func NewDBCorpusFromStorage(file string, s sorted.KeyValue) (*corpusDB, error) {
	c, err := NewCorpusDB(file)
	if err != nil {
		return nil, err
	}
	if err := c.ScanFromStorage(s); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// corpusDB is an corpusDB implementation backed by a SQLite database.
//
// A corpusDB is safe for concurrent use, as long as AddBlob is not invoked
// concurrently for the same blob.
type corpusDB struct {
	db *sql.DB

	// gen is incremented on every blob received.
	// It's used as a query cache invalidator.
	gen int64

	// building is true while scanning all rows of the index in
	// ScanFromStorage. Like in the in-memory corpus, some invariants are
	// only settled at the end of the scan.
	building bool

	// hasLegacySHA1 reports whether the index rows scanned so far contain
	// SHA-1 blobs.
	hasLegacySHA1 bool
}

var _ Corpus = (*corpusDB)(nil)

// NewCorpusDB returns a Corpus backed by the SQLite database at file, creating it
// (with its schema) if needed.
func NewCorpusDB(file string) (*corpusDB, error) {
	db, err := openDB(file)
	if err != nil {
		return nil, err
	}
	c := &corpusDB{db: db}
	rows, err := db.Query(`SELECT metakey, value FROM meta`)
	if err != nil {
		db.Close()
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			db.Close()
			return nil, err
		}
		switch k {
		case metaHasLegacySHA1:
			c.hasLegacySHA1 = v == valHasLegacySHA1
		case metaGeneration:
			c.gen, _ = strconv.ParseInt(v, 10, 64)
		default:
		}
	}
	return c, rows.Close()
}

// Close closes the underlying database.
func (c *corpusDB) Close() error {
	if c == nil {
		return nil
	}
	db := c.db
	c.db = nil
	if db == nil {
		return nil
	}
	return db.Close()
}

func (c *corpusDB) Generation() int64 { return c.gen }

// *********** Updating the corpus

// scanPrefixes lists the index row key prefixes slurped into the corpus,
// in the order in which they must be scanned: "meta" rows first (they
// populate the blobs), then "signerkeyid" rows (needed to merge claims),
// then the rest.
var scanPrefixes = []string{
	"meta:",
	keySignerKeyIDName + ":",
	"claim|",
	"fileinfo|",
	"filetimes|",
	"imagesize|",
	"wholetofile|",
	"exifgps|",
	"mediatag|",
	"dirchild|",
}

// ScanFromStorage populates the corpus from all the rows of the index
// key/value store s. It is meant to be used on an empty corpus.
func (c *corpusDB) ScanFromStorage(s sorted.KeyValue) error {
	if s == nil {
		return errors.New("storage is nil")
	}
	c.building = true
	defer func() { c.building = false }()

	for _, prefix := range scanPrefixes {
		if err := c.scanPrefix(s, prefix); err != nil {
			return err
		}
	}
	if err := c.initDeletes(s); err != nil {
		return fmt.Errorf("Could not populate the corpus deletes: %w", err)
	}
	return nil
}

func (c *corpusDB) scanPrefix(s sorted.KeyValue, prefix string) (err error) {
	it := s.Find(prefix, prefixEnd(prefix))
	defer closeIterator(it, &err)
	var tx *sql.Tx
	defer func() {
		if tx != nil {
			tx.Commit()
		}
	}()
	start := time.Now()
	var batchSize int
	for it.Next() {
		if tx == nil {
			if tx, err = c.db.Begin(); err != nil {
				return err
			}
		}
		if err := c.mergeRow(tx, []byte(it.Key()), []byte(it.Value())); err != nil {
			return err
		}
		batchSize++
		if batchSize > 32767 {
			now := time.Now()
			log.Printf("%d: %s", batchSize, now.Sub(start))
			start = now
			batchSize = 0
			if err = tx.Commit(); err != nil {
				return err
			}
			tx = nil
		}
	}
	if tx != nil {
		return tx.Commit()
	}
	return nil
}

// prefixEnd returns the smallest string greater than every string that has
// the given prefix.
func prefixEnd(prefix string) string {
	if prefix == "" {
		return ""
	}
	return prefix[:len(prefix)-1] + string(prefix[len(prefix)-1]+1)
}

// initDeletes populates the corpus deletes from the "deleted" rows in s.
func (c *corpusDB) initDeletes(s sorted.KeyValue) (err error) {
	it := s.Find("deleted|", prefixEnd("deleted|"))
	defer closeIterator(it, &err)
	for it.Next() {
		cl, ok := parseDeletedKey(it.Key())
		if !ok {
			return fmt.Errorf("Bogus keyDeleted entry key: want |\"deleted\"|<deleted blobref>|<reverse claimdate>|<deleter claim>|, got %q", it.Key())
		}
		if _, err := c.db.Exec(`INSERT OR IGNORE INTO deletes (deleted, deleter, deltime) VALUES (?, ?, ?)`,
			cl.Target.String(), cl.BlobRef.String(), cl.Date.UnixNano()); err != nil {
			return err
		}
	}
	return nil
}

// AddBlob applies to the corpus the mutations that were committed to the
// index for the received blob br.
func (c *corpusDB) AddBlob(ctx context.Context, br blob.Ref, mm *mutationMap) error {
	var exists bool
	if err := c.db.QueryRow(
		`SELECT 1 FROM blobs_`+refPartition(br)+` WHERE ref = ?`,
		br.String(),
	).Scan(&exists); err == nil {
		// already known.
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	start := time.Now()
	defer func() { log.Printf("AddBlob(%s): %s", br, time.Since(start)) }()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Make sure the signerkeyid entry is added first: the signer
	// blobRef-signerID relation needs to be known before the claim
	// mutations themselves.
	if signerRef := mm.SignerBlobRef(); mm.SignerID() != "" && signerRef.Valid() {
		if err := c.addKeyID(tx, signerRef, mm.SignerID()); err != nil {
			return err
		}
	}
	for k, v := range mm.Rows() {
		if strings.HasPrefix(k, keySignerKeyIDName+":") {
			// because we already took care of it in addKeyID.
			continue
		}
		if err := c.mergeRow(tx, []byte(k), []byte(v)); err != nil {
			return err
		}
	}
	for _, cl := range mm.Deletes() {
		if err := c.updateDeletes(tx, cl); err != nil {
			return fmt.Errorf("Could not update the deletes cache after deletion from %v: %w", cl, err)
		}
	}
	c.gen++
	return tx.Commit()
}

func (c *corpusDB) addKeyID(tx dbtx, signerBlobRef blob.Ref, signerID string) error {
	if signerID == "" || !signerBlobRef.Valid() {
		return nil
	}
	var existing string
	err := tx.QueryRow(`SELECT keyid FROM signers WHERE signerref = ?`, signerBlobRef.String()).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.Exec(`INSERT INTO signers (signerref, keyid) VALUES (?, ?)`, signerBlobRef.String(), signerID)
		return err
	case err != nil:
		return err
	}
	if existing != signerID {
		return fmt.Errorf("GPG ID mismatch for signer %v: refusing to overwrite %v with %v", signerBlobRef, existing, signerID)
	}
	return nil
}

// updateDeletes updates the corpus deletes with the delete claim
// deleteClaim, which is trusted to be a valid delete Claim.
func (c *corpusDB) updateDeletes(tx dbtx, deleteClaim schema.Claim) error {
	target := deleteClaim.Target()
	deleter := deleteClaim.Blob()
	when, err := deleter.ClaimDate()
	if err != nil {
		return fmt.Errorf("Could not get date of delete claim %v: %w", deleteClaim, err)
	}
	_, err = tx.Exec(`INSERT OR IGNORE INTO deletes (deleted, deleter, deltime) VALUES (?, ?, ?)`,
		target.String(), deleter.BlobRef().String(), when.UnixNano())
	return err
}

// mergeRow applies one index row (k, v) to the corpus.
func (c *corpusDB) mergeRow(db dbtx, k, v []byte) error {
	switch {
	case bytes.HasPrefix(k, []byte("meta:")):
		return c.mergeMetaRow(db, k, v)
	case bytes.HasPrefix(k, []byte(keySignerKeyIDName+":")):
		return c.mergeSignerKeyIDRow(db, k, v)
	case bytes.HasPrefix(k, []byte("claim|")):
		return c.mergeClaimRow(db, k, v)
	case bytes.HasPrefix(k, []byte("fileinfo|")):
		return c.mergeFileInfoRow(db, k, v)
	case bytes.HasPrefix(k, []byte("filetimes|")):
		return c.mergeFileTimesRow(db, k, v)
	case bytes.HasPrefix(k, []byte("imagesize|")):
		return c.mergeImageSizeRow(db, k, v)
	case bytes.HasPrefix(k, []byte("wholetofile|")):
		return c.mergeWholeToFileRow(db, k, v)
	case bytes.HasPrefix(k, []byte("exifgps|")):
		return c.mergeEXIFGPSRow(db, k, v)
	case bytes.HasPrefix(k, []byte("mediatag|")):
		return c.mergeMediaTagRow(db, k, v)
	case bytes.HasPrefix(k, []byte("dirchild|")):
		return c.mergeStaticDirChildRow(db, k, v)
	default:
		// "have", "recpn", "signerattrvalue", and "exiftag" rows are not
		// represented in the corpus.
		return nil
	}
}

func (c *corpusDB) mergeMetaRow(db dbtx, k, v []byte) error {
	// "meta:<ref>" -> "<size>|<mime>"
	br, ok := blob.ParseBytes(k[len("meta:"):])
	if !ok {
		return fmt.Errorf("bogus meta row: %q -> %q", k, v)
	}
	pipe := bytes.IndexByte(v, '|')
	if pipe < 0 {
		return fmt.Errorf("bogus meta row: %q -> %q", k, v)
	}
	size, err := strconv.ParseUint(string(v[:pipe]), 10, 32)
	if err != nil {
		return fmt.Errorf("bogus meta row: %q -> %q", k, v)
	}
	if _, err = db.Exec(
		`INSERT OR IGNORE INTO blobs_`+refPartition(br)+` (ref, size, camlitype) VALUES (?, ?, ?)`,
		br.String(), size, string(camliTypeFromMIME(string(v[pipe+1:]))),
	); err != nil {
		if e, ok := errors.AsType[*sqlite.Error](err); ok && e.Code() == 1555 {
			return nil
		}
		return err
	}
	return nil
}

func (c *corpusDB) mergeSignerKeyIDRow(db dbtx, k, v []byte) error {
	br, ok := blob.ParseBytes(k[len(keySignerKeyIDName+":"):])
	if !ok {
		return fmt.Errorf("bogus signerid row: %q -> %q", k, v)
	}
	return c.addKeyID(db, br, string(v))
}

func (c *corpusDB) mergeClaimRow(db dbtx, k, v []byte) error {
	cl, ok := parseClaimBytes(k, v)
	if !ok || !cl.Permanode.Valid() {
		return fmt.Errorf("bogus claim row: %q -> %q", k, v)
	}
	_, err := db.Exec(`INSERT OR REPLACE INTO claims
		(claimref, permanode, signerref, date, type, attr, value)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		cl.BlobRef.String(), cl.Permanode.String(), cl.Signer.String(),
		cl.Date.UnixNano(), cl.Type, cl.Attr, cl.Value)
	return err
}

func (c *corpusDB) mergeFileInfoRow(db dbtx, k, v []byte) error {
	// "fileinfo|<fileref>" -> "<size>|<filename>|<mimetype>[|<wholeref>]"
	pipe := bytes.IndexByte(k, '|')
	if pipe < 0 {
		return fmt.Errorf("unexpected fileinfo key %q", k)
	}
	br, ok := blob.ParseBytes(k[pipe+1:])
	if !ok {
		return fmt.Errorf("unexpected fileinfo blobref in key %q", k)
	}
	fields := strings.Split(string(v), "|")
	if len(fields) != 3 && len(fields) != 4 {
		return fmt.Errorf("unexpected fileinfo value %q", v)
	}
	size, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return fmt.Errorf("unexpected fileinfo value %q", v)
	}
	var wholeRef string
	if len(fields) == 4 && fields[3] != "" {
		wr, ok := blob.Parse(urld(fields[3]))
		if !ok {
			return fmt.Errorf("invalid wholeRef blobref in value %q for fileinfo key %q", v, k)
		}
		wholeRef = wr.String()
	}
	_, err = db.Exec(`INSERT INTO files (fileref, size, filename, mimetype, wholeref)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(fileref) DO UPDATE SET
			size = excluded.size,
			filename = excluded.filename,
			mimetype = excluded.mimetype,
			wholeref = excluded.wholeref`,
		br.String(), size, urld(fields[1]), urld(fields[2]), wholeRef)
	return err
}

func (c *corpusDB) mergeFileTimesRow(db dbtx, k, v []byte) error {
	if len(v) == 0 {
		return nil
	}
	// "filetimes|<fileref>" -> "<time3339>[,<time3339>]"
	pipe := bytes.IndexByte(k, '|')
	if pipe < 0 {
		return fmt.Errorf("unexpected filetimes key %q", k)
	}
	br, ok := blob.ParseBytes(k[pipe+1:])
	if !ok {
		return fmt.Errorf("unexpected filetimes blobref in key %q", k)
	}
	times := strings.Split(urld(string(v)), ",")
	if _, err := db.Exec(`INSERT OR IGNORE INTO files (fileref) VALUES (?)`, br.String()); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE files SET time = ? WHERE fileref = ?`,
		time3339OrNilNanos(times[0]), br.String()); err != nil {
		return err
	}
	if len(times) == 2 {
		if _, err := db.Exec(`UPDATE files SET modtime = ? WHERE fileref = ?`,
			time3339OrNilNanos(times[1]), br.String()); err != nil {
			return err
		}
	}
	return nil
}

func (c *corpusDB) mergeImageSizeRow(db dbtx, k, v []byte) error {
	br, okk := blob.ParseBytes(k[len("imagesize|"):])
	ii, okv := parseImageInfo(v)
	if !okk || !okv {
		return fmt.Errorf("bogus row %q = %q", k, v)
	}
	_, err := db.Exec(`INSERT INTO imagesizes (fileref, width, height) VALUES (?, ?, ?)
		ON CONFLICT(fileref) DO UPDATE SET width = excluded.width, height = excluded.height`,
		br.String(), ii.Width, ii.Height)
	return err
}

func (c *corpusDB) mergeWholeToFileRow(db dbtx, k, v []byte) error {
	// "wholetofile|<wholeref>|<fileref>" -> "1"
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
	if _, err := db.Exec(`INSERT INTO wholetofile (fileref, wholeref) VALUES (?, ?)
		ON CONFLICT(fileref) DO UPDATE SET wholeref = excluded.wholeref`,
		fileRef.String(), wholeRef.String()); err != nil {
		return err
	}
	if c.building && !c.hasLegacySHA1 && bytes.HasPrefix(pair, sha1Prefix) {
		c.hasLegacySHA1 = true
		if _, err := db.Exec(`INSERT OR REPLACE INTO meta (metakey, value) VALUES (?, ?)`,
			metaHasLegacySHA1, valHasLegacySHA1); err != nil {
			return err
		}
	}
	return nil
}

func (c *corpusDB) mergeEXIFGPSRow(db dbtx, k, v []byte) error {
	// "exifgps|<wholeref>" -> "<lat>|<long>"
	wholeRef, ok := blob.ParseBytes(k[len("exifgps|"):])
	pipe := bytes.IndexByte(v, '|')
	if pipe < 0 || !ok {
		return fmt.Errorf("bogus row %q = %q", k, v)
	}
	lat, latErr := strconv.ParseFloat(string(v[:pipe]), 64)
	long, longErr := strconv.ParseFloat(string(v[pipe+1:]), 64)
	if latErr != nil || longErr != nil {
		if latErr != nil {
			log.Printf("dbcorpus: bogus latitude in value of row %q = %q", k, v)
		} else {
			log.Printf("dbcorpus: bogus longitude in value of row %q = %q", k, v)
		}
		return nil
	}
	_, err := db.Exec(`INSERT OR REPLACE INTO exifgps (wholeref, lat, long) VALUES (?, ?, ?)`,
		wholeRef.String(), lat, long)
	return err
}

func (c *corpusDB) mergeMediaTagRow(db dbtx, k, v []byte) error {
	// "mediatag|<wholeref>|<tag>" -> "<value>"
	f := strings.Split(string(k), "|")
	if len(f) != 3 {
		return fmt.Errorf("unexpected key %q", k)
	}
	wholeRef, ok := blob.Parse(f[1])
	if !ok {
		return fmt.Errorf("failed to parse wholeref from key %q", k)
	}
	_, err := db.Exec(`INSERT INTO mediatags (wholeref, tag, value) VALUES (?, ?, ?)
		ON CONFLICT(wholeref, tag) DO UPDATE SET value = excluded.value`,
		wholeRef.String(), f[2], urld(string(v)))
	return err
}

func (c *corpusDB) mergeStaticDirChildRow(db dbtx, k, v []byte) error {
	// "dirchild|<parent>|<child>" -> "1"
	sk := k[len("dirchild|"):]
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
	if _, err := db.Exec(`INSERT OR IGNORE INTO dirchildren (parent, child) VALUES (?, ?)`,
		parent.String(), child.String()); err != nil {
		return err
	}
	_, err := db.Exec(`INSERT OR IGNORE INTO fileparents (child, parent) VALUES (?, ?)`,
		child.String(), parent.String())
	return err
}

// *********** Reading from the corpus

func (c *corpusDB) IsDeleted(br blob.Ref) bool {
	return c.isDeleted(br.String())
}

func (c *corpusDB) isDeleted(target string) bool {
	rows, err := c.db.Query(`SELECT deleter FROM deletes WHERE deleted = ?`, target)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var deleter string
		if err := rows.Scan(&deleter); err != nil {
			continue
		}
		if !c.isDeleted(deleter) {
			return true
		}
	}
	return false
}

func (c *corpusDB) HasLegacySHA1() bool { return c.hasLegacySHA1 }

func (c *corpusDB) SignerRefs(keyID string) SignerRefSet {
	rows, err := c.db.Query(`SELECT signerref FROM signers WHERE keyid = ?`, keyID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var refs SignerRefSet
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return refs
		}
		refs = append(refs, ref)
	}
	return refs
}

func (c *corpusDB) KeyId(ctx context.Context, signer blob.Ref) (string, error) {
	var keyID string
	err := c.db.QueryRow(`SELECT keyid FROM signers WHERE signerref = ?`, signer.String()).Scan(&keyID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", sorted.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return keyID, nil
}

func (c *corpusDB) GetBlobMeta(ctx context.Context, br blob.Ref) (camtypes.BlobMeta, error) {
	var size uint64
	var camliType string
	err := c.db.QueryRow(
		`SELECT size, camlitype FROM blobs_`+refPartition(br)+` WHERE ref = ?`,
		br.String(),
	).Scan(&size, &camliType)
	if errors.Is(err, sql.ErrNoRows) {
		return camtypes.BlobMeta{}, os.ErrNotExist
	}
	if err != nil {
		return camtypes.BlobMeta{}, err
	}
	return camtypes.BlobMeta{
		Ref:       br,
		Size:      uint32(size),
		CamliType: schema.CamliType(camliType),
	}, nil
}

func (c *corpusDB) GetFileInfo(ctx context.Context, fileRef blob.Ref) (camtypes.FileInfo, error) {
	fi, ok, err := c.fileInfo(fileRef)
	if err != nil {
		return camtypes.FileInfo{}, err
	}
	if !ok {
		return camtypes.FileInfo{}, os.ErrNotExist
	}
	return fi, nil
}

func (c *corpusDB) fileInfo(fileRef blob.Ref) (camtypes.FileInfo, bool, error) {
	var (
		size           int64
		filename, mime string
		wholeRef       string
		timeN, modN    sql.NullInt64
	)
	err := c.db.QueryRow(`SELECT size, filename, mimetype, wholeref, time, modtime
		FROM files WHERE fileref = ?`, fileRef.String()).
		Scan(&size, &filename, &mime, &wholeRef, &timeN, &modN)
	if errors.Is(err, sql.ErrNoRows) {
		return camtypes.FileInfo{}, false, nil
	}
	if err != nil {
		return camtypes.FileInfo{}, false, err
	}
	fi := camtypes.FileInfo{
		Size:     size,
		FileName: filename,
		MIMEType: mime,
	}
	if wholeRef != "" {
		fi.WholeRef = blob.ParseOrZero(wholeRef)
	}
	if timeN.Valid {
		t := types.Time3339(time.Unix(0, timeN.Int64))
		fi.Time = &t
	}
	if modN.Valid {
		t := types.Time3339(time.Unix(0, modN.Int64))
		fi.ModTime = &t
	}
	return fi, true, nil
}

func (c *corpusDB) GetImageInfo(ctx context.Context, fileRef blob.Ref) (camtypes.ImageInfo, error) {
	var ii camtypes.ImageInfo
	var width, height uint64
	err := c.db.QueryRow(`SELECT width, height FROM imagesizes WHERE fileref = ?`, fileRef.String()).
		Scan(&width, &height)
	if errors.Is(err, sql.ErrNoRows) {
		return camtypes.ImageInfo{}, os.ErrNotExist
	}
	if err != nil {
		return camtypes.ImageInfo{}, err
	}
	ii.Width = uint16(width)
	ii.Height = uint16(height)
	return ii, nil
}

func (c *corpusDB) GetMediaTags(ctx context.Context, fileRef blob.Ref) (map[string]string, error) {
	wholeRef, ok := c.GetWholeRef(ctx, fileRef)
	if !ok {
		return nil, os.ErrNotExist
	}
	rows, err := c.db.Query(`SELECT tag, value FROM mediatags WHERE wholeref = ?`, wholeRef.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tags map[string]string
	for rows.Next() {
		var tag, value string
		if err := rows.Scan(&tag, &value); err != nil {
			return nil, err
		}
		if tags == nil {
			tags = make(map[string]string)
		}
		tags[tag] = value
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if tags == nil {
		return nil, os.ErrNotExist
	}
	return tags, nil
}

func (c *corpusDB) GetWholeRef(ctx context.Context, fileRef blob.Ref) (wholeRef blob.Ref, ok bool) {
	var s string
	err := c.db.QueryRow(`SELECT wholeref FROM wholetofile WHERE fileref = ?`, fileRef.String()).Scan(&s)
	if err != nil {
		return blob.Ref{}, false
	}
	br, ok := blob.Parse(s)
	return br, ok
}

func (c *corpusDB) FileLatLong(fileRef blob.Ref) (lat, long float64, ok bool) {
	wholeRef, ok := c.GetWholeRef(context.TODO(), fileRef)
	if !ok {
		return 0, 0, false
	}
	err := c.db.QueryRow(`SELECT lat, long FROM exifgps WHERE wholeref = ?`, wholeRef.String()).Scan(&lat, &long)
	if err != nil {
		return 0, 0, false
	}
	return lat, long, true
}

func (c *corpusDB) GetDirChildren(ctx context.Context, dirRef blob.Ref) (map[blob.Ref]struct{}, error) {
	children, err := c.childRefs(`SELECT child FROM dirchildren WHERE parent = ?`, dirRef)
	if err != nil {
		return nil, err
	}
	if children == nil {
		if _, ok, err := c.fileInfo(dirRef); err != nil {
			return nil, err
		} else if !ok {
			return nil, os.ErrNotExist
		}
	}
	return children, nil
}

func (c *corpusDB) GetParentDirs(ctx context.Context, childRef blob.Ref) (map[blob.Ref]struct{}, error) {
	parents, err := c.childRefs(`SELECT parent FROM fileparents WHERE child = ?`, childRef)
	if err != nil {
		return nil, err
	}
	if parents == nil {
		if _, ok, err := c.fileInfo(childRef); err != nil {
			return nil, err
		} else if !ok {
			return nil, os.ErrNotExist
		}
	}
	return parents, nil
}

func (c *corpusDB) childRefs(query string, br blob.Ref) (map[blob.Ref]struct{}, error) {
	rows, err := c.db.Query(query, br.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var m map[blob.Ref]struct{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if m == nil {
			m = make(map[blob.Ref]struct{})
		}
		m[blob.ParseOrZero(s)] = struct{}{}
	}
	return m, rows.Err()
}

// claimsOf returns the claims of the permanode pn, sorted by date.
func (c *corpusDB) claimsOf(pn blob.Ref) ([]*camtypes.Claim, error) {
	rows, err := c.db.Query(`SELECT claimref, signerref, permanode, date, type, attr, value
		FROM claims WHERE permanode = ? ORDER BY date, claimref`, pn.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var claims []*camtypes.Claim
	for rows.Next() {
		cl, err := scanClaim(rows)
		if err != nil {
			return nil, err
		}
		claims = append(claims, cl)
	}
	return claims, rows.Err()
}

func scanClaim(rows *sql.Rows) (*camtypes.Claim, error) {
	var (
		claimRef, signerRef, permanode string
		dateN                          int64
		typ, attr, value               string
	)
	if err := rows.Scan(&claimRef, &signerRef, &permanode, &dateN, &typ, &attr, &value); err != nil {
		return nil, err
	}
	return &camtypes.Claim{
		BlobRef:   blob.ParseOrZero(claimRef),
		Signer:    blob.ParseOrZero(signerRef),
		Permanode: blob.ParseOrZero(permanode),
		Date:      time.Unix(0, dateN),
		Type:      typ,
		Attr:      attr,
		Value:     value,
	}, nil
}

func (c *corpusDB) AppendClaims(ctx context.Context, dst []camtypes.Claim, permaNode blob.Ref,
	signerFilter string,
	attrFilter string) ([]camtypes.Claim, error) {
	claims, err := c.claimsOf(permaNode)
	if err != nil {
		return dst, err
	}
	var signerRefs SignerRefSet
	if signerFilter != "" {
		signerRefs = c.SignerRefs(signerFilter)
		if len(signerRefs) == 0 {
			return dst, nil
		}
	}
	for _, cl := range claims {
		if c.IsDeleted(cl.BlobRef) {
			continue
		}
		if len(signerRefs) > 0 && !signerRefsMatch(signerRefs, cl.Signer) {
			continue
		}
		if attrFilter != "" && cl.Attr != attrFilter {
			continue
		}
		dst = append(dst, *cl)
	}
	return dst, nil
}

func (c *corpusDB) AppendPermanodeAttrValues(dst []string,
	permaNode blob.Ref,
	attr string,
	at time.Time,
	signerFilter string) []string {
	if len(dst) > 0 {
		panic("len(dst) must be 0")
	}
	claims, err := c.claimsOf(permaNode)
	if err != nil || len(claims) == 0 {
		return dst
	}
	var signerRefs SignerRefSet
	if signerFilter != "" {
		signerRefs = c.SignerRefs(signerFilter)
		if len(signerRefs) == 0 {
			return dst
		}
	}
	return replayAttrValues(claims, attr, at, signerRefs, dst)
}

// replayAttrValues computes the values of attr from the (date-sorted)
// claims at time at, for the given signerRefs filter.
func replayAttrValues(claims []*camtypes.Claim, attr string, at time.Time, signerRefs SignerRefSet, dst []string) []string {
	if at.IsZero() {
		at = time.Now()
	}
	for _, cl := range claims {
		if cl.Attr != attr || cl.Date.After(at) {
			continue
		}
		if len(signerRefs) > 0 && !signerRefsMatch(signerRefs, cl.Signer) {
			continue
		}
		switch cl.Type {
		case string(schema.DelAttributeClaim):
			if cl.Value == "" {
				dst = dst[:0]
			} else {
				for i := 0; i < len(dst); i++ {
					if dst[i] == cl.Value {
						dst = append(dst[:i], dst[i+1:]...)
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

func (c *corpusDB) PermanodeAttrValue(permaNode blob.Ref,
	attr string,
	at time.Time,
	signerFilter string) string {
	values := c.AppendPermanodeAttrValues(nil, permaNode, attr, at, signerFilter)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (c *corpusDB) PermanodeHasAttrValue(pn blob.Ref, at time.Time, attr, val string) bool {
	claims, err := c.claimsOf(pn)
	if err != nil || len(claims) == 0 {
		return false
	}
	if at.IsZero() {
		at = time.Now()
	}
	ret := false
	for _, cl := range claims {
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
			ret = cl.Value == val
		case string(schema.AddAttributeClaim):
			if cl.Value == val {
				ret = true
			}
		}
	}
	return ret
}

func (c *corpusDB) ForeachClaim(permaNode blob.Ref, at time.Time, fn func(*camtypes.Claim) bool) {
	claims, err := c.claimsOf(permaNode)
	if err != nil {
		return
	}
	for _, cl := range claims {
		if !at.IsZero() && cl.Date.After(at) {
			continue
		}
		if !fn(cl) {
			return
		}
	}
}

func (c *corpusDB) ForeachClaimBack(value blob.Ref, at time.Time, fn func(*camtypes.Claim) bool) {
	rows, err := c.db.Query(`SELECT claimref, signerref, permanode, date, type, attr, value
		FROM claims WHERE value = ? ORDER BY date, claimref`, value.String())
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		cl, err := scanClaim(rows)
		if err != nil {
			return
		}
		if !at.IsZero() && cl.Date.After(at) {
			continue
		}
		if !fn(cl) {
			return
		}
	}
}

func (c *corpusDB) PermanodeModtime(pn blob.Ref) (t time.Time, ok bool) {
	claims, err := c.claimsOf(pn)
	if err != nil || len(claims) == 0 {
		return time.Time{}, false
	}
	for _, cl := range claims {
		if c.IsDeleted(cl.BlobRef) {
			continue
		}
		if cl.Date.After(t) {
			t = cl.Date
		}
	}
	return t, !t.IsZero()
}

func (c *corpusDB) pnTimeAttr(pn blob.Ref, attr string) (t time.Time, ok bool) {
	if v := c.PermanodeAttrValue(pn, attr, time.Time{}, ""); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, true
		}
	}
	return
}

func (c *corpusDB) pnCamliContent(pn blob.Ref) (cc blob.Ref, t time.Time, ok bool) {
	claims, err := c.claimsOf(pn)
	if err != nil {
		return
	}
	for _, cl := range claims {
		if cl.Attr != "camliContent" {
			continue
		}
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

func (c *corpusDB) PermanodeTime(pn blob.Ref) (t time.Time, ok bool) {
	// Priorities:
	// -- Permanode explicit "camliTime" property
	// -- EXIF GPS time
	// -- Exif camera time (already in the FileInfo)
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
		fi, _, _ = c.fileInfo(ccRef)
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

func (c *corpusDB) PermanodeAnyTime(pn blob.Ref) (t time.Time, ok bool) {
	if t, ok := c.PermanodeTime(pn); ok {
		return t, ok
	}
	return c.PermanodeModtime(pn)
}

func (c *corpusDB) PermanodeAttrsOrClaims(permaNode blob.Ref,
	at time.Time, signerID string) (m map[string][]string, claims []*camtypes.Claim) {
	claims, err := c.claimsOf(permaNode)
	if err != nil || len(claims) == 0 {
		return nil, nil
	}
	if !at.IsZero() && claims[len(claims)-1].Date.After(at) {
		return nil, claims
	}
	var signerRefs SignerRefSet
	if signerID != "" {
		signerRefs = c.SignerRefs(signerID)
		if len(signerRefs) == 0 {
			return nil, nil
		}
	}
	m = make(map[string][]string)
	for _, cl := range claims {
		if len(signerRefs) > 0 && !signerRefsMatch(signerRefs, cl.Signer) {
			continue
		}
		if !at.IsZero() && cl.Date.After(at) {
			break
		}
		switch cl.Type {
		case string(schema.SetAttributeClaim):
			m[cl.Attr] = []string{cl.Value}
		case string(schema.AddAttributeClaim):
			m[cl.Attr] = append(m[cl.Attr], cl.Value)
		case string(schema.DelAttributeClaim):
			if cl.Value == "" {
				delete(m, cl.Attr)
			} else {
				v := m[cl.Attr]
				i := 0
				for _, w := range v {
					if w != cl.Value {
						v[i] = w
						i++
					}
				}
				m[cl.Attr] = v[:i]
			}
		}
	}
	return m, nil
}

func (c *corpusDB) listPermanodes(pnTime func(blob.Ref) (time.Time, bool), reverse bool) ([]pnAndTime, error) {
	rows, err := c.db.Query(`SELECT DISTINCT permanode FROM claims`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pns []pnAndTime
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		pn := blob.ParseOrZero(s)
		if !pn.Valid() || c.IsDeleted(pn) {
			continue
		}
		if t, ok := pnTime(pn); ok {
			pns = append(pns, pnAndTime{pn, t})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if reverse {
		sort.Sort(sort.Reverse(byPermanodeTime(pns)))
	} else {
		sort.Sort(byPermanodeTime(pns))
	}
	return pns, nil
}

func (c *corpusDB) enumeratePermanodes(fn func(camtypes.BlobMeta) bool, pns []pnAndTime) {
	for _, cand := range pns {
		bm, err := c.GetBlobMeta(context.TODO(), cand.pn)
		if err != nil {
			continue
		}
		if !fn(bm) {
			return
		}
	}
}

func (c *corpusDB) EnumeratePermanodesLastModified(fn func(camtypes.BlobMeta) bool) {
	pns, err := c.listPermanodes(c.PermanodeModtime, true)
	if err != nil {
		return
	}
	c.enumeratePermanodes(fn, pns)
}

func (c *corpusDB) EnumeratePermanodesCreated(fn func(camtypes.BlobMeta) bool, newestFirst bool) {
	pns, err := c.listPermanodes(c.PermanodeAnyTime, newestFirst)
	if err != nil {
		return
	}
	c.enumeratePermanodes(fn, pns)
}

func (c *corpusDB) EnumeratePermanodesByNodeTypes(fn func(camtypes.BlobMeta) bool, camliNodeTypes []string) {
	for _, typ := range camliNodeTypes {
		rows, err := c.db.Query(`SELECT DISTINCT permanode FROM claims WHERE attr = 'camliNodeType' AND value = ?`, typ)
		if err != nil {
			return
		}
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				rows.Close()
				return
			}
			bm, err := c.GetBlobMeta(context.TODO(), blob.ParseOrZero(s))
			if err != nil {
				continue
			}
			if !fn(bm) {
				rows.Close()
				return
			}
		}
		rows.Close()
	}
}

func (c *corpusDB) EnumerateBlobMeta(fn func(camtypes.BlobMeta) bool) {
	var stop bool
	fn2 := func(b camtypes.BlobMeta) bool { cont := fn(b); stop = !cont; return cont }
	for i := range partNum {
		c.enumerateBlobs(
			fmt.Sprintf(`SELECT ref, size, camlitype FROM blobs_`+partPat, i),
			fn2,
		)
		if stop {
			break
		}
	}
}

func (c *corpusDB) enumerateBlobs(query string, fn func(camtypes.BlobMeta) bool, args ...any) {
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var ref, camliType string
		var size uint64
		if err := rows.Scan(&ref, &size, &camliType); err != nil {
			return
		}
		if !fn(camtypes.BlobMeta{
			Ref:       blob.ParseOrZero(ref),
			Size:      uint32(size),
			CamliType: schema.CamliType(camliType),
		}) {
			return
		}
	}
}

func (c *corpusDB) EnumerateCamliBlobs(camType schema.CamliType, fn func(camtypes.BlobMeta) bool) {
	var stop bool
	fn2 := func(b camtypes.BlobMeta) bool { cont := fn(b); stop = !cont; return cont }
	if camType != "" {
		for i := range partNum {
			c.enumerateBlobs(fmt.Sprintf(
				`SELECT ref, size, camlitype FROM blobs_`+partPat+` WHERE camlitype = ?`,
				i),
				fn2, string(camType))
			if stop {
				break
			}
		}
		return
	}
	for i := range partNum {
		c.enumerateBlobs(fmt.Sprintf(
			`SELECT ref, size, camlitype FROM blobs_`+partPat+` WHERE camlitype != ''`, i),
			fn2)
		if stop {
			break
		}
	}
}

func (c *corpusDB) EnumerateSingleBlob(fn func(camtypes.BlobMeta) bool, br blob.Ref) {
	bm, err := c.GetBlobMeta(context.TODO(), br)
	if err == nil {
		fn(bm)
	}
}

func (c *corpusDB) IterPermanodes() iter.Seq[blob.Ref] {
	rows, err := c.db.Query(`SELECT DISTINCT permanode FROM claims WHERE permanode IS NOT NULL`)
	if err != nil {
		panic(err)
	}
	return func(yield func(blob.Ref) bool) {
		defer rows.Close()
		for rows.Next() {
			var pn string
			if err := rows.Scan(&pn); err != nil {
				panic(err)
			}
			if !yield(blob.RefFromString(pn)) {
				return
			}
		}
	}
}

// *********** Row parsing helpers

// parseClaimBytes parses a "claim|<permanode>|<signerID>|<date>|<claim>"
// key and its value, mirroring the in-memory corpus.
func parseClaimBytes(k, v []byte) (cl camtypes.Claim, ok bool) {
	const sep = "|"
	keyPart := strings.Split(string(k), sep)
	valPart := strings.Split(string(v), sep)
	if len(keyPart) < 5 || len(valPart) < 4 {
		return
	}
	signerRef, ok := blob.Parse(valPart[3])
	if !ok {
		return
	}
	permaNode, ok := blob.Parse(keyPart[1])
	if !ok {
		return
	}
	claimRef, ok := blob.Parse(keyPart[4])
	if !ok {
		return
	}
	date, err := time.Parse(time.RFC3339, keyPart[3])
	if err != nil {
		return
	}
	return camtypes.Claim{
		BlobRef:   claimRef,
		Signer:    signerRef,
		Permanode: permaNode,
		Date:      date,
		Type:      urld(valPart[0]),
		Attr:      urld(valPart[1]),
		Value:     urld(valPart[2]),
	}, true
}

// parseDeletedKey parses a
// "deleted|<deleted blobref>|<reverse claimdate>|<deleter claim>|" key.
func parseDeletedKey(k string) (cl camtypes.Claim, ok bool) {
	keyPart := strings.Split(k, "|")
	if len(keyPart) != 4 || keyPart[0] != "deleted" {
		return
	}
	target, ok := blob.Parse(keyPart[1])
	if !ok {
		return
	}
	claimRef, ok := blob.Parse(keyPart[3])
	if !ok {
		return
	}
	date, err := time.Parse(time.RFC3339, unreverseTimeString(keyPart[2]))
	if err != nil {
		return
	}
	return camtypes.Claim{
		BlobRef: claimRef,
		Target:  target,
		Date:    date,
		Type:    string(schema.DeleteClaim),
	}, true
}

// parseImageInfo parses a "width|height" value.
func parseImageInfo(v []byte) (ii camtypes.ImageInfo, ok bool) {
	pipei := bytes.IndexByte(v, '|')
	if pipei < 0 {
		return
	}
	w, err := strconv.ParseUint(string(v[:pipei]), 10, 16)
	if err != nil {
		return
	}
	h, err := strconv.ParseUint(string(v[pipei+1:]), 10, 16)
	if err != nil {
		return
	}
	ii.Width = uint16(w)
	ii.Height = uint16(h)
	return ii, true
}

// time3339OrNilNanos parses s as a time, returning the unix nanos as an
// int64, or nil if s is empty or not a valid time.
func time3339OrNilNanos(s string) any {
	if t := types.ParseTime3339OrNil(s); t != nil {
		return time.Time(*t).UnixNano()
	}
	return nil
}

// signerRefsMatch reports whether br is in the set of signer refs.
func signerRefsMatch(refs SignerRefSet, br blob.Ref) bool {
	s := br.String()
	for _, ref := range refs {
		if ref == s {
			return true
		}
	}
	return false
}

func refPartition(br blob.Ref) string {
	return fmt.Sprintf(partPat, br.Sum32()%partNum)
}
