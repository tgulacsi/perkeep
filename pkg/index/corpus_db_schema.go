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

package index

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// misc table keys.
const (
	metaSchemaVersion  = "schemaversion"
	metaHasLegacySHA1  = "hasLegacySHA1"
	valHasLegacySHA1   = "1"
	keySignerKeyIDName = "signerkeyid"
	metaGeneration     = "generation"
)

// dbtx is the subset of database/sql used by the merge functions, and is
// implemented by both *sql.DB and *sql.Tx.
type dbtx interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

// openDB opens (creating it if needed) the sqlite database at file, and
// initializes its schema.
func openDB(file string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", file)
	if err != nil {
		return nil, err
	}
	for _, s := range []string{
		"journal_mode = WAL",
		"synchronous = OFF",
		"cache_size = -40000",
		"page_size = 16384",
		"optimize",
	} {
		if _, err := db.Exec("PRAGMA " + s); err != nil {
			db.Close()
			return nil, fmt.Errorf("PRAGMA %s: %w", s, err)
		}
	}

	for _, stmt := range []string{
		// partition blobs
		`CREATE TABLE IF NOT EXISTS blobs_` + partPat + `(
		ref       TEXT PRIMARY KEY,
		size      INTEGER NOT NULL,
		camlitype TEXT NOT NULL DEFAULT ''
	) WITHOUT ROWID, STRICT`,

		`CREATE TABLE IF NOT EXISTS signers (
		signerref TEXT PRIMARY KEY,
		keyid     TEXT NOT NULL
	) STRICT`,
		`CREATE INDEX IF NOT EXISTS signers_by_keyid ON signers(keyid)`,

		`CREATE TABLE IF NOT EXISTS claims_` + partPat + ` (
		claimref  TEXT PRIMARY KEY,
		permanode TEXT NOT NULL,
		signerref TEXT NOT NULL,
		date      INTEGER NOT NULL, -- unix nanos
		type      TEXT NOT NULL DEFAULT '',
		attr      TEXT NOT NULL DEFAULT '',
		value     TEXT NOT NULL DEFAULT ''
	) STRICT`,
		`CREATE INDEX IF NOT EXISTS claims_by_value_` + partPat + ` ON claims_` + partPat + `(value)`,
		// `CREATE INDEX IF NOT EXISTS claims_by_permanode ON claims(permanode, date)`,
		`CREATE TABLE IF NOT EXISTS claims_by_permanode_` + partPat + ` (
		permanode TEXT NOT NULL,
		claimref TEXT NOT NULL,
		PRIMARY KEY (permanode, claimref)
	) WITHOUT ROWID, STRICT`,

		`CREATE TABLE IF NOT EXISTS files (
		fileref  TEXT PRIMARY KEY,
		size     INTEGER NOT NULL DEFAULT 0,
		filename TEXT NOT NULL DEFAULT '',
		mimetype TEXT NOT NULL DEFAULT '',
		wholeref TEXT NOT NULL DEFAULT '',
		time     INTEGER, -- unix nanos, NULL if unknown
		modtime  INTEGER  -- unix nanos, NULL if unknown
	) STRICT`,
		`CREATE INDEX IF NOT EXISTS files_by_wholeref ON files(wholeref)`,
		`CREATE TABLE IF NOT EXISTS wholetofile (
		fileref  TEXT PRIMARY KEY,
		wholeref TEXT NOT NULL
	) STRICT`,
		`CREATE INDEX IF NOT EXISTS wholetofile_by_wholeref ON wholetofile(wholeref)`,
		`CREATE TABLE IF NOT EXISTS imagesizes (
		fileref TEXT PRIMARY KEY,
		width   INTEGER NOT NULL,
		height  INTEGER NOT NULL
	) WITHOUT ROWID, STRICT`,
		`CREATE TABLE IF NOT EXISTS mediatags (
		wholeref TEXT NOT NULL,
		tag      TEXT NOT NULL,
		value    TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (wholeref, tag)
	) STRICT`,
		`CREATE TABLE IF NOT EXISTS exifgps (
		wholeref TEXT PRIMARY KEY,
		lat      REAL NOT NULL,
		long     REAL NOT NULL
	) WITHOUT ROWID, STRICT`,
		`CREATE TABLE IF NOT EXISTS dirchildren (
		parent TEXT NOT NULL,
		child  TEXT NOT NULL,
		PRIMARY KEY (parent, child)
	) STRICT`,
		`CREATE TABLE IF NOT EXISTS fileparents (
		child  TEXT NOT NULL,
		parent TEXT NOT NULL,
		PRIMARY KEY (child, parent)
	) STRICT`,

		`CREATE TABLE IF NOT EXISTS deletes (
		deleted TEXT NOT NULL,
		deleter TEXT NOT NULL,
		deltime INTEGER NOT NULL, -- unix nanos
		PRIMARY KEY (deleted, deleter)
	) STRICT`,

		`CREATE TABLE IF NOT EXISTS meta (
		metakey TEXT PRIMARY KEY,
		value   TEXT NOT NULL
	) WITHOUT ROWID, STRICT`,
	} {
		if n := strings.Count(stmt, "_"+partPat); n > 0 {
			for i := range partNum {
				stmt := fmt.Sprintf(stmt, []any{i, i}[:n]...)
				if _, err := db.Exec(stmt); err != nil {
					return nil, fmt.Errorf("initializing schema: %s: %w", stmt, err)
				}
			}
		} else if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("initializing schema: %s: %w", stmt, err)
		}
	}
	var buf strings.Builder
	buf.WriteString(`CREATE VIEW IF NOT EXISTS blobs AS `)
	for i := range partNum {
		if i != 0 {
			buf.WriteString(" UNION ALL ")
		}
		fmt.Fprintf(&buf, `SELECT ref, size, camlitype FROM blobs_`+partPat, i)
	}
	if _, err := db.Exec(buf.String()); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing schema: %s: %w", buf.String(), err)
	}
	buf.Reset()
	buf.WriteString(`CREATE VIEW IF NOT EXISTS claims AS `)
	for i := range partNum {
		if i != 0 {
			buf.WriteString(" UNION ALL ")
		}
		fmt.Fprintf(&buf, `SELECT claimref, permanode, signerref, date, type, attr, value FROM claims_`+partPat, i)
	}
	if _, err := db.Exec(buf.String()); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing schema: %s: %w", buf.String(), err)
	}
	buf.Reset()
	buf.WriteString(`CREATE VIEW IF NOT EXISTS claims_by_permanode AS `)
	for i := range partNum {
		if i != 0 {
			buf.WriteString(" UNION ALL ")
		}
		fmt.Fprintf(&buf, `SELECT permanode, claimref FROM claims_by_permanode_`+partPat, i)
	}
	if _, err := db.Exec(buf.String()); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing schema: %s: %w", buf.String(), err)
	}

	var version string
	err = db.QueryRow(`SELECT value FROM meta WHERE metakey = ?`, metaSchemaVersion).Scan(&version)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := db.Exec(`INSERT INTO meta (metakey, value) VALUES (?, ?)`,
			metaSchemaVersion, fmt.Sprint(requiredSchemaVersion)); err != nil {
			db.Close()
			return nil, err
		}
	case err != nil:
		db.Close()
		return nil, err
	default:
		if version != fmt.Sprint(requiredSchemaVersion) {
			db.Close()
			return nil, fmt.Errorf("schema version mismatch: have %v, want %v", version, requiredSchemaVersion)
		}
	}
	return db, nil
}

const (
	partPat = "%02x"
	partNum = 0xff // 4096
)
