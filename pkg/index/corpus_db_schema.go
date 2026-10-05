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
	"context"
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
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// openDB opens (creating it if needed) the sqlite database at file, and
// initializes its schema.
func openDB(ctx context.Context, file string) (*sql.DB, *sql.Conn, error) {
	db, err := sql.Open("sqlite", file)
	if err != nil {
		return nil, nil, err
	}
	var conn *sql.Conn
	if err := func() error {
		const n = 4
		db.SetMaxIdleConns(n + 1)
		db.SetMaxOpenConns(n + 1)
		tbc := make([]*sql.Conn, 0, n)
		defer func() {
			for _, c := range tbc {
				c.Close()
			}
		}()
		for i := range n {
			cx, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			for _, s := range []string{
				"journal_mode = WAL",
				"synchronous = NORMAL",
				"cache_size = -65536",
				"mmap_size = 8000000000",
				"page_size = 16384",
				"temp_store = MEMORY",
				"threads = 4",
				"optimize",
			} {
				if _, err := cx.ExecContext(ctx, "PRAGMA "+s); err != nil {
					return fmt.Errorf("PRAGMA %s: %w", s, err)
				}
			}
			if i == 0 {
				conn = cx
			} else {
				if _, err := cx.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
					return err
				}
				tbc = append(tbc, cx)
			}
		}
		for _, c := range tbc {
			c.Close()
		}
		tbc = nil

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
					if _, err := conn.ExecContext(ctx, stmt); err != nil {
						return fmt.Errorf("initializing schema: %s: %w", stmt, err)
					}
				}
			} else if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("initializing schema: %s: %w", stmt, err)
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
		if _, err := conn.ExecContext(ctx, buf.String()); err != nil {
			return fmt.Errorf("initializing schema: %s: %w", buf.String(), err)
		}
		buf.Reset()
		buf.WriteString(`CREATE VIEW IF NOT EXISTS claims AS `)
		for i := range partNum {
			if i != 0 {
				buf.WriteString(" UNION ALL ")
			}
			fmt.Fprintf(&buf, `SELECT claimref, permanode, signerref, date, type, attr, value FROM claims_`+partPat, i)
		}
		if _, err := conn.ExecContext(ctx, buf.String()); err != nil {
			return fmt.Errorf("initializing schema: %s: %w", buf.String(), err)
		}
		buf.Reset()
		buf.WriteString(`CREATE VIEW IF NOT EXISTS claims_by_permanode AS `)
		for i := range partNum {
			if i != 0 {
				buf.WriteString(" UNION ALL ")
			}
			fmt.Fprintf(&buf, `SELECT permanode, claimref FROM claims_by_permanode_`+partPat, i)
		}
		if _, err := conn.ExecContext(ctx, buf.String()); err != nil {
			return fmt.Errorf("initializing schema: %s: %w", buf.String(), err)
		}

		var version string
		err = conn.QueryRowContext(ctx, `SELECT value FROM meta WHERE metakey = ?`, metaSchemaVersion).Scan(&version)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := conn.ExecContext(ctx, `INSERT INTO meta (metakey, value) VALUES (?, ?)`,
				metaSchemaVersion, fmt.Sprint(requiredSchemaVersion)); err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			if version != fmt.Sprint(requiredSchemaVersion) {
				return fmt.Errorf("schema version mismatch: have %v, want %v", version, requiredSchemaVersion)
			}
		}
		return err
	}(); err != nil {
		db.Close()
		return nil, nil, err
	}
	return db, conn, nil
}

const (
	partPat = "%02x"
	partNum = 0xff // 4096
)
