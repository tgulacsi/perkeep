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

package index

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"go4.org/syncutil"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/types/camtypes"

	//_ "modernc.org/sqlite"
	_ "github.com/mattn/go-sqlite3"
)

func newPmCache(sizeHint int) (permanodeCache, error) {
	return newPmCacheSqlite(sizeHint)
}

// permanodeCache is the interface for a permanode cache.
type permanodeCache interface {
	// Iter calls the given function for each and every member till it gets an error.
	// On ErrIterBreak, it silently breaks the iteration.
	Iter(func(blob.Ref, *PermanodeMeta, error) error) error
	IterKeys(func(blob.Ref, error) error) error

	Get(br blob.Ref) (*PermanodeMeta, error)
	Put(br blob.Ref, pm *PermanodeMeta) error

	Len() int
	Close() error
}

type pmCacheMem map[blob.Ref]*PermanodeMeta

func (c pmCacheMem) Close() error {
	for k := range c {
		delete(c, k)
	}
	return nil
}
func (c pmCacheMem) Len() int { return len(c) }
func (c pmCacheMem) IterKeys(f func(blob.Ref, error) error) error {
	for k := range c {
		if err := f(k, nil); err != nil {
			if errors.Is(err, ErrIterBreak) {
				return nil
			}
			return err
		}
	}
	return nil
}
func (c pmCacheMem) Iter(f func(blob.Ref, *PermanodeMeta, error) error) error {
	for k, v := range c {
		if err := f(k, v, nil); err != nil {
			if errors.Is(err, ErrIterBreak) {
				return nil
			}
			return err
		}
	}
	return nil
}
func (c pmCacheMem) Get(br blob.Ref) (*PermanodeMeta, error) { return c[br], nil }
func (c pmCacheMem) Put(br blob.Ref, pm *PermanodeMeta) error {
	v := c[br]
	if v == nil {
		c[br] = pm
	} else {
		*(c[br]) = *pm
	}
	return nil
}

var ErrIterBreak = errors.New("break iteration")

var _ permanodeCache = (pmCacheMem)(nil)

func newPmCacheMem(sizeHint int) (pmCacheMem, error) {
	return make(map[blob.Ref]*PermanodeMeta, sizeHint), nil
}

type pmCacheSqlite struct {
	db   *sql.DB
	fn   string
	gate *syncutil.Gate
}

var _ permanodeCache = (*pmCacheSqlite)(nil)

func newPmCacheSqlite(_ int) (*pmCacheSqlite, error) {
	fh, err := os.CreateTemp("", "perkeep-pm-cache-*.sqlite")
	db, err := sql.Open("sqlite3", "file://"+fh.Name()+"?mode=rwc&vfs=unix-excl&cache=shared")
	fh.Close()
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(3)

	c := &pmCacheSqlite{db: db, fn: fh.Name(), gate: syncutil.NewGate(1)}
	if err = func() error {
		var err error
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		for _, qry := range []string{
			`PRAGMA journal_mode = wal;
PRAGMA locking_mode = exclusive;
PRAGMA read_uncommitted = true;
PRAGMA synchronous = normal;
PRAGMA temp_store = 2;`,
			`CREATE TABLE IF NOT EXISTS pm_cache (blob_ref BLOB, PRIMARY KEY(blob_ref))`,
			`CREATE TABLE IF NOT EXISTS pm_cache_claim (
  blob_ref BLOB, signer BLOB, --BlobRef, Signer blob.Ref
  date BIGINT, --Date time.Time
  type TEXT, --Type string // "set-attribute", "add-attribute", etc
  attr TEXT, 
  value TEXT,
  permanode BLOB,
  target BLOB
)`,
			`CREATE INDEX IF NOT EXISTS K_pm_cache_claim ON pm_cache_claim(blob_ref)`,
		} {
			if _, err = c.db.ExecContext(ctx, qry); err != nil {
				return fmt.Errorf("exec %s: %w", qry, err)
			}
		}
		return nil
	}(); err != nil {
		c.Close()
		return nil, err
	}
	runtime.SetFinalizer(c, func(_ interface{}) { _ = c.Close() })
	return c, nil
}
func (c *pmCacheSqlite) Close() error {
	fn, db := c.fn, c.db
	c.fn, c.db = "", nil
	if db != nil {
		fmt.Println("CLOSE", fn)
		err := db.Close()
		if fn != "" {
			_ = os.Remove(fn)
		}
		if err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}
	return nil
}

func (c *pmCacheSqlite) Len() int {
	const qry = "SELECT COUNT(0) FROM pm_cache"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var n int
	_ = c.db.QueryRowContext(ctx, qry).Scan(&n)
	cancel()
	return n
}
func (c *pmCacheSqlite) IterKeys(f func(blob.Ref, error) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return c.iterKeys(ctx, tx, f)
}

func (c *pmCacheSqlite) iterKeys(ctx context.Context, tx *sql.Tx, f func(blob.Ref, error) error) error {
	const qry = `SELECT blob_ref FROM pm_cache`
	rows, err := tx.QueryContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("iterkeys %s: %w", qry, err)
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return fmt.Errorf("scan %s: %w", qry, err)
		}
		//fmt.Println(b)
		var br blob.Ref
		if err = br.UnmarshalBinary(b); err != nil {
			return err
		}
		if err := f(br, nil); err != nil {
			if errors.Is(err, ErrIterBreak) {
				return nil
			}
			return err
		}
	}
	return rows.Close()
}
func (c *pmCacheSqlite) Iter(f func(blob.Ref, *PermanodeMeta, error) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	return c.iterKeys(ctx, tx, func(br blob.Ref, err error) error {
		if err != nil {
			return err
		}
		pm, err := c.get(ctx, tx, br)
		return f(br, pm, err)
	})
}
func (c *pmCacheSqlite) Get(br blob.Ref) (*PermanodeMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return c.get(ctx, tx, br)
}

func (c *pmCacheSqlite) get(ctx context.Context, tx *sql.Tx, br blob.Ref) (*PermanodeMeta, error) {
	brB, err := br.MarshalBinary()
	if err != nil {
		return nil, err
	}
	c.gate.Start()
	defer c.gate.Done()
	var pm PermanodeMeta
	if err = func() error {
		const qryClaim = "SELECT signer, date, type, attr, value, permanode, target FROM pm_cache_claim WHERE blob_ref = ? ORDER BY date"
		rows, err := tx.QueryContext(ctx, qryClaim, brB)
		if err != nil {
			return fmt.Errorf("query %s: %w", qryClaim, err)
		}
		defer rows.Close()
		for rows.Next() {
			var signer, permanode, target []byte
			var dt int64
			var claim camtypes.Claim
			if err = rows.Scan(&signer, &dt, &claim.Type, &claim.Attr, &claim.Value, &permanode, &target); err != nil {
				return fmt.Errorf("scan %s: %w", qryClaim, err)
			}
			if err = claim.Signer.UnmarshalBinary(signer); err != nil {
				return err
			}
			claim.Date = time.UnixMilli(dt)
			if len(permanode) != 0 {
				if err = claim.Permanode.UnmarshalBinary(permanode); err != nil {
					return err
				}
			}
			if len(target) != 0 {
				if err = claim.Target.UnmarshalBinary(target); err != nil {
					return err
				}
			}
			pm.Claims = append(pm.Claims, &claim)
		}
		return rows.Close()
	}(); err != nil {
		return &pm, err
	}

	return &pm, nil
}
func (c *pmCacheSqlite) Put(br blob.Ref, pm *PermanodeMeta) error {
	if pm == nil {
		return nil
	}
	brB, err := br.MarshalBinary()
	if err != nil {
		return err
	}
	c.gate.Start()
	defer c.gate.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("put BEGIN: %w", err)
	}
	defer tx.Rollback()

	const insRef = `DELETE FROM pm_cache_claim WHERE blob_ref = ?;
INSERT OR IGNORE INTO pm_cache (blob_ref) VALUES (?);`
	if _, err = tx.ExecContext(ctx, insRef, brB, brB, brB); err != nil {
		return fmt.Errorf("exec %s [%v]: %w", insRef, brB, err)
	}

	if len(pm.Claims) != 0 {
		if err = func() error {
			const insClaim = `INSERT INTO pm_cache_claim 
  (blob_ref, signer, date, type, attr, value, permanode, target) 
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
			stmt, err := tx.PrepareContext(ctx, insClaim)
			if err != nil {
				return fmt.Errorf("prepare %s: %w", insClaim, err)
			}
			defer stmt.Close()
			for _, claim := range pm.Claims {
				signer, err := claim.Signer.MarshalBinary()
				if err != nil {
					return err
				}
				dt := claim.Date.UnixMilli()
				var permanode, target []byte
				if claim.Permanode.Valid() {
					if permanode, err = claim.Permanode.MarshalBinary(); err != nil {
						return err
					}
				}
				if claim.Target.Valid() {
					if target, err = claim.Target.MarshalBinary(); err != nil {
						return err
					}
				}
				if _, err = stmt.ExecContext(ctx,
					brB, signer, dt, claim.Type, claim.Attr, claim.Value, permanode, target,
				); err != nil {
					return fmt.Errorf("exec %s [%v %v %v %q %q %v %v]: %w", insClaim, brB, signer, dt, claim.Attr, claim.Value, permanode, target, err)
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	return tx.Commit()
}
