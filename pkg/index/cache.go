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
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/types/camtypes"

	_ "modernc.org/sqlite"
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
	db *sql.DB
	fn string
}

var _ permanodeCache = (pmCacheSqlite{})

func newPmCacheSqlite(_ int) (pmCacheSqlite, error) {
	fh, err := os.CreateTemp("", "perkeep-pm-cache-*.sqlite")
	db, err := sql.Open("sqlite", "file://"+fh.Name()+"?mode=rwc&vfs=unix-excl")
	fh.Close()
	if err != nil {
		return pmCacheSqlite{}, err
	}
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, qry := range []string{
		`CREATE TABLE IF NOT EXISTS pm_cache (blob_ref BLOB, PRIMARY KEY(blob_ref));`,
		`CREATE TABLE IF NOT EXISTS pm_cache_claim (
  blob_ref BLOB, signer BLOB, --BlobRef, Signer blob.Ref
  date BIGINT, --Date time.Time
  type TEXT, --Type string // "set-attribute", "add-attribute", etc
  attr TEXT, 
  value TEXT,
  permanode BLOB,
  target BLOB
);`,
		`CREATE INDEX IF NOT EXISTS K_pm_cache_claim ON pm_cache_claim(blob_ref);`,
		`CREATE TABLE IF NOT EXISTS pm_cache_attr(
  blob_ref BLOB,
  sig_id TEXT,
  attr TEXT,
  value TEXT
);`,
		`CREATE INDEX IF NOT EXISTS K_pm_cache_attr ON pm_cache_attr(blob_ref);`,
	} {
		if _, err = db.ExecContext(ctx, qry); err != nil {
			db.Close()
			return pmCacheSqlite{}, fmt.Errorf("exec %s: %w", qry, err)
		}
	}
	return pmCacheSqlite{db: db, fn: fh.Name()}, nil
}
func (c pmCacheSqlite) Close() error {
	err := c.db.Close()
	if c.fn != "" {
		_ = os.Remove(c.fn)
	}
	return err
}

func (c pmCacheSqlite) Len() int {
	const qry = "SELECT COUNT(0) FROM pm_cache;"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var n int
	_ = c.db.QueryRowContext(ctx, qry).Scan(&n)
	cancel()
	return n
}
func (c pmCacheSqlite) IterKeys(f func(blob.Ref, error) error) error {
	const qry = `SELECT blob_ref FROM pm_cache;`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows, err := c.db.QueryContext(ctx, qry)
	if err != nil {
		return fmt.Errorf("%s: %w", qry, err)
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return fmt.Errorf("%s: %w", qry, err)
		}
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
	return nil
}
func (c pmCacheSqlite) Iter(f func(blob.Ref, *PermanodeMeta, error) error) error {
	return c.IterKeys(func(br blob.Ref, err error) error {
		if err != nil {
			return err
		}
		pm, err := c.Get(br)
		return f(br, pm, err)
	})
}
func (c pmCacheSqlite) Get(br blob.Ref) (*PermanodeMeta, error) {
	brB, err := br.MarshalBinary()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	fmt.Printf("Get(%q)\n", br)
	defer fmt.Printf("got(%q)\n", br)
	var pm PermanodeMeta
	if err = func() error {
		const qryClaim = "SELECT signer, date, type, attr, value, permanode, target FROM pm_cache_claim WHERE blob_ref = $1 ORDER BY date;"
		rows, err := tx.QueryContext(ctx, qryClaim, brB)
		if err != nil {
			return fmt.Errorf("%s: %w", qryClaim, err)
		}
		defer rows.Close()
		for rows.Next() {
			var signer, permanode, target []byte
			var dt int64
			var claim camtypes.Claim
			if err = rows.Scan(&signer, &dt, &claim.Type, &claim.Attr, &claim.Value, &permanode, &target); err != nil {
				return fmt.Errorf("%s: %w", qryClaim, err)
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

	err = func() error {
		const qryAttr = "SELECT sig_id, attr, value FROM pm_cache_attr WHERE blob_ref = $1;"
		rows, err := tx.QueryContext(ctx, qryAttr, brB)
		if err != nil {
			return fmt.Errorf("%s: %w", qryAttr, err)
		}
		defer rows.Close()
		for rows.Next() {
			var sig, attr, value string
			if err = rows.Scan(&sig, &attr, &value); err != nil {
				return fmt.Errorf("%s: %w", qryAttr, err)
			}
			if pm.attr == nil {
				pm.attr = make(attrValues)
				pm.signer = make(map[string]attrValues, 1)
			}
			pm.attr[attr] = append(pm.attr[attr], value)
			m := pm.signer[sig]
			if m == nil {
				m = make(attrValues)
				pm.signer[sig] = m
			}
			m[attr] = append(m[attr], value)
		}
		return rows.Close()
	}()
	return &pm, err
}
func (c pmCacheSqlite) Put(br blob.Ref, pm *PermanodeMeta) error {
	if pm == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Printf("Put(%q)\n", br)
	defer fmt.Printf("put(%q)\n", br)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	brB, err := br.MarshalBinary()
	if err != nil {
		return err
	}
	//fmt.Println(c.fn, br.String())
	const insRef = `DELETE FROM pm_cache_claim WHERE blob_ref = $1;
DELETE FROM pm_cache_attr WHERE  blob_ref = $2;
INSERT OR IGNORE INTO pm_cache (blob_ref) VALUES ($3);`
	if _, err = tx.ExecContext(ctx, insRef, brB, brB, brB); err != nil {
		return fmt.Errorf("exec %s [%v]: %w", insRef, brB, err)
	}

	if len(pm.signer) != 0 {
		if err = func() error {
			const insAttr = `INSERT INTO pm_cache_attr 
  (blob_ref, sig_id, attr, value) 
  VALUES ($1, $2, $3, $4);`
			stmt, err := tx.PrepareContext(ctx, insAttr)
			if err != nil {
				return fmt.Errorf("prepare %s: %w", insAttr, err)
			}
			defer stmt.Close()
			for sig, m := range pm.signer {
				for k, vv := range m {
					for _, v := range vv {
						if _, err = stmt.ExecContext(ctx,
							brB, sig, k, v,
						); err != nil {
							return fmt.Errorf("exec %s [%v %q %q %q]: %w", insAttr, brB, sig, k, v, err)
						}
					}
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	if len(pm.Claims) != 0 {
		if err = func() error {
			const insClaim = `DELETE FROM pm_cache_claim WHERE blob_ref = $1;
INSERT INTO pm_cache_claim 
  (blob_ref, signer, date, type, attr, value, permanode, target) 
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`
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
