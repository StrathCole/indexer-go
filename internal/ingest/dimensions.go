package ingest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/classic-terra/indexer-go/internal/db"
)

type Dimensions struct {
	pg *db.Postgres

	// Caches
	addressCache sync.Map // map[string]addressCacheEntry
	denomCache   sync.Map // map[string]uint16
	msgTypeCache sync.Map // map[string]uint16

	// Track address cache size for periodic cleanup
	addressCacheSize int64
	maxAddressCache  int64
}

type addressCacheEntry struct {
	id             uint64
	lastSeenHeight uint64
}

func NewDimensions(pg *db.Postgres) *Dimensions {
	return &Dimensions{
		pg:              pg,
		maxAddressCache: 500000, // Cap at 500k addresses (~40MB for strings + IDs)
	}
}

// clearAddressCache clears the address cache when it grows too large
// This is a simple approach - a proper LRU would be better but adds complexity
func (d *Dimensions) clearAddressCacheIfNeeded() {
	size := atomic.LoadInt64(&d.addressCacheSize)
	if size > d.maxAddressCache {
		// Clear the entire cache - entries will be re-fetched from DB as needed
		d.addressCache = sync.Map{}
		atomic.StoreInt64(&d.addressCacheSize, 0)
	}
}


func (d *Dimensions) GetOrCreateAddressID(ctx context.Context, address string, lastSeenHeight uint64, lastSeenAt time.Time) (uint64, bool, error) {
	if v, ok := d.addressCache.Load(address); ok {
		entry := v.(addressCacheEntry)
		if lastSeenHeight <= entry.lastSeenHeight {
			return entry.id, false, nil
		}

		// Update last-seen marker for known addresses, but only when height moves forward.
		_, err := d.pg.Pool.Exec(ctx,
			`UPDATE addresses
			 SET last_seen_height = GREATEST(last_seen_height, $2),
			     last_seen_at = GREATEST(last_seen_at, $3)
			 WHERE id = $1`,
			entry.id, lastSeenHeight, lastSeenAt,
		)
		if err == nil {
			entry.lastSeenHeight = lastSeenHeight
			d.addressCache.Store(address, entry)
		}
		return entry.id, false, nil
	}

	// Check if cache needs cleanup before adding new entries
	d.clearAddressCacheIfNeeded()

	var id uint64
	var storedHeight uint64
	var inserted bool
	query := `
		WITH ins AS (
			INSERT INTO addresses (address, type, last_seen_height, last_seen_at)
			VALUES ($1, 'account', $2, $3)
			ON CONFLICT (address) DO NOTHING
			RETURNING id, last_seen_height
		), upd AS (
			UPDATE addresses
			SET last_seen_height = GREATEST(addresses.last_seen_height, $2),
			    last_seen_at = GREATEST(addresses.last_seen_at, $3)
			WHERE address = $1
			  AND NOT EXISTS (SELECT 1 FROM ins)
			RETURNING id, last_seen_height
		)
		SELECT
			id,
			last_seen_height,
			(SELECT count(*) > 0 FROM ins) AS inserted
		FROM (
			SELECT id, last_seen_height FROM ins
			UNION ALL
			SELECT id, last_seen_height FROM upd
			LIMIT 1
		) t;
	`

	err := d.pg.Pool.QueryRow(ctx, query, address, lastSeenHeight, lastSeenAt).Scan(&id, &storedHeight, &inserted)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get or create address id: %w", err)
	}

	d.addressCache.Store(address, addressCacheEntry{id: id, lastSeenHeight: storedHeight})
	atomic.AddInt64(&d.addressCacheSize, 1)
	return id, inserted, nil
}

func (d *Dimensions) GetOrCreateDenomID(ctx context.Context, denom string) (uint16, error) {
	if id, ok := d.denomCache.Load(denom); ok {
		return id.(uint16), nil
	}

	var id uint16
	query := `
		WITH ins AS (
			INSERT INTO denoms (denom) VALUES ($1)
			ON CONFLICT (denom) DO NOTHING
			RETURNING id
		)
		SELECT id FROM ins
		UNION ALL
		SELECT id FROM denoms WHERE denom = $1
		LIMIT 1;
	`

	err := d.pg.Pool.QueryRow(ctx, query, denom).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create denom id: %w", err)
	}

	d.denomCache.Store(denom, id)
	return id, nil
}

func (d *Dimensions) GetOrCreateMsgTypeID(ctx context.Context, msgType string) (uint16, error) {
	if id, ok := d.msgTypeCache.Load(msgType); ok {
		return id.(uint16), nil
	}

	var id uint16
	query := `
		WITH ins AS (
			INSERT INTO msg_types (msg_type) VALUES ($1)
			ON CONFLICT (msg_type) DO NOTHING
			RETURNING id
		)
		SELECT id FROM ins
		UNION ALL
		SELECT id FROM msg_types WHERE msg_type = $1
		LIMIT 1;
	`

	err := d.pg.Pool.QueryRow(ctx, query, msgType).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create msg_type id: %w", err)
	}

	d.msgTypeCache.Store(msgType, id)
	return id, nil
}
