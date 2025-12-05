package ingest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/classic-terra/indexer-go/internal/db"
)

type Dimensions struct {
	pg *db.Postgres

	// Caches
	addressCache sync.Map // map[string]uint64
	denomCache   sync.Map // map[string]uint16
	msgTypeCache sync.Map // map[string]uint16

	// Track address cache size for periodic cleanup
	addressCacheSize int64
	maxAddressCache  int64
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

func (d *Dimensions) GetOrCreateAddressID(ctx context.Context, address string) (uint64, error) {
	if id, ok := d.addressCache.Load(address); ok {
		return id.(uint64), nil
	}

	// Check if cache needs cleanup before adding new entries
	d.clearAddressCacheIfNeeded()

	// Try to select first
	var id uint64
	err := d.pg.Pool.QueryRow(ctx, "SELECT id FROM addresses WHERE address = $1", address).Scan(&id)
	if err == nil {
		d.addressCache.Store(address, id)
		atomic.AddInt64(&d.addressCacheSize, 1)
		return id, nil
	}

	// Insert if not found
	// We use ON CONFLICT DO NOTHING and then select again to handle race conditions
	// Or we can use RETURNING id if we are sure about uniqueness constraint handling
	// A common pattern is:
	// WITH ins AS (
	//   INSERT INTO addresses (address, type) VALUES ($1, 'account')
	//   ON CONFLICT (address) DO NOTHING
	//   RETURNING id
	// )
	// SELECT id FROM ins
	// UNION ALL
	// SELECT id FROM addresses WHERE address = $1
	// LIMIT 1;

	query := `
		WITH ins AS (
			INSERT INTO addresses (address, type) VALUES ($1, 'account')
			ON CONFLICT (address) DO NOTHING
			RETURNING id
		)
		SELECT id FROM ins
		UNION ALL
		SELECT id FROM addresses WHERE address = $1
		LIMIT 1;
	`

	err = d.pg.Pool.QueryRow(ctx, query, address).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create address id: %w", err)
	}

	d.addressCache.Store(address, id)
	atomic.AddInt64(&d.addressCacheSize, 1)
	return id, nil
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
