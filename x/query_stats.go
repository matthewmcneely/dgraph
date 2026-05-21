/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"context"
	"sync/atomic"
)

type queryStatsKey struct{}

// QueryStats collects per-query performance statistics across all cache layers.
// It is stored in context and incremented atomically by LocalCache operations.
//
// Posting list (PL) cache stats are tracked via atomic increments in the posting layer.
// Badger block/index cache stats are computed as deltas from global counters
// (snapshotted before and after query execution in doQuery).
type QueryStats struct {
	// Posting list cache layer (tracked atomically during query)
	DiskReads     atomic.Int64 // Total reads that went to badger
	CacheHits     atomic.Int64 // MemoryLayer posting list cache hits
	CacheMisses   atomic.Int64 // MemoryLayer posting list cache misses
	PLCacheBypass atomic.Int64 // Reads that skipped PL cache (GetSinglePosting path)

	// Badger block/index cache layer (computed as deltas after query)
	BadgerBlockHits   uint64
	BadgerBlockMisses uint64
	BadgerIndexHits   uint64
	BadgerIndexMisses uint64
}

// TotalCacheHits returns the combined cache hits across all layers.
func (qs *QueryStats) TotalCacheHits() int64 {
	return qs.CacheHits.Load() + int64(qs.BadgerBlockHits) + int64(qs.BadgerIndexHits)
}

// TotalCacheMisses returns the combined cache misses across all layers.
func (qs *QueryStats) TotalCacheMisses() int64 {
	return qs.CacheMisses.Load() + qs.PLCacheBypass.Load() + int64(qs.BadgerBlockMisses) + int64(qs.BadgerIndexMisses)
}

// CacheHitRate returns the overall cache hit rate as a percentage (0-100).
func (qs *QueryStats) CacheHitRate() float64 {
	hits := qs.TotalCacheHits()
	misses := qs.TotalCacheMisses()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total) * 100
}

// BadgerCacheSnapshot holds a point-in-time snapshot of badger's global cache counters.
type BadgerCacheSnapshot struct {
	BlockHits   uint64
	BlockMisses uint64
	IndexHits   uint64
	IndexMisses uint64
}

// WithQueryStats attaches a new QueryStats collector to the context.
func WithQueryStats(ctx context.Context) (context.Context, *QueryStats) {
	qs := &QueryStats{}
	return context.WithValue(ctx, queryStatsKey{}, qs), qs
}

// QueryStatsFrom retrieves the QueryStats from context, or nil if not present.
func QueryStatsFrom(ctx context.Context) *QueryStats {
	qs, _ := ctx.Value(queryStatsKey{}).(*QueryStats)
	return qs
}
