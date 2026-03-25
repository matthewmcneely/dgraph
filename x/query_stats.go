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

// QueryStats collects per-query performance statistics from the posting layer.
// It is stored in context and incremented atomically by LocalCache operations.
type QueryStats struct {
	DiskReads   atomic.Int64
	CacheHits   atomic.Int64
	CacheMisses atomic.Int64
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
