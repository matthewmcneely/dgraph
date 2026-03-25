/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package edgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/glog"
	"go.opentelemetry.io/otel/trace"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/dgraph/v25/query"
	"github.com/dgraph-io/dgraph/v25/x"
)

// debugEntry holds the data for a single debug operation record.
type debugEntry struct {
	Query     string
	Variables string
	Operation string // "query" or "mutation"
	Timestamp time.Time
	Latency   *query.Latency
	Metrics   map[string]uint64
	Error     string
	Namespace uint64
	TraceID   string

	// Detailed metrics from posting layer (populated when enable-detailed-metrics is also on).
	DiskReads   int64
	CacheHits   int64
	CacheMisses int64
}

// writeDebugEntry writes a debug operation record to the dgraph.debug.* predicates.
// It runs asynchronously to avoid blocking the query/mutation response path.
func writeDebugEntry(ctx context.Context, entry *debugEntry) {
	// Build NQuads for the debug entry. We use a blank node so dgraph assigns a UID.
	nquads := make([]*api.NQuad, 0, 20)
	blank := "_:debug"

	nquads = append(nquads, &api.NQuad{
		Subject:   blank,
		Predicate: "dgraph.type",
		ObjectValue: &api.Value{
			Val: &api.Value_StrVal{StrVal: "dgraph.debug.Operation"},
		},
	})

	nquads = append(nquads, &api.NQuad{
		Subject:   blank,
		Predicate: "dgraph.debug.query",
		ObjectValue: &api.Value{
			Val: &api.Value_StrVal{StrVal: entry.Query},
		},
	})

	if entry.Variables != "" {
		nquads = append(nquads, &api.NQuad{
			Subject:   blank,
			Predicate: "dgraph.debug.variables",
			ObjectValue: &api.Value{
				Val: &api.Value_StrVal{StrVal: entry.Variables},
			},
		})
	}

	nquads = append(nquads, &api.NQuad{
		Subject:   blank,
		Predicate: "dgraph.debug.operation",
		ObjectValue: &api.Value{
			Val: &api.Value_StrVal{StrVal: entry.Operation},
		},
	})

	nquads = append(nquads, &api.NQuad{
		Subject:   blank,
		Predicate: "dgraph.debug.timestamp",
		ObjectValue: &api.Value{
			Val: &api.Value_StrVal{StrVal: entry.Timestamp.Format(time.RFC3339Nano)},
		},
	})

	totalNs := int64(time.Since(entry.Latency.Start).Nanoseconds())
	nquads = append(nquads, intNQuad(blank, "dgraph.debug.latency_total_ns", totalNs))
	nquads = append(nquads, intNQuad(blank, "dgraph.debug.latency_parsing_ns",
		int64(entry.Latency.Parsing.Nanoseconds())))
	nquads = append(nquads, intNQuad(blank, "dgraph.debug.latency_processing_ns",
		int64(entry.Latency.Processing.Nanoseconds())))
	nquads = append(nquads, intNQuad(blank, "dgraph.debug.latency_encoding_ns",
		int64(entry.Latency.Json.Nanoseconds())))
	nquads = append(nquads, intNQuad(blank, "dgraph.debug.latency_assign_ts_ns",
		int64(entry.Latency.AssignTimestamp.Nanoseconds())))

	if entry.Metrics != nil {
		total := entry.Metrics["_total"]
		nquads = append(nquads, intNQuad(blank, "dgraph.debug.uids_total", int64(total)))

		metricsJSON, err := json.Marshal(entry.Metrics)
		if err == nil {
			nquads = append(nquads, &api.NQuad{
				Subject:   blank,
				Predicate: "dgraph.debug.uid_metrics",
				ObjectValue: &api.Value{
					Val: &api.Value_StrVal{StrVal: string(metricsJSON)},
				},
			})
		}
	}

	// Detailed posting-layer metrics (disk reads, cache hits/misses).
	nquads = append(nquads, intNQuad(blank, "dgraph.debug.disk_reads", entry.DiskReads))
	nquads = append(nquads, intNQuad(blank, "dgraph.debug.cache_hits", entry.CacheHits))
	nquads = append(nquads, intNQuad(blank, "dgraph.debug.cache_misses", entry.CacheMisses))

	if entry.Error != "" {
		nquads = append(nquads, &api.NQuad{
			Subject:   blank,
			Predicate: "dgraph.debug.error",
			ObjectValue: &api.Value{
				Val: &api.Value_StrVal{StrVal: entry.Error},
			},
		})
	}

	nquads = append(nquads, intNQuad(blank, "dgraph.debug.namespace", int64(entry.Namespace)))

	if entry.TraceID != "" {
		nquads = append(nquads, &api.NQuad{
			Subject:   blank,
			Predicate: "dgraph.debug.trace_id",
			ObjectValue: &api.Value{
				Val: &api.Value_StrVal{StrVal: entry.TraceID},
			},
		})
	}

	// Use a detached context so we don't get cancelled by the request context.
	writeCtx := x.AttachNamespace(context.Background(), entry.Namespace)
	// Mark as internal GraphQL context to bypass user-mutation restrictions on reserved predicates.
	writeCtx = context.WithValue(writeCtx, IsGraphql, true)

	req := &Request{
		req: &api.Request{
			CommitNow: true,
			Mutations: []*api.Mutation{{Set: nquads}},
		},
		doAuth: NoAuthorize,
	}

	if _, err := (&Server{}).doQuery(writeCtx, req); err != nil {
		glog.Warningf("Failed to write query debug entry: %v", err)
	}
}

// intNQuad builds an NQuad with an integer value.
func intNQuad(subject, predicate string, val int64) *api.NQuad {
	return &api.NQuad{
		Subject:   subject,
		Predicate: predicate,
		ObjectValue: &api.Value{
			Val: &api.Value_IntVal{IntVal: val},
		},
	}
}

// maybeWriteDebugEntry checks whether query debug is enabled and, if so,
// asynchronously writes a debug record for the completed operation.
func maybeWriteDebugEntry(ctx context.Context, req *api.Request, l *query.Latency,
	resp *api.Response, queryErr error, stats *x.QueryStats) {

	if !x.Config.QueryDebugEnabled {
		return
	}

	// Don't record debug writes themselves (they are internal mutations on dgraph.debug.* predicates).
	if isDebugInternalRequest(req) {
		return
	}

	entry := &debugEntry{
		Query:     req.Query,
		Timestamp: l.Start,
		Latency:   l,
	}

	// Serialize variables separately from query text.
	if len(req.Vars) > 0 {
		if varsJSON, err := json.Marshal(req.Vars); err == nil {
			entry.Variables = string(varsJSON)
		}
	}

	if len(req.Mutations) > 0 {
		entry.Operation = "mutation"
	} else {
		entry.Operation = "query"
	}

	if resp != nil && resp.Metrics != nil {
		entry.Metrics = resp.Metrics.NumUids
	}

	if queryErr != nil {
		entry.Error = queryErr.Error()
	}

	if ns, err := x.ExtractNamespace(ctx); err == nil {
		entry.Namespace = ns
	}

	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		entry.TraceID = span.SpanContext().TraceID().String()
	}

	// Capture detailed posting-layer metrics from the per-query stats collector.
	if stats != nil {
		entry.DiskReads = stats.DiskReads.Load()
		entry.CacheHits = stats.CacheHits.Load()
		entry.CacheMisses = stats.CacheMisses.Load()
	}

	go writeDebugEntry(ctx, entry)
}

// isDebugInternalRequest returns true if the request is an internal debug write
// to prevent infinite recursion.
func isDebugInternalRequest(req *api.Request) bool {
	if req.Query != "" {
		return false
	}
	for _, mu := range req.Mutations {
		for _, nq := range mu.Set {
			if len(nq.Predicate) > 12 && nq.Predicate[:12] == "dgraph.debug" {
				return true
			}
		}
	}
	return false
}

// FormatDebugQuery returns a DQL query that can be used to find the slowest debug-recorded
// operations. This is a convenience for users to know how to query the debug data.
func FormatDebugQuery(limit int) string {
	return fmt.Sprintf(`{
  slow_queries(func: type(dgraph.debug.Operation), orderdesc: dgraph.debug.latency_total_ns, first: %d) {
    uid
    dgraph.debug.query
    dgraph.debug.variables
    dgraph.debug.operation
    dgraph.debug.timestamp
    dgraph.debug.latency_total_ns
    dgraph.debug.latency_parsing_ns
    dgraph.debug.latency_processing_ns
    dgraph.debug.latency_encoding_ns
    dgraph.debug.latency_assign_ts_ns
    dgraph.debug.uids_total
    dgraph.debug.uid_metrics
    dgraph.debug.disk_reads
    dgraph.debug.cache_hits
    dgraph.debug.cache_misses
    dgraph.debug.error
    dgraph.debug.namespace
    dgraph.debug.trace_id
  }
}`, limit)
}
