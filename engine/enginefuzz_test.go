// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/thanos-io/promql-engine/engine"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/cortexproject/promqlsmith"
	"github.com/efficientgo/core/errors"
	"github.com/efficientgo/core/testutil"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/stretchr/testify/require"
)

const testRuns = 100

type testCase struct {
	query              string
	loads              []string
	oldRes, newRes     *promql.Result
	oldStats, newStats *stats.Statistics
	start, end         time.Time
	interval           time.Duration
	validateSamples    bool
}

// shouldValidateSamples checks if the samples can be compared for the expr.
// For certain known cases, prometheus engine and thanos engine returns different samples.
func shouldValidateSamples(expr parser.Expr) bool {
	valid := true

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.Call:
			if n.Func.Name == "scalar" {
				valid = false
				return errors.New("error")
			}
		}
		return nil
	})
	return valid
}

func FuzzEnginePromQLSmithRangeQuery(f *testing.F) {
	f.Add(int64(0), uint32(0), uint32(120), uint32(30), 1.0, 1.0, 1.0, 2.0, 30)

	f.Fuzz(func(t *testing.T, seed int64, startTS, endTS, intervalSeconds uint32, initialVal1, initialVal2, inc1, inc2 float64, stepRange int) {
		if math.IsNaN(initialVal1) || math.IsNaN(initialVal2) || math.IsNaN(inc1) || math.IsNaN(inc2) {
			return
		}
		if math.IsInf(initialVal1, 0) || math.IsInf(initialVal2, 0) || math.IsInf(inc1, 0) || math.IsInf(inc2, 0) {
			return
		}
		if inc1 < 0 || inc2 < 0 || stepRange <= 0 || intervalSeconds <= 0 || endTS < startTS {
			return
		}
		rnd := rand.New(rand.NewSource(seed))

		load := fmt.Sprintf(`load 30s
			http_requests_total{pod="nginx-1"} %.2f+%.2fx15
			http_requests_total{pod="nginx-2"} %2.f+%.2fx21`, initialVal1, inc1, initialVal2, inc2)

		opts := promql.EngineOpts{
			Timeout:              1 * time.Hour,
			MaxSamples:           1e10,
			EnableNegativeOffset: true,
			EnableAtModifier:     true,
			EnablePerStepStats:   true,
		}
		qOpts := promql.NewPrometheusQueryOpts(true, 0)

		storage := promqltest.LoadedStorage(t, load)
		defer storage.Close()

		start := time.Unix(int64(startTS), 0)
		end := time.Unix(int64(endTS), 0)
		interval := time.Duration(intervalSeconds) * time.Second

		seriesSet, err := getSeries(context.Background(), storage)
		require.NoError(t, err)
		psOpts := []promqlsmith.Option{
			promqlsmith.WithEnableOffset(true),
			promqlsmith.WithEnableAtModifier(true),
			// bottomk and topk sometimes lead to random failures since their result on equal values is essentially random
			promqlsmith.WithEnabledAggrs([]parser.ItemType{parser.SUM, parser.MIN, parser.MAX, parser.AVG, parser.GROUP, parser.COUNT, parser.COUNT_VALUES, parser.QUANTILE}),
		}
		ps := promqlsmith.New(rnd, seriesSet, psOpts...)

		newEngine := engine.New(engine.Opts{EngineOpts: opts, EnableAnalysis: true})
		oldEngine := promql.NewEngine(opts)

		var (
			q1              promql.Query
			query           string
			validateSamples bool
		)
		cases := make([]*testCase, testRuns)
		for i := 0; i < testRuns; i++ {
			for {
				expr := ps.WalkRangeQuery()
				validateSamples = shouldValidateSamples(expr)

				query = expr.Pretty(0)
				q1, err = newEngine.NewRangeQuery(context.Background(), storage, qOpts, query, start, end, interval)
				if engine.IsUnimplemented(err) || errors.As(err, &parser.ParseErrors{}) {
					continue
				} else {
					break
				}
			}

			testutil.Ok(t, err)
			newResult := q1.Exec(context.Background())
			newStats := q1.Stats()
			stats.NewQueryStats(newStats)

			q2, err := oldEngine.NewRangeQuery(context.Background(), storage, qOpts, query, start, end, interval)
			testutil.Ok(t, err)

			oldResult := q2.Exec(context.Background())
			oldStats := q2.Stats()
			stats.NewQueryStats(oldStats)

			cases[i] = &testCase{
				query:           query,
				newRes:          newResult,
				newStats:        newStats,
				oldRes:          oldResult,
				oldStats:        oldStats,
				loads:           []string{load},
				start:           start,
				end:             end,
				interval:        interval,
				validateSamples: validateSamples,
			}
		}
		validateTestCases(t, cases)
	})
}

func FuzzEnginePromQLSmithInstantQuery(f *testing.F) {
	f.Add(int64(0), uint32(0), 1.0, 1.0, 1.0, 2.0)

	f.Fuzz(func(t *testing.T, seed int64, ts uint32, initialVal1, initialVal2, inc1, inc2 float64) {
		t.Parallel()
		if inc1 < 0 || inc2 < 0 {
			return
		}
		rnd := rand.New(rand.NewSource(seed))

		load := fmt.Sprintf(`load 30s
			http_requests_total{pod="nginx-1", route="/"} %.2f+%.2fx40
			http_requests_total{pod="nginx-2", route="/"} %2.f+%.2fx40`, initialVal1, inc1, initialVal2, inc2)

		opts := promql.EngineOpts{
			Timeout:              1 * time.Hour,
			MaxSamples:           1e10,
			EnableNegativeOffset: true,
			EnableAtModifier:     true,
			EnablePerStepStats:   true,
		}
		qOpts := promql.NewPrometheusQueryOpts(true, 0)

		storage := promqltest.LoadedStorage(t, load)
		defer storage.Close()

		queryTime := time.Unix(int64(ts), 0)
		newEngine := engine.New(engine.Opts{
			EngineOpts:        opts,
			LogicalOptimizers: logicalplan.AllOptimizers,
			EnableAnalysis:    true,
		})
		oldEngine := promql.NewEngine(opts)

		seriesSet, err := getSeries(context.Background(), storage)
		require.NoError(t, err)
		psOpts := []promqlsmith.Option{
			promqlsmith.WithEnableOffset(true),
			promqlsmith.WithEnableAtModifier(true),
			promqlsmith.WithAtModifierMaxTimestamp(180 * 1000),
			// bottomk and topk sometimes lead to random failures since their result on equal values is essentially random
			promqlsmith.WithEnabledAggrs([]parser.ItemType{parser.SUM, parser.MIN, parser.MAX, parser.AVG, parser.GROUP, parser.COUNT, parser.COUNT_VALUES, parser.QUANTILE}),
		}
		ps := promqlsmith.New(rnd, seriesSet, psOpts...)

		var (
			q1    promql.Query
			query string
		)
		cases := make([]*testCase, testRuns)
		for i := 0; i < testRuns; i++ {
			// Since we disabled fallback, keep trying until we find a query
			// that can be natively execute by the engine.
			// Parsing experimental function, like mad_over_time, will lead to a parser.ParseErrors, so we also ignore those.
			for {
				expr := ps.WalkInstantQuery()
				if !shouldValidateSamples(expr) {
					continue
				}
				query = expr.Pretty(0)
				q1, err = newEngine.NewInstantQuery(context.Background(), storage, qOpts, query, queryTime)
				if engine.IsUnimplemented(err) || errors.As(err, &parser.ParseErrors{}) {
					continue
				} else {
					break
				}
			}

			testutil.Ok(t, err)
			newResult := q1.Exec(context.Background())
			newStats := q1.Stats()
			stats.NewQueryStats(newStats)

			q2, err := oldEngine.NewInstantQuery(context.Background(), storage, qOpts, query, queryTime)
			testutil.Ok(t, err)

			oldResult := q2.Exec(context.Background())
			oldStats := q2.Stats()
			stats.NewQueryStats(oldStats)

			cases[i] = &testCase{
				query:           query,
				newRes:          newResult,
				newStats:        newStats,
				oldRes:          oldResult,
				oldStats:        oldStats,
				loads:           []string{load},
				start:           queryTime,
				end:             queryTime,
				validateSamples: true,
			}
		}
		validateTestCases(t, cases)
	})
}

func getSeries(ctx context.Context, q storage.Queryable) ([]labels.Labels, error) {
	querier, err := q.Querier(0, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	res := make([]labels.Labels, 0)
	ss := querier.Select(ctx, false, &storage.SelectHints{Func: "series"}, labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"))
	for ss.Next() {
		lbls := ss.At().Labels()
		res = append(res, lbls)
	}
	if err := ss.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func validateTestCases(t *testing.T, cases []*testCase) {
	failures := 0
	logQuery := func(c *testCase) {
		for _, load := range c.loads {
			t.Log(load)
		}
		t.Logf("query: %s, start: %d, end: %d, interval: %v", c.query, c.start.UnixMilli(), c.end.UnixMilli(), c.interval)
	}
	for i, c := range cases {
		if !cmp.Equal(c.oldRes, c.newRes, comparer) {
			logQuery(c)

			t.Logf("case %d error mismatch.\nnew result: %s\nold result: %s\n", i, c.newRes.String(), c.oldRes.String())
			failures++
			continue
		}
		if !c.validateSamples || c.oldRes.Err != nil {
			// Skip sample comparison
			continue
		}
		if !cmp.Equal(c.oldStats.Samples, c.newStats.Samples, samplesComparer) {
			logQuery(c)
			t.Logf("case: %d, samples mismatch. total samples: old: %v, new: %v. samples per step: old: %v, new: %v", i, c.oldStats.Samples.TotalSamples, c.newStats.Samples.TotalSamples, c.oldStats.Samples.TotalSamplesPerStep, c.newStats.Samples.TotalSamplesPerStep)
			failures++
		}
	}
	if failures > 0 {
		t.Fatalf("failed %d test cases", failures)
	}
}
