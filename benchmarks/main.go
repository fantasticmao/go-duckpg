package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

var stderr = os.Stderr

func main() {
	rowsFlag := flag.Int("rows", 100_000, "row count of the bench table")
	quick := flag.Bool("quick", false, "fewer rows and iterations for a smoke run")
	flag.Parse()

	rows := *rowsFlag
	if *quick && rows == 100_000 {
		rows = 1_000
	}
	if rows < 1 {
		fatal(fmt.Errorf("-rows must be at least 1"))
	}

	ctx := context.Background()
	counts := loadCounts(*quick)
	backends := []Backend{newDirect(), newDuckpg(), newQuack()}
	defer closeBackends(backends)

	var reference Checksum
	var duckdbVersion string
	for i, backend := range backends {
		fmt.Fprintf(stderr, "prepare %s\n", backend.Name())
		sum, err := backend.Prepare(ctx, rows)
		if err != nil {
			fatal(err)
		}
		if i == 0 {
			reference = sum
			duckdbVersion, err = versionOf(ctx, backend)
			if err != nil {
				fatal(err)
			}
			continue
		}
		if !reference.equal(sum) {
			fatal(fmt.Errorf("%s checksum %s, direct %s", backend.Name(), sum, reference))
		}
	}

	var lines []reportLine
	lines = append(lines, runConnect(ctx, backends, counts.connect)...)
	lines = append(lines, runSession(ctx, backends, "select_1", counts.select1, "", 0, func(ctx context.Context, s Session, _ int) error {
		return querySelect1(ctx, s)
	})...)
	lines = append(lines, runSession(ctx, backends, "aggregate", counts.aggregate, "", 0, func(ctx context.Context, s Session, _ int) error {
		return queryAggregate(ctx, s, rows)
	})...)
	lines = append(lines, runSession(ctx, backends, "point", counts.point, "", 0, func(ctx context.Context, s Session, i int) error {
		return queryPoint(ctx, s, i%rows)
	})...)
	for _, n := range scanLimits(rows) {
		limit := n
		name := fmt.Sprintf("scan_%d", limit)
		iters := scanIters(limit, *quick)
		lines = append(lines, runSession(ctx, backends, name, iters, "rows/s", float64(limit), func(ctx context.Context, s Session, _ int) error {
			return queryScan(ctx, s, limit)
		})...)
	}
	lines = append(lines, runSession(ctx, backends, "insert", counts.insert, "tx/s", 1, func(ctx context.Context, s Session, i int) error {
		return execInsert(ctx, s, i+1)
	})...)
	lines = append(lines, runParallel(ctx, backends, "parallel_point", counts.parallel, "qps", func(ctx context.Context, s Session, id int) error {
		return queryPoint(ctx, s, id%rows)
	})...)
	lines = append(lines, runParallel(ctx, backends, "parallel_insert", counts.parallel, "tx/s", func(ctx context.Context, s Session, id int) error {
		return execInsert(ctx, s, id)
	})...)

	fmt.Fprintln(os.Stdout)
	printReport(os.Stdout, reportHeader{
		goVersion: runtime.Version(),
		cpus:      runtime.NumCPU(),
		duckdb:    duckdbVersion,
		rows:      rows,
		quick:     *quick,
	}, lines)
}

type counts struct {
	connect   int
	select1   int
	aggregate int
	point     int
	insert    int
	parallel  int
}

func loadCounts(quick bool) counts {
	if quick {
		return counts{connect: 3, select1: 8, aggregate: 4, point: 8, insert: 8, parallel: 4}
	}
	return counts{connect: 20, select1: 200, aggregate: 40, point: 100, insert: 200, parallel: 50}
}

func scanLimits(rows int) []int {
	var out []int
	add := func(n int) {
		if n <= 0 || n > rows {
			return
		}
		for _, existing := range out {
			if existing == n {
				return
			}
		}
		out = append(out, n)
	}
	add(1_000)
	add(10_000)
	add(rows)
	return out
}

func scanIters(n int, quick bool) int {
	if quick {
		return 2
	}
	switch {
	case n >= 100_000:
		return 3
	case n >= 10_000:
		return 5
	default:
		return 10
	}
}

func warmupN(iters int) int {
	if iters <= 2 {
		return 1
	}
	return 2
}

type observation struct {
	samples  []time.Duration
	rateName string
	rate     float64
}

func runConnect(ctx context.Context, backends []Backend, iters int) []reportLine {
	obs := make([]observation, len(backends))
	for i, backend := range backends {
		fmt.Fprintf(stderr, "run %s connect\n", backend.Name())
		samples, err := measureDial(ctx, backend, iters, warmupN(iters))
		if err != nil {
			fatal(fmt.Errorf("%s connect: %w", backend.Name(), err))
		}
		obs[i] = observation{samples: samples}
	}
	return linesFor("connect", obs)
}

func measureDial(ctx context.Context, backend Backend, iters, warmup int) ([]time.Duration, error) {
	once := func() (time.Duration, error) {
		start := time.Now()
		session, err := backend.Dial(ctx)
		elapsed := time.Since(start)
		if err != nil {
			return 0, err
		}
		if err := session.Close(); err != nil {
			return 0, err
		}
		return elapsed, nil
	}
	for i := 0; i < warmup; i++ {
		if _, err := once(); err != nil {
			return nil, err
		}
	}
	samples := make([]time.Duration, iters)
	for i := 0; i < iters; i++ {
		elapsed, err := once()
		if err != nil {
			return nil, err
		}
		samples[i] = elapsed
	}
	return samples, nil
}

func runSession(ctx context.Context, backends []Backend, name string, iters int, rateName string, perOp float64, op func(context.Context, Session, int) error) []reportLine {
	obs := make([]observation, len(backends))
	for i, backend := range backends {
		fmt.Fprintf(stderr, "run %s %s\n", backend.Name(), name)
		session, err := backend.Session(ctx)
		if err != nil {
			fatal(fmt.Errorf("%s %s: %w", backend.Name(), name, err))
		}
		step := 0
		samples, err := measure(iters, warmupN(iters), func() error {
			id := step
			step++
			return op(ctx, session, id)
		})
		closeErr := session.Close()
		if err != nil {
			fatal(fmt.Errorf("%s %s: %w", backend.Name(), name, err))
		}
		if closeErr != nil {
			fatal(fmt.Errorf("%s %s close: %w", backend.Name(), name, closeErr))
		}
		obs[i] = observation{samples: samples, rateName: rateName, rate: rateOf(perOp, samples)}
	}
	return linesFor(name, obs)
}

func measure(iters, warmup int, fn func() error) ([]time.Duration, error) {
	for i := 0; i < warmup; i++ {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	samples := make([]time.Duration, iters)
	for i := 0; i < iters; i++ {
		start := time.Now()
		if err := fn(); err != nil {
			return nil, err
		}
		samples[i] = time.Since(start)
	}
	return samples, nil
}

func rateOf(perOp float64, samples []time.Duration) float64 {
	if perOp <= 0 {
		return 0
	}
	med := median(samples)
	if med <= 0 {
		return 0
	}
	return perOp / med.Seconds()
}

func linesFor(name string, obs []observation) []reportLine {
	medians := [3]time.Duration{}
	p99s := [3]time.Duration{}
	for i, item := range obs {
		medians[i] = median(item.samples)
		p99s[i] = percentile(item.samples, 0.99)
	}
	ratio := func(base, other time.Duration) string {
		return formatRatio(timeRatio(base, other))
	}
	lines := []reportLine{
		{
			workload: name,
			metric:   "median",
			values:   [3]string{formatDuration(medians[0]), formatDuration(medians[1]), formatDuration(medians[2])},
			ratios:   [2]string{ratio(medians[0], medians[1]), ratio(medians[0], medians[2])},
		},
		{
			workload: name,
			metric:   "p99",
			values:   [3]string{formatDuration(p99s[0]), formatDuration(p99s[1]), formatDuration(p99s[2])},
			ratios:   [2]string{ratio(p99s[0], p99s[1]), ratio(p99s[0], p99s[2])},
		},
	}
	if obs[0].rateName == "" {
		return lines
	}
	lines = append(lines, reportLine{
		workload: name,
		metric:   obs[0].rateName,
		values:   [3]string{formatRate(obs[0].rate), formatRate(obs[1].rate), formatRate(obs[2].rate)},
		ratios:   [2]string{formatRatio(obs[0].rate / obs[1].rate), formatRatio(obs[0].rate / obs[2].rate)},
	})
	return lines
}

func runParallel(ctx context.Context, backends []Backend, name string, iters int, rateName string, op func(context.Context, Session, int) error) []reportLine {
	obs := make([]observation, len(backends))
	for i, backend := range backends {
		fmt.Fprintf(stderr, "run %s %s\n", backend.Name(), name)
		sessions := make([]Session, parallelWorkers)
		for w := 0; w < parallelWorkers; w++ {
			session, err := backend.Session(ctx)
			if err != nil {
				closeSessions(sessions[:w])
				fatal(fmt.Errorf("%s %s: %w", backend.Name(), name, err))
			}
			sessions[w] = session
		}
		for w, session := range sessions {
			if err := op(ctx, session, w); err != nil {
				closeSessions(sessions)
				fatal(fmt.Errorf("%s %s warmup: %w", backend.Name(), name, err))
			}
		}
		samples, wall, err := parallelSamples(ctx, sessions, iters, op)
		closeSessions(sessions)
		if err != nil {
			fatal(fmt.Errorf("%s %s: %w", backend.Name(), name, err))
		}
		total := float64(parallelWorkers * iters)
		rate := 0.0
		if wall > 0 {
			rate = total / wall.Seconds()
		}
		obs[i] = observation{samples: samples, rateName: rateName, rate: rate}
	}
	return linesFor(name, obs)
}

func parallelSamples(ctx context.Context, sessions []Session, iters int, op func(context.Context, Session, int) error) ([]time.Duration, time.Duration, error) {
	samples := make([]time.Duration, len(sessions)*iters)
	var wg sync.WaitGroup
	errCh := make(chan error, len(sessions))
	start := time.Now()
	for w, session := range sessions {
		wg.Add(1)
		go func(w int, session Session) {
			defer wg.Done()
			base := w * iters
			for i := 0; i < iters; i++ {
				opStart := time.Now()
				if err := op(ctx, session, base+i+parallelWorkers); err != nil {
					errCh <- err
					return
				}
				samples[base+i] = time.Since(opStart)
			}
		}(w, session)
	}
	wg.Wait()
	wall := time.Since(start)
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, wall, err
		}
	}
	return samples, wall, nil
}

func versionOf(ctx context.Context, backend Backend) (string, error) {
	session, err := backend.Session(ctx)
	if err != nil {
		return "", err
	}
	defer session.Close()
	version, err := queryVersion(ctx, session)
	if err != nil {
		return "", fmt.Errorf("duckdb version: %w", err)
	}
	return version, nil
}

func closeSessions(sessions []Session) {
	for _, session := range sessions {
		if session != nil {
			_ = session.Close()
		}
	}
}

func closeBackends(backends []Backend) {
	for _, backend := range backends {
		_ = backend.Close()
	}
}

func fatal(err error) {
	fmt.Fprintf(stderr, "benchmark: %v\n", err)
	os.Exit(1)
}
