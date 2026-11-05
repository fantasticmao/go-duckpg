package benchmarks

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
)

var rowsFlag = flag.Int("rows", 100_000, "row count of the bench table")

type benchSuite struct {
	rows     int
	backends []backend
}

var suite benchSuite

func TestMain(m *testing.M) {
	flag.Parse()
	rows := *rowsFlag
	if testing.Short() && rows == 100_000 {
		rows = 1_000
	}
	if rows < 1 {
		logger.Error("invalid rows")
		os.Exit(1)
	}

	ctx := context.Background()
	backends, err := startAll(ctx, rows)
	if err != nil {
		closeBackends(backends)
		logger.Error("start failed", "err", err)
		os.Exit(1)
	}
	suite = benchSuite{rows: rows, backends: backends}
	code := m.Run()
	closeBackends(backends)
	os.Exit(code)
}

func startAll(ctx context.Context, rows int) ([]backend, error) {
	starters := []struct {
		name  string
		start func(context.Context, int) (backend, checksum, error)
	}{
		{"direct", startDirect},
		{"duckpg", startDuckpg},
		{"quack", startQuack},
	}
	backends := make([]backend, 0, len(starters))
	var reference checksum
	for i, item := range starters {
		logger.Info("prepare", "backend", item.name)
		be, sum, err := item.start(ctx, rows)
		if be != nil {
			backends = append(backends, be)
		}
		if err != nil {
			return backends, err
		}
		if i == 0 {
			reference = sum
			continue
		}
		if !reference.equal(sum) {
			return backends, fmt.Errorf("%s checksum %s, direct %s", be.name(), sum, reference)
		}
	}
	return backends, nil
}

func closeBackends(backends []backend) {
	for _, be := range backends {
		if be == nil {
			continue
		}
		if err := be.close(); err != nil {
			logger.Error("close failed", "backend", be.name(), "err", err)
		}
	}
}

func BenchmarkConnect(b *testing.B) {
	for _, be := range suite.backends {
		b.Run(be.name(), func(b *testing.B) {
			ctx := b.Context()
			for b.Loop() {
				s, err := be.dial(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if err := s.close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSelect1(b *testing.B) {
	benchSession(b, func(ctx context.Context, s session, _ int) error {
		return querySelect1(ctx, s)
	})
}

func BenchmarkAggregate(b *testing.B) {
	rows := suite.rows
	benchSession(b, func(ctx context.Context, s session, _ int) error {
		return queryAggregate(ctx, s, rows)
	})
}

func BenchmarkPoint(b *testing.B) {
	rows := suite.rows
	benchSession(b, func(ctx context.Context, s session, i int) error {
		return queryPoint(ctx, s, i%rows)
	})
}

func BenchmarkScan(b *testing.B) {
	for _, be := range suite.backends {
		for _, n := range scanLimits(suite.rows) {
			b.Run(fmt.Sprintf("%s/%d", be.name(), n), func(b *testing.B) {
				runSession(b, be, func(ctx context.Context, s session, _ int) error {
					return queryScan(ctx, s, n)
				})
			})
		}
	}
}

func BenchmarkInsert(b *testing.B) {
	benchSession(b, func(ctx context.Context, s session, i int) error {
		return execInsert(ctx, s, i+1)
	})
}

func BenchmarkParallelPoint(b *testing.B) {
	rows := suite.rows
	benchParallel(b, func(ctx context.Context, s session, i int) error {
		return queryPoint(ctx, s, i%rows)
	})
}

func BenchmarkParallelInsert(b *testing.B) {
	benchParallel(b, func(ctx context.Context, s session, i int) error {
		return execInsert(ctx, s, i+1)
	})
}

func benchSession(b *testing.B, op func(context.Context, session, int) error) {
	b.Helper()
	for _, be := range suite.backends {
		b.Run(be.name(), func(b *testing.B) {
			runSession(b, be, op)
		})
	}
}

func runSession(b *testing.B, be backend, op func(context.Context, session, int) error) {
	b.Helper()
	ctx := b.Context()
	s, err := be.session(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := s.close(); err != nil {
			b.Error(err)
		}
	}()
	var i int
	for b.Loop() {
		if err := op(ctx, s, i); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func benchParallel(b *testing.B, op func(context.Context, session, int) error) {
	b.Helper()
	for _, be := range suite.backends {
		b.Run(be.name(), func(b *testing.B) {
			ctx := b.Context()
			// RunParallel uses one goroutine per GOMAXPROCS unless SetParallelism changes it.
			n := runtime.GOMAXPROCS(0)
			sessions := make([]session, n)
			for i := range sessions {
				s, err := be.session(ctx)
				if err != nil {
					_ = closeSessions(sessions[:i])
					b.Fatal(err)
				}
				sessions[i] = s
			}
			b.ResetTimer()
			var worker atomic.Int32
			b.RunParallel(func(pb *testing.PB) {
				s := sessions[worker.Add(1)-1]
				var i int
				for pb.Next() {
					if err := op(ctx, s, i); err != nil {
						b.Error(err)
						return
					}
					i++
				}
			})
			b.StopTimer()
			if err := closeSessions(sessions); err != nil {
				b.Error(err)
			}
		})
	}
}

func closeSessions(sessions []session) error {
	var err error
	for _, s := range sessions {
		if s == nil {
			continue
		}
		if closeErr := s.close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
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
