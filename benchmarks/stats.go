package main

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"
)

func median(samples []time.Duration) time.Duration {
	cp := append([]time.Duration(nil), samples...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	n := len(cp)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return cp[n/2]
	}
	return (cp[n/2-1] + cp[n/2]) / 2
}

// percentile returns the nearest-rank value. p is in the range 0 to 1.
func percentile(samples []time.Duration, p float64) time.Duration {
	cp := append([]time.Duration(nil), samples...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	n := len(cp)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return cp[0]
	}
	if p >= 1 {
		return cp[n-1]
	}
	idx := int(math.Ceil(p*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return cp[idx]
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
}

func formatRate(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return "n/a"
	}
	if v >= 100 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.2f", v)
}

func formatRatio(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", v)
}

func timeRatio(base, other time.Duration) float64 {
	if base <= 0 || other < 0 {
		return math.NaN()
	}
	return float64(other) / float64(base)
}

type reportLine struct {
	workload string
	metric   string
	values   [3]string
	ratios   [2]string
}

type reportHeader struct {
	goVersion string
	cpus      int
	duckdb    string
	rows      int
	quick     bool
}

func printReport(w io.Writer, header reportHeader, lines []reportLine) {
	fmt.Fprintf(w, "go: %s\n", header.goVersion)
	fmt.Fprintf(w, "cpu: %d\n", header.cpus)
	fmt.Fprintf(w, "duckdb: %s\n", header.duckdb)
	fmt.Fprintf(w, "rows: %d\n", header.rows)
	fmt.Fprintf(w, "quick: %t\n\n", header.quick)

	headers := []string{"workload", "metric", "direct", "duckpg", "quack", "duckpg/direct", "quack/direct"}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	cells := make([][7]string, len(lines))
	for i, line := range lines {
		cells[i] = [7]string{
			line.workload, line.metric,
			line.values[0], line.values[1], line.values[2],
			line.ratios[0], line.ratios[1],
		}
		for col, cell := range cells[i] {
			if len(cell) > widths[col] {
				widths[col] = len(cell)
			}
		}
	}

	fmt.Fprintln(w, formatRow(headers, widths))
	for _, row := range cells {
		fmt.Fprintln(w, formatRow(row[:], widths))
	}
}

func formatRow(cells []string, widths []int) string {
	var b strings.Builder
	for i, cell := range cells {
		if i > 0 {
			b.WriteString("  ")
		}
		if i < 2 {
			b.WriteString(padRight(cell, widths[i]))
			continue
		}
		b.WriteString(padLeft(cell, widths[i]))
	}
	return b.String()
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func padLeft(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return strings.Repeat(" ", n-len(s)) + s
}
