package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	nodeStatsFileName = "node_perf_stats.json"

	dedupPhaseRequestTotal = "request_total"
	dedupPhaseChunkWrap    = "chunk_wrap"
	dedupPhaseEnginePut    = "engine_put"
	dedupPhaseIndexUpdate  = "index_update"
	dedupPhaseOther        = "other"

	restorePhaseRequestTotal = "request_total"
	restorePhaseIndexLookup  = "index_lookup"
	restorePhaseEngineGet    = "engine_get"
	restorePhaseResponsePack = "response_build"
	restorePhaseOther        = "other"

	flushPhaseTotal       = "flush_total"
	flushPhaseEngineFlush = "engine_flush"
	flushPhaseIndexSave   = "index_persist"
	flushPhaseOther       = "other"
)

var dedupPhaseOrder = []string{
	dedupPhaseRequestTotal,
	dedupPhaseChunkWrap,
	dedupPhaseEnginePut,
	dedupPhaseIndexUpdate,
}

var restorePhaseOrder = []string{
	restorePhaseRequestTotal,
	restorePhaseIndexLookup,
	restorePhaseEngineGet,
	restorePhaseResponsePack,
}

var flushPhaseOrder = []string{
	flushPhaseTotal,
	flushPhaseEngineFlush,
	flushPhaseIndexSave,
}

type taskAgg struct {
	Requests      int64
	Bytes         int64
	TotalDuration time.Duration
	PhaseDuration map[string]time.Duration
}

type runtimeStats struct {
	StartedAt time.Time
	Dedup     taskAgg
	Restore   taskAgg
	Flush     taskAgg
}

type NodePhaseMetric struct {
	Phase           string  `json:"phase"`
	DurationSeconds float64 `json:"duration_seconds"`
	Percentage      float64 `json:"percentage"`
}

type NodeTaskStats struct {
	Requests             int64             `json:"requests"`
	Bytes                int64             `json:"bytes,omitempty"`
	TotalDurationSeconds float64           `json:"total_duration_seconds"`
	ThroughputMBps       float64           `json:"throughput_mb_s,omitempty"`
	PhaseBreakdown       []NodePhaseMetric `json:"phase_breakdown,omitempty"`
}

type NodePerfSnapshot struct {
	NodeID       string        `json:"node_id"`
	Role         string        `json:"role"`
	GeneratedAt  string        `json:"generated_at"`
	UptimeSecond float64       `json:"uptime_seconds"`
	Dedup        NodeTaskStats `json:"dedup"`
	Restore      NodeTaskStats `json:"restore"`
	Flush        NodeTaskStats `json:"flush"`
}

func newTaskAgg() taskAgg {
	return taskAgg{
		PhaseDuration: make(map[string]time.Duration),
	}
}

func newRuntimeStats() runtimeStats {
	return runtimeStats{
		StartedAt: time.Now(),
		Dedup:     newTaskAgg(),
		Restore:   newTaskAgg(),
		Flush:     newTaskAgg(),
	}
}

func (a *taskAgg) add(bytes int64, total time.Duration, phases map[string]time.Duration) {
	a.Requests++
	a.Bytes += bytes
	a.TotalDuration += total
	for name, d := range phases {
		if name == "" || d <= 0 {
			continue
		}
		a.PhaseDuration[name] += d
	}
}

func phaseBreakdown(totalSec float64, phase map[string]time.Duration, order []string, otherName string) []NodePhaseMetric {
	if totalSec <= 0 {
		totalSec = 0.001
	}
	if len(phase) == 0 {
		return nil
	}

	out := make([]NodePhaseMetric, 0, len(order)+2)
	seen := make(map[string]struct{}, len(order))
	accounted := 0.0

	for _, p := range order {
		seen[p] = struct{}{}
		sec := phase[p].Seconds()
		if sec <= 0 {
			continue
		}
		out = append(out, NodePhaseMetric{
			Phase:           p,
			DurationSeconds: sec,
			Percentage:      sec * 100.0 / totalSec,
		})
		accounted += sec
	}

	extras := make([]string, 0)
	for p := range phase {
		if _, ok := seen[p]; ok || p == otherName {
			continue
		}
		extras = append(extras, p)
	}
	sort.Strings(extras)
	for _, p := range extras {
		sec := phase[p].Seconds()
		if sec <= 0 {
			continue
		}
		out = append(out, NodePhaseMetric{
			Phase:           p,
			DurationSeconds: sec,
			Percentage:      sec * 100.0 / totalSec,
		})
		accounted += sec
	}

	other := totalSec - accounted
	if other > 0.001 {
		out = append(out, NodePhaseMetric{
			Phase:           otherName,
			DurationSeconds: other,
			Percentage:      other * 100.0 / totalSec,
		})
	}
	return out
}

func safeSec(d time.Duration) float64 {
	sec := d.Seconds()
	if sec <= 0 {
		return 0.001
	}
	return sec
}

func throughputMBps(bytes int64, sec float64) float64 {
	if sec <= 0 {
		return 0
	}
	return float64(bytes) / 1024.0 / 1024.0 / sec
}

func taskSnapshot(a taskAgg, order []string, otherName string) NodeTaskStats {
	totalSec := safeSec(a.TotalDuration)
	return NodeTaskStats{
		Requests:             a.Requests,
		Bytes:                a.Bytes,
		TotalDurationSeconds: totalSec,
		ThroughputMBps:       throughputMBps(a.Bytes, totalSec),
		PhaseBreakdown:       phaseBreakdown(totalSec, a.PhaseDuration, order, otherName),
	}
}

func persistSnapshotAtomic(path string, s NodePerfSnapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
