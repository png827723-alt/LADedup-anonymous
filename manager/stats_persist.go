package manager

import (
	"dedup-system/config"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	DedupPhaseSetup          = "setup"
	DedupPhaseFileProcessing = "file_processing"
	DedupPhaseChunkReadHash  = "chunk_read_hash"
	DedupPhaseRecipeWrite    = "recipe_write"
	DedupPhaseMetaLookup     = "global_meta_lookup"
	DedupPhasePlacement      = "placement_decision"
	DedupPhaseNetworkPut     = "network_put_edge_cloud"
	DedupPhaseFlushNodes     = "flush_nodes"
	DedupPhaseSaveGlobalMeta = "save_global_meta"
	DedupPhaseOther          = "other"

	RestorePhaseSetup       = "setup"
	RestorePhaseRecipeScan  = "recipe_scan"
	RestorePhasePolicyCheck = "restore_source_check"
	RestorePhaseWindowPrep  = "window_prepare"
	RestorePhaseEdgeFetch   = "edge_fetch"
	RestorePhaseCloudFetch  = "cloud_fallback_fetch"
	RestorePhaseOutputWrite = "output_write"
	RestorePhaseOther       = "other"
)

var dedupPhaseOrder = []string{
	DedupPhaseSetup,
	DedupPhaseFileProcessing,
	DedupPhaseChunkReadHash,
	DedupPhaseRecipeWrite,
	DedupPhaseMetaLookup,
	DedupPhasePlacement,
	DedupPhaseNetworkPut,
	DedupPhaseFlushNodes,
	DedupPhaseSaveGlobalMeta,
}

var restorePhaseOrder = []string{
	RestorePhaseSetup,
	RestorePhaseRecipeScan,
	RestorePhasePolicyCheck,
	RestorePhaseWindowPrep,
	RestorePhaseEdgeFetch,
	RestorePhaseCloudFetch,
	RestorePhaseOutputWrite,
}

type PhaseMetric struct {
	Phase           string  `json:"phase"`
	DurationSeconds float64 `json:"duration_seconds"`
	Percentage      float64 `json:"percentage"`
}

type TaskFileError struct {
	FilePath string `json:"file_path"`
	Error    string `json:"error"`
}

type DedupFileStats struct {
	FilePath              string  `json:"file_path"`
	RecipePath            string  `json:"recipe_path"`
	Chunks                int     `json:"chunks"`
	OriginalBytes         int64   `json:"original_bytes"`
	UniqueBytes           int64   `json:"unique_bytes"`
	CloudOnlyUniqueChunks int     `json:"cloud_only_unique_chunks"`
	CloudOnlyUniqueBytes  int64   `json:"cloud_only_unique_bytes"`
	NetworkSentBytes      int64   `json:"network_sent_bytes"`
	DurationSeconds       float64 `json:"duration_seconds"`
	ThroughputMBps        float64 `json:"throughput_mb_s"`
	DedupRatio            float64 `json:"dedup_ratio"`
}

type DedupOverallAverage struct {
	FileCount                     int     `json:"file_count"`
	TotalChunks                   int     `json:"total_chunks"`
	TotalOriginalBytes            int64   `json:"total_original_bytes"`
	TotalUniqueBytes              int64   `json:"total_unique_bytes"`
	TotalCloudOnlyUniqueChunks    int     `json:"total_cloud_only_unique_chunks"`
	TotalCloudOnlyUniqueBytes     int64   `json:"total_cloud_only_unique_bytes"`
	TotalNetworkSentBytes         int64   `json:"total_network_sent_bytes"`
	TotalDurationSeconds          float64 `json:"total_duration_seconds"`
	ThroughputMBps                float64 `json:"throughput_mb_s"`
	DedupRatio                    float64 `json:"dedup_ratio"`
	AverageChunksPerFile          float64 `json:"average_chunks_per_file"`
	AverageOriginalBytesPerFile   float64 `json:"average_original_bytes_per_file"`
	AverageUniqueBytesPerFile     float64 `json:"average_unique_bytes_per_file"`
	AverageCloudOnlyChunksPerFile float64 `json:"average_cloud_only_chunks_per_file"`
	AverageCloudOnlyBytesPerFile  float64 `json:"average_cloud_only_bytes_per_file"`
	AverageNetworkBytesPerFile    float64 `json:"average_network_bytes_per_file"`
	AverageDurationPerFile        float64 `json:"average_duration_seconds_per_file"`
}

type DedupStatsSnapshot struct {
	Task           string              `json:"task"`
	GeneratedAt    string              `json:"generated_at"`
	OverallAverage DedupOverallAverage `json:"overall_average"`
	PhaseBreakdown []PhaseMetric       `json:"phase_breakdown,omitempty"`
	Files          []DedupFileStats    `json:"files"`
	FailedFiles    []TaskFileError     `json:"failed_files,omitempty"`
}

type RestoreFileStats struct {
	InputPath          string             `json:"input_path"`
	RecipePath         string             `json:"recipe_path"`
	OutputPath         string             `json:"output_path"`
	NonEdgeChunks      int                `json:"non_edge_chunks"`
	CloudFullDownload  bool               `json:"cloud_full_download"`
	CloudFullThreshold int                `json:"cloud_full_threshold"`
	Chunks             int                `json:"chunks"`
	Bytes              int64              `json:"bytes"`
	DurationSeconds    float64            `json:"duration_seconds"`
	ThroughputMBps     float64            `json:"throughput_mb_s"`
	PhaseBreakdown     []PhaseMetric      `json:"phase_breakdown,omitempty"`
	NodeStats          []RestoreNodeStats `json:"node_stats,omitempty"`
}

type RestoreTimingMetric struct {
	Seconds    float64 `json:"seconds"`
	Percentage float64 `json:"percentage,omitempty"`
	AvgSeconds float64 `json:"avg_seconds,omitempty"`
}

type RestoreExtractMetric struct {
	Seconds            float64 `json:"seconds"`
	Percentage         float64 `json:"percentage,omitempty"`
	InServerPercentage float64 `json:"in_server_percentage,omitempty"`
	AvgSeconds         float64 `json:"avg_seconds,omitempty"`
}

type RestoreNodeStats struct {
	NodeID          string `json:"node_id"`
	NodeNumber      int    `json:"node_number"`
	Batches         int    `json:"batches"`
	ChunksRequested int    `json:"chunks_requested"`
	ChunksFound     int    `json:"chunks_found"`
	MissingChunks   int    `json:"missing_chunks"`
	BytesReturned   int64  `json:"bytes_returned"`

	RpcRoundTrip     RestoreTimingMetric  `json:"rpc_round_trip"`
	ServerTotal      RestoreTimingMetric  `json:"server_total"`
	TransferEstimate RestoreTimingMetric  `json:"transfer_estimate"`
	Extract          RestoreExtractMetric `json:"extract"`

	// Internal aggregation fields (not persisted directly).
	RpcRoundTripSeconds     float64 `json:"-"`
	ServerTotalSeconds      float64 `json:"-"`
	TransferEstimateSeconds float64 `json:"-"`
	ExtractSeconds          float64 `json:"-"`
}

type RestoreOverallAverage struct {
	FileCount              int     `json:"file_count"`
	CloudFullDownloadFiles int     `json:"cloud_full_download_files"`
	TotalChunks            int     `json:"total_chunks"`
	TotalBytes             int64   `json:"total_bytes"`
	TotalDurationSeconds   float64 `json:"total_duration_seconds"`
	ThroughputMBps         float64 `json:"throughput_mb_s"`
	AverageChunksPerFile   float64 `json:"average_chunks_per_file"`
	AverageBytesPerFile    float64 `json:"average_bytes_per_file"`
	AverageDurationPerFile float64 `json:"average_duration_seconds_per_file"`
}

type RestoreStatsSnapshot struct {
	Task           string                `json:"task"`
	GeneratedAt    string                `json:"generated_at"`
	OverallAverage RestoreOverallAverage `json:"overall_average"`
	PhaseBreakdown []PhaseMetric         `json:"phase_breakdown,omitempty"`
	Files          []RestoreFileStats    `json:"files"`
	Nodes          []RestoreNodeStats    `json:"nodes,omitempty"`
}

func safeSeconds(d time.Duration) float64 {
	sec := d.Seconds()
	if sec <= 0 {
		return 0.001
	}
	return sec
}

func throughputMBps(bytes int64, sec float64) float64 {
	if sec <= 0 {
		sec = 0.001
	}
	return float64(bytes) / 1024.0 / 1024.0 / sec
}

func ratioFromBytes(numerator, denominator int64) float64 {
	if denominator > 0 {
		return float64(numerator) / float64(denominator)
	}
	if numerator > 0 {
		return 9999.0
	}
	return 1.0
}

func addPhase(phases map[string]time.Duration, phase string, d time.Duration) {
	if phases == nil || phase == "" || d <= 0 {
		return
	}
	phases[phase] += d
}

func buildPhaseBreakdown(totalSec float64, phases map[string]time.Duration, order []string, otherName string) []PhaseMetric {
	if totalSec <= 0 {
		totalSec = 0.001
	}
	if len(phases) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(order))
	out := make([]PhaseMetric, 0, len(order)+2)
	accounted := 0.0

	for _, name := range order {
		seen[name] = struct{}{}
		sec := phases[name].Seconds()
		if sec <= 0 {
			continue
		}
		out = append(out, PhaseMetric{
			Phase:           name,
			DurationSeconds: sec,
			Percentage:      sec * 100.0 / totalSec,
		})
		accounted += sec
	}

	extras := make([]string, 0)
	for name := range phases {
		if _, ok := seen[name]; ok {
			continue
		}
		if name == otherName {
			continue
		}
		extras = append(extras, name)
	}
	sort.Strings(extras)
	for _, name := range extras {
		sec := phases[name].Seconds()
		if sec <= 0 {
			continue
		}
		out = append(out, PhaseMetric{
			Phase:           name,
			DurationSeconds: sec,
			Percentage:      sec * 100.0 / totalSec,
		})
		accounted += sec
	}

	other := totalSec - accounted
	if other > 0.001 {
		out = append(out, PhaseMetric{
			Phase:           otherName,
			DurationSeconds: other,
			Percentage:      other * 100.0 / totalSec,
		})
	}
	return out
}

func printPhaseBreakdown(metrics []PhaseMetric) {
	if len(metrics) == 0 {
		return
	}
	fmt.Println(" 阶段耗时占比:")
	for _, m := range metrics {
		fmt.Printf("  - %-24s %8.4f s (%6.2f%%)\n", m.Phase, m.DurationSeconds, m.Percentage)
	}
}

func statsOutputDir(cfg config.Config) string {
	if strings.TrimSpace(cfg.StatsPath) != "" {
		return cfg.StatsPath
	}
	if strings.TrimSpace(cfg.RecipePath) != "" {
		return filepath.Join(cfg.RecipePath, "stats")
	}
	return "./stats"
}

func writeJSONAtomic(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func PersistDedupStats(cfg config.Config, s *DedupStats) (string, error) {
	if s == nil {
		return "", nil
	}
	totalSec := safeSeconds(time.Since(s.Start))
	fileCount := len(s.FileStats)
	sumFileSec := 0.0
	for _, fs := range s.FileStats {
		sumFileSec += fs.DurationSeconds
	}

	overall := DedupOverallAverage{
		FileCount:                  fileCount,
		TotalChunks:                s.TotalChunks,
		TotalOriginalBytes:         s.TotalOriginalBytes,
		TotalUniqueBytes:           s.TotalUniqueBytes,
		TotalCloudOnlyUniqueChunks: s.TotalCloudOnlyUniqueChunks,
		TotalCloudOnlyUniqueBytes:  s.TotalCloudOnlyUniqueBytes,
		TotalNetworkSentBytes:      s.NetworkSentBytes,
		TotalDurationSeconds:       totalSec,
		ThroughputMBps:             throughputMBps(s.TotalOriginalBytes, totalSec),
		DedupRatio:                 ratioFromBytes(s.TotalOriginalBytes, s.TotalUniqueBytes),
	}
	if fileCount > 0 {
		div := float64(fileCount)
		overall.AverageChunksPerFile = float64(s.TotalChunks) / div
		overall.AverageOriginalBytesPerFile = float64(s.TotalOriginalBytes) / div
		overall.AverageUniqueBytesPerFile = float64(s.TotalUniqueBytes) / div
		overall.AverageCloudOnlyChunksPerFile = float64(s.TotalCloudOnlyUniqueChunks) / div
		overall.AverageCloudOnlyBytesPerFile = float64(s.TotalCloudOnlyUniqueBytes) / div
		overall.AverageNetworkBytesPerFile = float64(s.NetworkSentBytes) / div
		overall.AverageDurationPerFile = sumFileSec / div
	}

	payload := DedupStatsSnapshot{
		Task:           "dedup",
		GeneratedAt:    time.Now().Format(time.RFC3339),
		OverallAverage: overall,
		PhaseBreakdown: buildPhaseBreakdown(totalSec, s.PhaseDurations, dedupPhaseOrder, DedupPhaseOther),
		Files:          s.FileStats,
		FailedFiles:    s.FailedFiles,
	}

	path := filepath.Join(statsOutputDir(cfg), "dedup_stats.json")
	return path, writeJSONAtomic(path, payload)
}

func PersistRestoreStats(cfg config.Config, s *RestoreStats) (string, error) {
	if s == nil {
		return "", nil
	}
	totalSec := safeSeconds(time.Since(s.Start))
	fileCount := len(s.FileStats)
	sumFileSec := 0.0
	for _, fs := range s.FileStats {
		sumFileSec += fs.DurationSeconds
	}

	overall := RestoreOverallAverage{
		FileCount:              fileCount,
		CloudFullDownloadFiles: s.CloudFullDownloadFiles,
		TotalChunks:            s.TotalChunks,
		TotalBytes:             s.TotalBytes,
		TotalDurationSeconds:   totalSec,
		ThroughputMBps:         throughputMBps(s.TotalBytes, totalSec),
	}
	if fileCount > 0 {
		div := float64(fileCount)
		overall.AverageChunksPerFile = float64(s.TotalChunks) / div
		overall.AverageBytesPerFile = float64(s.TotalBytes) / div
		overall.AverageDurationPerFile = sumFileSec / div
	}

	payload := RestoreStatsSnapshot{
		Task:           "restore",
		GeneratedAt:    time.Now().Format(time.RFC3339),
		OverallAverage: overall,
		PhaseBreakdown: buildPhaseBreakdown(totalSec, s.PhaseDurations, restorePhaseOrder, RestorePhaseOther),
		Files:          s.FileStats,
		Nodes:          s.NodeStats,
	}

	path := filepath.Join(statsOutputDir(cfg), "restore_stats.json")
	return path, writeJSONAtomic(path, payload)
}
