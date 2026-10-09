package manager

import (
	"bufio"
	"dedup-system/config"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	RestoreExperimentEdgeAll   = "edge-all"
	RestoreExperimentCloudFull = "cloud-full"
	RestoreExperimentHybrid    = "hybrid"
)

func RunRestoreExperiment(cfg config.Config, mode, inputNameOrPath, outputPath string, edgeRatio float64) error {
	return RunRestoreExperimentBatch(cfg, mode, []RestoreRequest{
		{
			InputNameOrPath: inputNameOrPath,
			OutputPath:      outputPath,
		},
	}, edgeRatio)
}

func RunRestoreExperimentBatch(cfg config.Config, mode string, requests []RestoreRequest, edgeRatio float64) error {
	mode = strings.TrimSpace(strings.ToLower(mode))
	switch mode {
	case RestoreExperimentEdgeAll, RestoreExperimentCloudFull, RestoreExperimentHybrid:
	default:
		return fmt.Errorf("unknown restore experiment mode: %s", mode)
	}
	if mode == RestoreExperimentHybrid {
		if edgeRatio < 0 || edgeRatio > 1 {
			return fmt.Errorf("edge_ratio must be within [0,1], got %.4f", edgeRatio)
		}
	}
	if len(requests) == 0 {
		return fmt.Errorf("restore requests is empty")
	}

	runStart := time.Now()
	setupStart := runStart
	setupDuration := time.Duration(0)

	if !cfg.DistributedEnabled {
		return fmt.Errorf("distributed_enabled must be true for manager")
	}
	if cfg.RecipePath == "" {
		cfg.RecipePath = "./recipes"
	}
	if cfg.GlobalMetaPath == "" {
		cfg.GlobalMetaPath = cfg.MetaPath
	}
	if cfg.CloudNode.ID == "" {
		return fmt.Errorf("cloud_node must be configured for restore experiments")
	}

	var err error
	var gm map[string]int
	if mode != RestoreExperimentCloudFull {
		gm, err = LoadGlobalMeta(cfg.GlobalMetaPath)
		if err != nil {
			return fmt.Errorf("load global meta: %w", err)
		}
	}
	setupDuration += time.Since(setupStart)

	pool := NewNodePool(cfg)
	defer pool.Close()

	warmupEndpoints := []config.NodeEndpoint{cfg.CloudNode}
	if mode != RestoreExperimentCloudFull {
		warmupEndpoints = allStorageEndpoints(cfg, true)
	}
	warmupStart := time.Now()
	if err := pool.Warmup(warmupEndpoints); err != nil {
		return err
	}
	setupDuration += time.Since(warmupStart)

	stats := &RestoreStats{
		Start:          runStart,
		PhaseDurations: make(map[string]time.Duration),
		FileStats:      make([]RestoreFileStats, 0, len(requests)),
		NodeStats:      make([]RestoreNodeStats, 0),
	}
	addPhase(stats.PhaseDurations, RestorePhaseSetup, setupDuration)
	runNodeAgg := make(map[string]*RestoreNodeStats)
	fmt.Printf("[RestoreExp] batch started: mode=%s files=%d\n", mode, len(requests))
	nextProgressPct := 20
	for i, req := range requests {
		if strings.TrimSpace(req.InputNameOrPath) == "" {
			return fmt.Errorf("invalid restore request[%d]: empty input", i)
		}
		if !cfg.RestoreDiscardOutput && strings.TrimSpace(req.OutputPath) == "" {
			return fmt.Errorf("invalid restore request[%d]: empty output", i)
		}
		if err := runRestoreExperimentOneFile(cfg, pool, gm, mode, edgeRatio, req, stats, runNodeAgg); err != nil {
			return err
		}
		pct := int(float64(i+1) * 100.0 / float64(len(requests)))
		if pct >= nextProgressPct || i+1 == len(requests) {
			if pct > 100 {
				pct = 100
			}
			fmt.Printf("[RestoreExp] progress: %d%% (%d/%d files)\n", pct, i+1, len(requests))
			for nextProgressPct <= pct {
				nextProgressPct += 20
			}
		}
	}
	stats.NodeStats = nodeStatsList(runNodeAgg)

	statsPath, err := PersistRestoreStats(cfg, stats)
	if err != nil {
		return fmt.Errorf("persist restore stats: %w", err)
	}
	if mode == RestoreExperimentHybrid {
		fmt.Printf("[RestoreExp] mode=%s edge_ratio=%.2f stats=%s\n", mode, edgeRatio, statsPath)
	} else {
		fmt.Printf("[RestoreExp] mode=%s stats=%s\n", mode, statsPath)
	}
	stats.report()
	return nil
}

func runRestoreExperimentOneFile(cfg config.Config, pool *NodePool, gm map[string]int, mode string, edgeRatio float64, req RestoreRequest, stats *RestoreStats, runNodeAgg map[string]*RestoreNodeStats) error {
	fileStart := time.Now()
	recipePath, err := ResolveRecipePath(cfg.RecipePath, req.InputNameOrPath)
	if err != nil {
		return err
	}
	recipeScanStart := time.Now()
	hashes, err := loadRecipeHashes(recipePath)
	if err != nil {
		return err
	}
	recipeScanDuration := time.Since(recipeScanStart)

	fileStats := RestoreFileStats{
		InputPath:  req.InputNameOrPath,
		RecipePath: recipePath,
		OutputPath: req.OutputPath,
	}
	if cfg.RestoreDiscardOutput {
		fileStats.OutputPath = ""
	}
	fileNodeAgg := make(map[string]*RestoreNodeStats)
	filePhases := make(map[string]time.Duration)
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseRecipeScan, recipeScanDuration)

	if !cfg.RestoreDiscardOutput {
		if err := os.MkdirAll(filepath.Dir(req.OutputPath), 0755); err != nil {
			return err
		}
	}
	out, closeOutput, err := newRestoreOutputWriter(cfg, req.OutputPath)
	if err != nil {
		return err
	}
	defer closeOutput()

	switch mode {
	case RestoreExperimentEdgeAll:
		if err := restoreExperimentEdgeAll(cfg, pool, gm, hashes, out, stats, &fileStats, fileNodeAgg, runNodeAgg, filePhases); err != nil {
			return err
		}
	case RestoreExperimentCloudFull:
		fileStats.CloudFullDownload = true
		stats.CloudFullDownloadFiles++
		objectNames, err := fullFileObjectNameCandidates(req.InputNameOrPath, recipePath)
		if err != nil {
			return err
		}
		cloudFetchStart := time.Now()
		objectName, bytesFetched, nodeStat, err := downloadFullFileFromCloudAny(cfg, pool, objectNames, out, len(hashes))
		if err != nil {
			return fmt.Errorf("cloud full download failed for %s: %w", objectName, err)
		}
		addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseCloudFetch, time.Since(cloudFetchStart))
		mergeNodeStat(fileNodeAgg, nodeStat)
		mergeNodeStat(runNodeAgg, nodeStat)
		stats.TotalBytes += bytesFetched
		stats.TotalChunks += len(hashes)
		fileStats.Bytes += bytesFetched
		fileStats.Chunks += len(hashes)
	case RestoreExperimentHybrid:
		if err := restoreExperimentHybrid(cfg, pool, gm, hashes, edgeRatio, out, stats, &fileStats, fileNodeAgg, runNodeAgg, filePhases); err != nil {
			return err
		}
	}

	flushStart := time.Now()
	if err := out.Flush(); err != nil {
		return err
	}
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseOutputWrite, time.Since(flushStart))

	fileSec := safeSeconds(time.Since(fileStart))
	fileStats.DurationSeconds = fileSec
	fileStats.ThroughputMBps = throughputMBps(fileStats.Bytes, fileSec)
	fileStats.PhaseBreakdown = buildPhaseBreakdown(fileSec, filePhases, restorePhaseOrder, RestorePhaseOther)
	fileStats.NodeStats = nodeStatsList(fileNodeAgg)
	stats.FileStats = append(stats.FileStats, fileStats)
	return nil
}

func restoreExperimentServiceSeconds(phases map[string]time.Duration) float64 {
	if len(phases) == 0 {
		return 0.001
	}
	service := time.Duration(0)
	service += phases[RestorePhaseEdgeFetch]
	service += phases[RestorePhaseCloudFetch]
	service += phases[RestorePhaseOutputWrite]
	return safeSeconds(service)
}

func restoreExperimentEdgeAll(cfg config.Config, pool *NodePool, gm map[string]int, hashes []string, out io.Writer, stats *RestoreStats, fileStats *RestoreFileStats, fileNodeAgg map[string]*RestoreNodeStats, runNodeAgg map[string]*RestoreNodeStats, filePhases map[string]time.Duration) error {
	windowSize := cfg.RestoreWindowSize
	if windowSize <= 0 {
		windowSize = 1024
	}
	for start := 0; start < len(hashes); start += windowSize {
		end := start + windowSize
		if end > len(hashes) {
			end = len(hashes)
		}
		if err := restoreWindow(cfg, pool, gm, hashes[start:end], out, stats, fileStats, fileNodeAgg, runNodeAgg, filePhases); err != nil {
			return err
		}
	}
	return nil
}

type experimentFetchTask struct {
	source     string
	nodeNumber int
	endpoint   config.NodeEndpoint
	hashes     []string
}

type experimentFetchResult struct {
	task experimentFetchTask
	res  fetchResult
}

func restoreExperimentHybrid(cfg config.Config, pool *NodePool, gm map[string]int, hashes []string, edgeRatio float64, out io.Writer, stats *RestoreStats, fileStats *RestoreFileStats, fileNodeAgg map[string]*RestoreNodeStats, runNodeAgg map[string]*RestoreNodeStats, filePhases map[string]time.Duration) error {
	windowSize := cfg.RestoreWindowSize
	if windowSize <= 0 {
		windowSize = 1024
	}
	for start := 0; start < len(hashes); start += windowSize {
		end := start + windowSize
		if end > len(hashes) {
			end = len(hashes)
		}
		if err := restoreHybridWindow(cfg, pool, gm, hashes[start:end], edgeRatio, out, stats, fileStats, fileNodeAgg, runNodeAgg, filePhases); err != nil {
			return err
		}
	}
	return nil
}

func restoreHybridWindow(cfg config.Config, pool *NodePool, gm map[string]int, hashes []string, edgeRatio float64, out io.Writer, stats *RestoreStats, fileStats *RestoreFileStats, fileNodeAgg map[string]*RestoreNodeStats, runNodeAgg map[string]*RestoreNodeStats, filePhases map[string]time.Duration) error {
	windowPrepStart := time.Now()
	unique := uniqueHashes(hashes)
	byEdge := make(map[int][]string)
	cloudHashes := make([]string, 0, len(unique))

	for _, h := range unique {
		if shouldFetchFromEdgeByRatio(h, edgeRatio) {
			nid, ok := gm[h]
			if ok {
				if _, edgeOK := findEndpointByNumber(cfg.EdgeNodes, nid); edgeOK {
					byEdge[nid] = append(byEdge[nid], h)
					continue
				}
			}
		}
		cloudHashes = append(cloudHashes, h)
	}
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseWindowPrep, time.Since(windowPrepStart))

	tasks := make([]experimentFetchTask, 0, len(byEdge)+1)
	for nid, list := range byEdge {
		endpoint, ok := findEndpointByNumber(cfg.EdgeNodes, nid)
		if !ok {
			cloudHashes = append(cloudHashes, list...)
			continue
		}
		tasks = append(tasks, experimentFetchTask{
			source:     "edge",
			nodeNumber: nid,
			endpoint:   endpoint,
			hashes:     list,
		})
	}
	if len(cloudHashes) > 0 {
		tasks = append(tasks, experimentFetchTask{
			source:     "cloud",
			nodeNumber: CloudNodeNumber,
			endpoint:   cfg.CloudNode,
			hashes:     cloudHashes,
		})
	}

	dataMap := make(map[string][]byte, len(unique))
	resCh := make(chan experimentFetchResult, len(tasks))
	var wg sync.WaitGroup
	fetchStart := time.Now()

	for _, task := range tasks {
		task := task
		wg.Add(1)
		go func() {
			defer wg.Done()
			resCh <- experimentFetchResult{
				task: task,
				res:  batchFetchResult(cfg, pool, task.endpoint, task.nodeNumber, task.hashes),
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	edgeBytes := int64(0)
	cloudBytes := int64(0)
	edgeRequested := 0
	cloudRequested := 0
	missing := make([]string, 0)

	for item := range resCh {
		mergeNodeStat(fileNodeAgg, item.res.nodeStat)
		mergeNodeStat(runNodeAgg, item.res.nodeStat)
		for h, b := range item.res.found {
			dataMap[h] = b
		}
		if item.task.source == "cloud" {
			cloudBytes += item.res.nodeStat.BytesReturned
			cloudRequested += item.res.nodeStat.ChunksRequested
		} else {
			edgeBytes += item.res.nodeStat.BytesReturned
			edgeRequested += item.res.nodeStat.ChunksRequested
		}
		if item.res.err != nil {
			return fmt.Errorf("%s fetch failed from %s: %w", item.task.source, item.task.endpoint.ID, item.res.err)
		}
		missing = append(missing, item.res.missing...)
	}

	recordHybridFetchPhases(stats, filePhases, time.Since(fetchStart), edgeBytes, cloudBytes, edgeRequested, cloudRequested)

	if len(missing) > 0 {
		sort.Strings(missing)
		missing = dedupSortedStrings(missing)
		return fmt.Errorf("hybrid restore missing chunks: %d", len(missing))
	}

	writeStart := time.Now()
	for _, h := range hashes {
		b, ok := dataMap[h]
		if !ok {
			return fmt.Errorf("missing chunk data after hybrid fetch: %s", h)
		}
		n, err := out.Write(b)
		if err != nil {
			return err
		}
		stats.TotalBytes += int64(n)
		stats.TotalChunks++
		fileStats.Bytes += int64(n)
		fileStats.Chunks++
	}
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseOutputWrite, time.Since(writeStart))
	return nil
}

func shouldFetchFromEdgeByRatio(chunkHash string, edgeRatio float64) bool {
	if edgeRatio <= 0 {
		return false
	}
	if edgeRatio >= 1 {
		return true
	}
	h := fnv.New64a()
	_, _ = io.WriteString(h, chunkHash)
	v := h.Sum64()
	threshold := uint64(edgeRatio * float64(^uint64(0)))
	return v <= threshold
}

func recordHybridFetchPhases(stats *RestoreStats, filePhases map[string]time.Duration, elapsed time.Duration, edgeBytes, cloudBytes int64, edgeRequested, cloudRequested int) {
	if stats == nil || elapsed <= 0 {
		return
	}

	totalWeight := edgeBytes + cloudBytes
	edgeWeight := edgeBytes
	cloudWeight := cloudBytes
	if totalWeight == 0 {
		totalWeight = int64(edgeRequested + cloudRequested)
		edgeWeight = int64(edgeRequested)
		cloudWeight = int64(cloudRequested)
	}
	if totalWeight == 0 {
		if edgeRequested > 0 {
			addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseEdgeFetch, elapsed)
			return
		}
		addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseCloudFetch, elapsed)
		return
	}

	edgeDur := time.Duration(int64(elapsed) * edgeWeight / totalWeight)
	cloudDur := elapsed - edgeDur
	if edgeWeight > 0 {
		addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseEdgeFetch, edgeDur)
	}
	if cloudWeight > 0 {
		addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseCloudFetch, cloudDur)
	}
}

func dedupSortedStrings(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := make([]string, 0, len(in))
	prev := ""
	for i, s := range in {
		if i == 0 || s != prev {
			out = append(out, s)
			prev = s
		}
	}
	return out
}

func RestoreExperimentUsage(w *bufio.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, "  manager -config manager.yaml restore-exp <edge-all|cloud-full|hybrid> <recipe_name_or_original_path> [output_path] [edge_ratio]")
	w.Flush()
}
