package manager

import (
	"dedup-system/config"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type fileInfoRestoreEntry struct {
	item        syntheticFileItem
	logicalPath string
	relPath     string
}

type fileInfoRestoreIndex struct {
	info   *syntheticFileInfo
	prefix string
	files  map[string]fileInfoRestoreEntry
}

// RunRestoreFromFileInfoBatch restores request traces directly from fileInfo
// recipes, while still fetching edge chunks and cloud full files through RPC.
func RunRestoreFromFileInfoBatch(cfg config.Config, fileInfoPath string, requests []RestoreRequest, datasetPrefix string) error {
	if len(requests) == 0 {
		return fmt.Errorf("restore requests is empty")
	}
	if strings.TrimSpace(fileInfoPath) == "" {
		return fmt.Errorf("fileinfo json path is empty")
	}
	if strings.TrimSpace(datasetPrefix) == "" {
		datasetPrefix = cfg.SyntheticDatasetPrefix
	}

	runStart := time.Now()
	setupStart := runStart

	if !cfg.DistributedEnabled {
		return fmt.Errorf("distributed_enabled must be true for manager")
	}
	if cfg.CloudNode.ID == "" || cfg.CloudNode.Addr == "" {
		return fmt.Errorf("cloud_node must be configured for fileinfo restore")
	}
	if strings.TrimSpace(cfg.PlacementJSON) == "" {
		return fmt.Errorf("placement_json is required for fileinfo restore; pass -placement-json <placement.json>")
	}

	index, err := loadFileInfoRestoreIndex(fileInfoPath, datasetPrefix)
	if err != nil {
		return err
	}
	placement, enabled, source, err := LoadPlacementTableFromConfig("", cfg.PlacementJSON)
	if err != nil {
		return err
	}
	if !enabled {
		return fmt.Errorf("placement table is disabled: %s", cfg.PlacementJSON)
	}
	if len(placement) == 0 {
		fmt.Printf("[RestoreFileInfo] placement source=%s is empty; all requested files will use cloud full download\n", source)
	}

	pool := NewNodePool(cfg)
	defer pool.Close()
	if err := pool.Warmup(allStorageEndpoints(cfg, true)); err != nil {
		return err
	}

	stats := &RestoreStats{
		Start:          runStart,
		PhaseDurations: make(map[string]time.Duration),
		FileStats:      make([]RestoreFileStats, 0, len(requests)),
		NodeStats:      make([]RestoreNodeStats, 0),
	}
	addPhase(stats.PhaseDurations, RestorePhaseSetup, time.Since(setupStart))

	gm, needed, err := prepareFileInfoEdgeChunks(cfg, pool, index, placement, requests)
	if err != nil {
		return err
	}
	fmt.Printf("[RestoreFileInfo] placement source=%s requested_edge_chunks=%d\n", source, needed)

	runNodeAgg := make(map[string]*RestoreNodeStats)
	fmt.Printf("[RestoreFileInfo] batch started: files=%d\n", len(requests))
	nextProgressPct := 20
	for i, req := range requests {
		if strings.TrimSpace(req.InputNameOrPath) == "" {
			return fmt.Errorf("invalid restore request[%d]: empty input", i)
		}
		if !cfg.RestoreDiscardOutput && strings.TrimSpace(req.OutputPath) == "" {
			return fmt.Errorf("invalid restore request[%d]: empty output", i)
		}
		if err := restoreOneFileFromFileInfo(cfg, pool, index, gm, req, stats, runNodeAgg); err != nil {
			return err
		}
		pct := int(float64(i+1) * 100.0 / float64(len(requests)))
		if pct >= nextProgressPct || i+1 == len(requests) {
			if pct > 100 {
				pct = 100
			}
			fmt.Printf("[RestoreFileInfo] progress: %d%% (%d/%d files)\n", pct, i+1, len(requests))
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
	fmt.Printf("[RestoreFileInfo] stats saved: %s\n", statsPath)
	stats.report()
	return nil
}

func loadFileInfoRestoreIndex(fileInfoPath, datasetPrefix string) (*fileInfoRestoreIndex, error) {
	info, err := loadSyntheticFileInfo(fileInfoPath)
	if err != nil {
		return nil, err
	}
	idx := &fileInfoRestoreIndex{
		info:   info,
		prefix: normalizeLogicalPath(datasetPrefix),
		files:  make(map[string]fileInfoRestoreEntry, len(info.Files)*2),
	}
	for _, item := range info.Files {
		rel := normalizeSyntheticRelativePath(item.Path)
		if rel == "" {
			return nil, fmt.Errorf("empty file path in fileinfo")
		}
		logical := fileInfoLogicalPath(idx.prefix, rel)
		entry := fileInfoRestoreEntry{item: item, logicalPath: logical, relPath: rel}
		idx.files[normalizeLogicalPath(logical)] = entry
		idx.files[normalizeLogicalPath(rel)] = entry
	}
	return idx, nil
}

func (idx *fileInfoRestoreIndex) lookup(input string) (fileInfoRestoreEntry, bool) {
	key := normalizeLogicalPath(input)
	if entry, ok := idx.files[key]; ok {
		return entry, true
	}
	if idx.prefix != "" {
		prefix := strings.TrimSuffix(idx.prefix, "/") + "/"
		if strings.HasPrefix(key, prefix) {
			if entry, ok := idx.files[strings.TrimPrefix(key, prefix)]; ok {
				return entry, true
			}
		}
	}
	return fileInfoRestoreEntry{}, false
}

func fileInfoLogicalPath(prefix, rel string) string {
	rel = normalizeSyntheticRelativePath(rel)
	if strings.TrimSpace(prefix) == "" {
		return rel
	}
	return normalizeLogicalPath(filepath.ToSlash(filepath.Join(prefix, filepath.FromSlash(rel))))
}

func normalizeLogicalPath(path string) string {
	trimmed := strings.TrimSpace(strings.ReplaceAll(path, "\\", "/"))
	if trimmed == "" {
		return ""
	}
	cleaned := filepath.ToSlash(filepath.Clean(trimmed))
	if cleaned == "." {
		return ""
	}
	return cleaned
}

func fileInfoHashesAndSizes(info *syntheticFileInfo, item syntheticFileItem) ([]string, map[string]int64, int64, error) {
	if len(item.ChunkSizes) > 0 && len(item.ChunkSizes) != len(item.ChunkIDs) {
		return nil, nil, 0, fmt.Errorf("chunk_sizes length mismatch for %s: ids=%d sizes=%d", item.Path, len(item.ChunkIDs), len(item.ChunkSizes))
	}
	hashes := make([]string, 0, len(item.ChunkIDs))
	sizes := make(map[string]int64, len(item.ChunkIDs))
	var logicalBytes int64
	for i, chunkID := range item.ChunkIDs {
		hash, uniqueLen, err := info.chunkMeta(chunkID)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("%s chunk_id=%d: %w", item.Path, chunkID, err)
		}
		logicalLen := uniqueLen
		if len(item.ChunkSizes) > 0 {
			logicalLen = item.ChunkSizes[i]
		}
		if logicalLen <= 0 {
			logicalLen = uniqueLen
		}
		hashes = append(hashes, hash)
		if _, ok := sizes[hash]; !ok {
			sizes[hash] = uniqueLen
		}
		logicalBytes += logicalLen
	}
	return hashes, sizes, logicalBytes, nil
}

func prepareFileInfoEdgeChunks(cfg config.Config, pool *NodePool, idx *fileInfoRestoreIndex, placement map[string]string, requests []RestoreRequest) (map[string]int, int, error) {
	gm := make(map[string]int)
	byNode := make(map[string][]pendingDedupChunk)
	endpoints := make(map[string]config.NodeEndpoint)
	seen := make(map[string]struct{})

	for _, req := range requests {
		entry, ok := idx.lookup(req.InputNameOrPath)
		if !ok {
			return nil, 0, fmt.Errorf("fileinfo entry not found for request: %s", req.InputNameOrPath)
		}
		hashes, sizes, _, err := fileInfoHashesAndSizes(idx.info, entry.item)
		if err != nil {
			return nil, 0, err
		}
		for _, hash := range uniqueHashes(hashes) {
			if _, ok := seen[hash]; ok {
				continue
			}
			seen[hash] = struct{}{}
			endpoint, nodeNumber, ok := resolvePlacementEndpoint(cfg, placement, hash)
			if !ok {
				continue
			}
			gm[hash] = nodeNumber
			key := endpointKey(endpoint)
			byNode[key] = append(byNode[key], pendingDedupChunk{
				hash:   hash,
				length: sizes[hash],
			})
			endpoints[key] = endpoint
		}
	}

	keys := make([]string, 0, len(byNode))
	for key := range byNode {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	batchSize := cfg.DedupBatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	for _, key := range keys {
		items := byNode[key]
		for start := 0; start < len(items); start += batchSize {
			end := start + batchSize
			if end > len(items) {
				end = len(items)
			}
			if err := putBatchSyntheticToNode(cfg, pool, endpoints[key], items[start:end]); err != nil {
				return nil, 0, fmt.Errorf("prepare edge chunks failed for %s: %w", endpoints[key].ID, err)
			}
		}
	}
	if err := FlushAll(cfg, pool); err != nil {
		return nil, 0, err
	}
	return gm, len(seen), nil
}

func resolvePlacementEndpoint(cfg config.Config, placement map[string]string, hash string) (config.NodeEndpoint, int, bool) {
	nid, ok := placement[hash]
	if !ok {
		return config.NodeEndpoint{}, 0, false
	}
	nid = strings.TrimSpace(nid)
	if nid == "" {
		return config.NodeEndpoint{}, 0, false
	}
	if ep, ok := findEndpoint(cfg.EdgeNodes, nid); ok {
		n := endpointNumber(cfg.EdgeNodes, ep)
		return ep, n, n > 0
	}
	if n, err := strconv.Atoi(nid); err == nil {
		if ep, ok := findEndpointByNumber(cfg.EdgeNodes, n); ok {
			return ep, n, true
		}
	}
	return config.NodeEndpoint{}, 0, false
}

func restoreOneFileFromFileInfo(cfg config.Config, pool *NodePool, idx *fileInfoRestoreIndex, gm map[string]int, req RestoreRequest, stats *RestoreStats, runNodeAgg map[string]*RestoreNodeStats) error {
	entry, ok := idx.lookup(req.InputNameOrPath)
	if !ok {
		return fmt.Errorf("fileinfo entry not found for request: %s", req.InputNameOrPath)
	}

	if err := os.MkdirAll(filepath.Dir(req.OutputPath), 0755); err != nil {
		if !cfg.RestoreDiscardOutput {
			return err
		}
	}
	bufferedOut, closeOutput, err := newRestoreOutputWriter(cfg, req.OutputPath)
	if err != nil {
		return err
	}
	defer closeOutput()

	fileStart := time.Now()
	fileStats := RestoreFileStats{
		InputPath:  req.InputNameOrPath,
		RecipePath: "fileinfo://" + entry.logicalPath,
		OutputPath: req.OutputPath,
	}
	if cfg.RestoreDiscardOutput {
		fileStats.OutputPath = ""
	}
	fileNodeAgg := make(map[string]*RestoreNodeStats)
	filePhases := make(map[string]time.Duration)

	recipeScanStart := time.Now()
	hashes, _, _, err := fileInfoHashesAndSizes(idx.info, entry.item)
	if err != nil {
		return err
	}
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseRecipeScan, time.Since(recipeScanStart))

	policyCheckStart := time.Now()
	policy := decideRestoreSourcePolicy(cfg, gm, hashes)
	policy.Threshold = 1
	policy.CloudFullDownload = policy.NonEdgeChunks >= 1
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhasePolicyCheck, time.Since(policyCheckStart))
	fileStats.NonEdgeChunks = policy.NonEdgeChunks
	fileStats.CloudFullDownload = policy.CloudFullDownload
	fileStats.CloudFullThreshold = 1
	if policy.CloudFullDownload {
		stats.CloudFullDownloadFiles++
		fmt.Printf("[RestoreFileInfo] cloud full download: input=%s non_edge_chunks=%d\n", req.InputNameOrPath, policy.NonEdgeChunks)
	}

	if policy.CloudFullDownload {
		fullNames, err := fullFileObjectNameCandidates(entry.logicalPath, "")
		if err != nil {
			return err
		}
		cloudFetchStart := time.Now()
		fullName, bytesFetched, nodeStat, err := downloadFullFileFromCloudAny(cfg, pool, fullNames, bufferedOut, len(hashes))
		if err != nil {
			return fmt.Errorf("cloud full download failed for %s: %w", fullName, err)
		}
		mergeNodeStat(fileNodeAgg, nodeStat)
		mergeNodeStat(runNodeAgg, nodeStat)
		addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseCloudFetch, time.Since(cloudFetchStart))
		stats.TotalBytes += bytesFetched
		stats.TotalChunks += len(hashes)
		fileStats.Bytes += bytesFetched
		fileStats.Chunks += len(hashes)
	} else {
		windowSize := cfg.RestoreWindowSize
		if windowSize <= 0 {
			windowSize = 1024
		}
		for start := 0; start < len(hashes); start += windowSize {
			end := start + windowSize
			if end > len(hashes) {
				end = len(hashes)
			}
			if err := restoreWindow(cfg, pool, gm, hashes[start:end], bufferedOut, stats, &fileStats, fileNodeAgg, runNodeAgg, filePhases); err != nil {
				return err
			}
		}
	}

	flushStart := time.Now()
	if err := bufferedOut.Flush(); err != nil {
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
