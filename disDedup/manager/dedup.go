package manager

import (
	"bufio"
	"context"
	"crypto/sha1"
	"dedup-system/chunker"
	"dedup-system/config"
	"dedup-system/rpc"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DedupStats struct {
	Files                      int
	TotalChunks                int
	TotalOriginalBytes         int64
	TotalUniqueBytes           int64
	TotalEdgePlacedUniqueBytes int64
	TotalCloudOnlyUniqueChunks int
	TotalCloudOnlyUniqueBytes  int64
	NetworkSentBytes           int64
	PhaseDurations             map[string]time.Duration
	FileStats                  []DedupFileStats
	FailedFiles                []TaskFileError
	Start                      time.Time
}

type pendingDedupChunk struct {
	hash   string
	data   []byte
	length int64
}

type dedupInputFile struct {
	Path string
	Size int64
}

type dedupFolderGroup struct {
	Name  string
	Files []dedupInputFile
}

func (s *DedupStats) report(cfg config.Config, placementEnabled bool, placementEntries int) {
	d := time.Since(s.Start)
	sec := d.Seconds()
	if sec <= 0 {
		sec = 0.001
	}
	mbOri := float64(s.TotalOriginalBytes) / 1024 / 1024
	mbUniq := float64(s.TotalUniqueBytes) / 1024 / 1024
	edgeUniqueBytes := s.TotalEdgePlacedUniqueBytes
	mbEdgePlaced := float64(edgeUniqueBytes) / 1024 / 1024
	mbNet := float64(s.NetworkSentBytes) / 1024 / 1024
	thr := mbOri / sec
	ratio := 1.0
	if s.TotalUniqueBytes > 0 {
		ratio = float64(s.TotalOriginalBytes) / float64(s.TotalUniqueBytes)
	} else if s.TotalOriginalBytes > 0 {
		ratio = 9999.0
	}
	edgeToDatasetRatio := 0.0
	if s.TotalOriginalBytes > 0 {
		edgeToDatasetRatio = float64(edgeUniqueBytes) / float64(s.TotalOriginalBytes)
	}
	placementMode := cfg.PlacementStrategy
	if placementMode == "" {
		placementMode = "round_robin"
	}
	if placementEnabled {
		placementMode = "placement_table_json"
	}

	fmt.Println("\n========================================================")
	fmt.Println("           分布式去重完成报告 (DISTRIBUTED DEDUP)       ")
	fmt.Println("========================================================")
	fmt.Printf(" 去重策略          : chunk_hash(global_meta+recipe)\n")
	fmt.Printf(" 放置策略          : %s", placementMode)
	if placementEnabled {
		fmt.Printf(" (entries=%d)", placementEntries)
	} else {
		fmt.Printf(" (strategy=%s)", placementMode)
	}
	fmt.Println()
	fmt.Printf(" 存储粒度          : %s (single_file=%v)\n", cfg.StorageGranularity, cfg.SingleFileMode)
	fmt.Printf(" 分块方式          : %s (chunk_size=%d)\n", cfg.ChunkingMethod, cfg.ChunkSize)
	fmt.Printf(" Edge 节点数       : %d\n", len(cfg.EdgeNodes))
	if cfg.CloudNode.ID != "" {
		fmt.Printf(" Cloud 节点        : %s (%s)\n", cfg.CloudNode.ID, cfg.CloudNode.Addr)
	}
	fmt.Printf(" 文件数量          : %d\n", s.Files)
	fmt.Printf(" 原始大小          : %.2f MB\n", mbOri)
	fmt.Printf(" 唯一块大小        : %.2f MB\n", mbUniq)
	fmt.Printf(" 放到边缘大小      : %.2f MB\n", mbEdgePlaced)
	fmt.Printf(" 边缘放置占比      : %.2f%% (edge_placed/original)\n", edgeToDatasetRatio*100.0)
	fmt.Printf(" 去重率            : %.2f : 1\n", ratio)
	fmt.Printf(" 网络发送          : %.2f MB\n", mbNet)
	fmt.Printf(" 耗时              : %.4f s\n", sec)
	fmt.Printf(" 处理吞吐量        : %.2f MB/s\n", thr)
	printPhaseBreakdown(buildPhaseBreakdown(sec, s.PhaseDurations, dedupPhaseOrder, DedupPhaseOther))
	fmt.Println("========================================================")
}

func RunDedupDistributed(cfg config.Config, inputPath string) error {
	runStart := time.Now()
	setupStart := runStart

	if !cfg.DistributedEnabled {
		return fmt.Errorf("distributed_enabled must be true for manager")
	}
	if cfg.RecipePath == "" {
		cfg.RecipePath = "./recipes"
	}
	if cfg.CloudChunkReplication && (cfg.CloudNode.ID == "" || cfg.CloudNode.Addr == "") {
		return fmt.Errorf("cloud_node must be configured when cloud_chunk_replication=true")
	}
	if err := os.MkdirAll(cfg.RecipePath, 0755); err != nil {
		return err
	}
	if cfg.GlobalMetaPath == "" {
		// backward compatibility
		cfg.GlobalMetaPath = cfg.MetaPath
	}

	gm := make(map[string]int)
	if cfg.LoadPreviousIndex {
		gmLoaded, _ := LoadGlobalMeta(cfg.GlobalMetaPath)
		gm = gmLoaded
	}

	pool := NewNodePool(cfg)
	defer pool.Close()
	if err := pool.Warmup(allStorageEndpoints(cfg, true)); err != nil {
		return err
	}
	placer := NewPlacer(cfg.PlacementStrategy, cfg.EdgeNodes)

	placementTable, placementEnabled, placementSource, err := LoadPlacementTableFromConfig(cfg.PlacementDir, cfg.PlacementJSON)
	if err != nil {
		return err
	}
	if placementEnabled {
		fmt.Printf(">>> Placement table enabled (source=%s, entries=%d)\n", placementSource, len(placementTable))
	}

	stats := &DedupStats{
		Start:          runStart,
		PhaseDurations: make(map[string]time.Duration),
		FileStats:      make([]DedupFileStats, 0),
		FailedFiles:    make([]TaskFileError, 0),
	}
	addPhase(stats.PhaseDurations, DedupPhaseSetup, time.Since(setupStart))

	processFile := func(in dedupInputFile) error {
		fpath := in.Path
		filePhaseStart := time.Now()
		defer func() {
			addPhase(stats.PhaseDurations, DedupPhaseFileProcessing, time.Since(filePhaseStart))
		}()

		fileStart := time.Now()
		fstat := DedupFileStats{
			FilePath: fpath,
		}

		f, err := os.Open(fpath)
		if err != nil {
			return err
		}
		defer f.Close()

		safe, err := GenerateRecipeName(fpath)
		if err != nil {
			return err
		}
		recipeFull := filepath.Join(cfg.RecipePath, safe)
		fstat.RecipePath = recipeFull
		rf, err := os.Create(recipeFull)
		if err != nil {
			return fmt.Errorf("create recipe: %w", err)
		}
		defer rf.Close()
		w := bufio.NewWriter(rf)

		ck := chunker.NewChunker(cfg.ChunkingMethod, f, cfg.ChunkSize)
		batchSize := cfg.DedupBatchSize
		if batchSize <= 0 {
			batchSize = 1
		}
		cloudChunkReplication := cfg.CloudChunkReplication
		pendingByNode := make(map[string][]pendingDedupChunk)
		pendingNodeEndpoint := make(map[string]config.NodeEndpoint)
		pendingHashes := make(map[string]struct{})

		markStored := func(p pendingDedupChunk, nodeNumber int, replicatedToCloud bool) {
			gm[p.hash] = nodeNumber
			delete(pendingHashes, p.hash)
			if nodeNumber == CloudNodeNumber {
				fstat.CloudOnlyUniqueChunks++
				fstat.CloudOnlyUniqueBytes += p.length
				stats.TotalCloudOnlyUniqueChunks++
				stats.TotalCloudOnlyUniqueBytes += p.length
			} else if nodeNumber > 0 {
				stats.TotalEdgePlacedUniqueBytes += p.length
			}

			fstat.NetworkSentBytes += p.length
			stats.NetworkSentBytes += p.length
			if replicatedToCloud {
				fstat.NetworkSentBytes += p.length
				stats.NetworkSentBytes += p.length
			}
		}

		flushNodePending := func(endpoint config.NodeEndpoint) error {
			key := endpointKey(endpoint)
			list := pendingByNode[key]
			if len(list) == 0 {
				return nil
			}
			delete(pendingByNode, key)
			delete(pendingNodeEndpoint, key)

			networkStart := time.Now()
			defer func() {
				addPhase(stats.PhaseDurations, DedupPhaseNetworkPut, time.Since(networkStart))
			}()

			replicatedToCloud := false
			if err := putBatchToNode(cfg, pool, endpoint, list); err != nil {
				// Strict placement-table mode: no extra fallback.
				if placementEnabled {
					return fmt.Errorf("batch put failed in strict placement mode: %w", err)
				}

				// Non-placement-table mode: retry each chunk with per-chunk edge failover.
				for _, p := range list {
					nodeNumber, err2 := placeWithoutCloudReplication(cfg, pool, placer, endpoint, p.hash, p.data)
					if err2 != nil {
						return err2
					}
					if cloudChunkReplication {
						if err3 := putToNode(cfg, pool, cfg.CloudNode, p.hash, p.data); err3 != nil {
							return fmt.Errorf("cloud replica put failed: %w", err3)
						}
						replicatedToCloud = true
					}
					markStored(p, nodeNumber, replicatedToCloud)
					replicatedToCloud = false
				}
				return nil
			}

			nodeNumber := endpointNumber(cfg.EdgeNodes, endpoint)
			if nodeNumber <= 0 {
				return fmt.Errorf("unable to map chosen edge endpoint to node number: id=%s addr=%s", endpoint.ID, endpoint.Addr)
			}
			if cloudChunkReplication {
				if err := putBatchToNode(cfg, pool, cfg.CloudNode, list); err != nil {
					return fmt.Errorf("cloud replica batch put failed: %w", err)
				}
				replicatedToCloud = true
			}
			for _, p := range list {
				markStored(p, nodeNumber, replicatedToCloud)
			}
			return nil
		}

		flushAllPending := func() error {
			for key, endpoint := range pendingNodeEndpoint {
				if err := flushNodePending(endpoint); err != nil {
					return err
				}
				// flushNodePending already deleted key; keep loop variable used.
				_ = key
			}
			return nil
		}

		for {
			readHashStart := time.Now()
			ch, err := ck.Next()
			if err == io.EOF {
				addPhase(stats.PhaseDurations, DedupPhaseChunkReadHash, time.Since(readHashStart))
				break
			}
			if err != nil {
				addPhase(stats.PhaseDurations, DedupPhaseChunkReadHash, time.Since(readHashStart))
				return err
			}
			sum := sha1.Sum(ch.Data)
			addPhase(stats.PhaseDurations, DedupPhaseChunkReadHash, time.Since(readHashStart))
			h := hex.EncodeToString(sum[:])
			fstat.Chunks++
			stats.TotalChunks++

			// recipe always records the logical sequence
			recipeWriteStart := time.Now()
			if _, err := w.WriteString(h + "\n"); err != nil {
				addPhase(stats.PhaseDurations, DedupPhaseRecipeWrite, time.Since(recipeWriteStart))
				return err
			}
			addPhase(stats.PhaseDurations, DedupPhaseRecipeWrite, time.Since(recipeWriteStart))

			l := int64(ch.Length)
			fstat.OriginalBytes += l
			stats.TotalOriginalBytes += l

			metaLookupStart := time.Now()
			if _, exists := gm[h]; exists {
				addPhase(stats.PhaseDurations, DedupPhaseMetaLookup, time.Since(metaLookupStart))
				continue
			}
			if _, exists := pendingHashes[h]; exists {
				addPhase(stats.PhaseDurations, DedupPhaseMetaLookup, time.Since(metaLookupStart))
				continue
			}
			addPhase(stats.PhaseDurations, DedupPhaseMetaLookup, time.Since(metaLookupStart))

			// Unique chunk is defined by first global_meta miss.
			// In strict placement-table mode, unmapped chunks are still marked in gm as
			// non-edge (CloudNodeNumber=0) so subsequent occurrences are treated as duplicates.
			fstat.UniqueBytes += l
			stats.TotalUniqueBytes += l

			// new unique chunk:
			// - if placement table is enabled: only write mapped-and-resolvable hashes;
			//   unmapped/unresolvable hashes are skipped (strict mode);
			// - if placement table is disabled, use strategy on edges.
			placementStart := time.Now()
			chosen := config.NodeEndpoint{}
			skipChunk := false
			if placementEnabled {
				if nid, ok := placementTable[h]; ok {
					nid = strings.TrimSpace(nid)
					if ep, ok2 := findEndpoint(cfg.EdgeNodes, nid); ok2 {
						chosen = ep
					} else if n, errN := strconv.Atoi(nid); errN == nil {
						if epByNum, ok3 := findEndpointByNumber(cfg.EdgeNodes, n); ok3 {
							chosen = epByNum
						}
					}
				}
				if chosen.ID == "" {
					skipChunk = true
				}
			} else {
				chosen = placer.Next()
			}
			if skipChunk {
				if cloudChunkReplication {
					cloudWriteStart := time.Now()
					if err := putToNode(cfg, pool, cfg.CloudNode, h, ch.Data); err != nil {
						addPhase(stats.PhaseDurations, DedupPhaseNetworkPut, time.Since(cloudWriteStart))
						return fmt.Errorf("cloud-only put failed for unmapped chunk: %w", err)
					}
					addPhase(stats.PhaseDurations, DedupPhaseNetworkPut, time.Since(cloudWriteStart))
					markStored(pendingDedupChunk{
						hash:   h,
						data:   nil,
						length: l,
					}, CloudNodeNumber, false)
				} else {
					// Record the hash as globally seen but not placed on any edge.
					// This keeps dedup accounting consistent with the single-node path.
					gm[h] = CloudNodeNumber
				}
				addPhase(stats.PhaseDurations, DedupPhasePlacement, time.Since(placementStart))
				continue
			}
			if chosen.ID == "" {
				addPhase(stats.PhaseDurations, DedupPhasePlacement, time.Since(placementStart))
				return fmt.Errorf("no edge placement target available")
			}
			addPhase(stats.PhaseDurations, DedupPhasePlacement, time.Since(placementStart))

			// Keep a copy because chunker buffers may be reused in subsequent iterations.
			dataCopy := make([]byte, len(ch.Data))
			copy(dataCopy, ch.Data)
			p := pendingDedupChunk{
				hash:   h,
				data:   dataCopy,
				length: l,
			}
			pendingHashes[h] = struct{}{}
			key := endpointKey(chosen)
			pendingByNode[key] = append(pendingByNode[key], p)
			pendingNodeEndpoint[key] = chosen

			if len(pendingByNode[key]) >= batchSize {
				if err := flushNodePending(chosen); err != nil {
					return err
				}
			}
		}

		if err := flushAllPending(); err != nil {
			return err
		}

		if err := w.Flush(); err != nil {
			return err
		}
		fileSec := safeSeconds(time.Since(fileStart))
		fstat.DurationSeconds = fileSec
		fstat.ThroughputMBps = throughputMBps(fstat.OriginalBytes, fileSec)
		fstat.DedupRatio = ratioFromBytes(fstat.OriginalBytes, fstat.UniqueBytes)
		stats.FileStats = append(stats.FileStats, fstat)
		stats.Files++
		return nil
	}

	inputInfo, err := os.Stat(inputPath)
	if err != nil {
		return err
	}
	if !inputInfo.IsDir() && shouldSkipDedupSource(inputPath, inputInfo, cfg.RecipePath) {
		fmt.Printf("[Dedup] skip source under recipe_path: %s\n", inputPath)
		return nil
	}

	files, err := collectDedupInputFiles(inputPath, cfg.RecipePath)
	if err != nil {
		return err
	}
	if !inputInfo.IsDir() {
		fmt.Printf("[Dedup] batch started: files=%d\n", len(files))
		nextProgressPct := 20
		totalFiles := len(files)
		for i, f := range files {
			if err := processFile(f); err != nil {
				return err
			}
			if totalFiles > 0 {
				pct := int(float64(i+1) * 100.0 / float64(totalFiles))
				if pct >= nextProgressPct || i+1 == totalFiles {
					if pct > 100 {
						pct = 100
					}
					fmt.Printf("[Dedup] progress: %d%% (%d/%d files)\n", pct, i+1, totalFiles)
					for nextProgressPct <= pct {
						nextProgressPct += 20
					}
				}
			}
		}
	} else {
		groups := groupFilesByTopFolder(inputPath, files)
		fmt.Printf("[Dedup] folder batch started: folders=%d files=%d\n", len(groups), len(files))
		nextProgressPct := 20
		totalFolders := len(groups)
		for gi, group := range groups {
			for _, f := range group.Files {
				if err := processFile(f); err != nil {
					stats.FailedFiles = append(stats.FailedFiles, TaskFileError{
						FilePath: f.Path,
						Error:    err.Error(),
					})
				}
			}
			if totalFolders > 0 {
				pct := int(float64(gi+1) * 100.0 / float64(totalFolders))
				if pct >= nextProgressPct || gi+1 == totalFolders {
					if pct > 100 {
						pct = 100
					}
					fmt.Printf("[Dedup] progress: %d%% (%d/%d folders)\n", pct, gi+1, totalFolders)
					for nextProgressPct <= pct {
						nextProgressPct += 20
					}
				}
			}
		}
	}

	// Flush all nodes so that container-mode data becomes durable.
	fmt.Printf("[Dedup] flushing storage nodes...\n")
	flushStart := time.Now()
	if err := FlushAll(cfg, pool); err != nil {
		addPhase(stats.PhaseDurations, DedupPhaseFlushNodes, time.Since(flushStart))
		return err
	}
	addPhase(stats.PhaseDurations, DedupPhaseFlushNodes, time.Since(flushStart))
	fmt.Printf("[Dedup] flush completed.\n")

	metaSaveStart := time.Now()
	if err := SaveGlobalMeta(cfg.GlobalMetaPath, gm); err != nil {
		addPhase(stats.PhaseDurations, DedupPhaseSaveGlobalMeta, time.Since(metaSaveStart))
		return err
	}
	addPhase(stats.PhaseDurations, DedupPhaseSaveGlobalMeta, time.Since(metaSaveStart))
	statsPath, err := PersistDedupStats(cfg, stats)
	if err != nil {
		return fmt.Errorf("persist dedup stats: %w", err)
	}
	fmt.Printf("[Dedup] stats saved: %s\n", statsPath)

	stats.report(cfg, placementEnabled, len(placementTable))
	return nil
}

func collectDedupInputFiles(inputPath, recipePath string) ([]dedupInputFile, error) {
	info, err := os.Stat(inputPath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		if shouldSkipDedupSource(inputPath, info, recipePath) {
			return nil, nil
		}
		return []dedupInputFile{{
			Path: inputPath,
			Size: info.Size(),
		}}, nil
	}

	files := make([]dedupInputFile, 0)
	walkErr := filepath.Walk(inputPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if shouldSkipDedupSource(path, info, recipePath) {
			return nil
		}
		files = append(files, dedupInputFile{
			Path: path,
			Size: info.Size(),
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files, nil
}

func groupFilesByTopFolder(root string, files []dedupInputFile) []dedupFolderGroup {
	groupMap := make(map[string][]dedupInputFile)
	for _, f := range files {
		key := dedupTopFolderName(root, f.Path)
		groupMap[key] = append(groupMap[key], f)
	}
	keys := make([]string, 0, len(groupMap))
	for k := range groupMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]dedupFolderGroup, 0, len(keys))
	for _, k := range keys {
		list := groupMap[k]
		sort.Slice(list, func(i, j int) bool {
			return list[i].Path < list[j].Path
		})
		out = append(out, dedupFolderGroup{
			Name:  k,
			Files: list,
		})
	}
	return out
}

func dedupTopFolderName(root, fullPath string) string {
	rel, err := filepath.Rel(root, fullPath)
	if err != nil {
		return "(unknown)"
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" || parts[0] == "." {
		return "(root)"
	}
	if len(parts) == 1 {
		return "(root)"
	}
	return parts[0]
}

func shouldSkipDedupSource(path string, info os.FileInfo, recipePath string) bool {
	if info != nil && info.IsDir() {
		return false
	}
	return isPathWithinDir(path, recipePath)
}

func isPathWithinDir(path string, dir string) bool {
	if strings.TrimSpace(dir) == "" {
		return false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absDir, absPath)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func placeWithoutCloudReplication(cfg config.Config, pool *NodePool, placer *Placer, first config.NodeEndpoint, hash string, data []byte) (int, error) {
	// choose a working edge node
	chosen := first
	if err := putToNode(cfg, pool, chosen, hash, data); err != nil {
		ok := false
		for i := 0; i < len(cfg.EdgeNodes); i++ {
			alt := placer.Next()
			if alt.ID == "" || alt.ID == chosen.ID {
				continue
			}
			if err2 := putToNode(cfg, pool, alt, hash, data); err2 == nil {
				chosen = alt
				ok = true
				break
			}
		}
		if !ok {
			return 0, fmt.Errorf("put to edge failed: %w", err)
		}
	}

	n := endpointNumber(cfg.EdgeNodes, chosen)
	if n <= 0 {
		return 0, fmt.Errorf("unable to map chosen edge endpoint to node number: id=%s addr=%s", chosen.ID, chosen.Addr)
	}
	return n, nil
}

func placeWithoutCloudReplicationSynthetic(cfg config.Config, pool *NodePool, placer *Placer, first config.NodeEndpoint, hash string, size int64) (int, error) {
	chosen := first
	if err := putSyntheticToNode(cfg, pool, chosen, hash, size); err != nil {
		ok := false
		for i := 0; i < len(cfg.EdgeNodes); i++ {
			alt := placer.Next()
			if alt.ID == "" || alt.ID == chosen.ID {
				continue
			}
			if err2 := putSyntheticToNode(cfg, pool, alt, hash, size); err2 == nil {
				chosen = alt
				ok = true
				break
			}
		}
		if !ok {
			return 0, fmt.Errorf("synthetic put to edge failed: %w", err)
		}
	}

	n := endpointNumber(cfg.EdgeNodes, chosen)
	if n <= 0 {
		return 0, fmt.Errorf("unable to map chosen edge endpoint to node number: id=%s addr=%s", chosen.ID, chosen.Addr)
	}
	return n, nil
}

func putToNode(cfg config.Config, pool *NodePool, node config.NodeEndpoint, hash string, data []byte) error {
	client, err := pool.Get(node)
	if err != nil {
		return err
	}
	to := time.Duration(cfg.RpcTimeoutMs) * time.Millisecond
	if to <= 0 {
		to = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	_, err = client.PutChunk(ctx, &rpc.PutChunkRequest{Hash: hash, Data: data})
	return err
}

func putSyntheticToNode(cfg config.Config, pool *NodePool, node config.NodeEndpoint, hash string, size int64) error {
	client, err := pool.Get(node)
	if err != nil {
		return err
	}
	to := time.Duration(cfg.RpcTimeoutMs) * time.Millisecond
	if to <= 0 {
		to = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	_, err = client.PutSyntheticChunk(ctx, &rpc.PutSyntheticChunkRequest{Hash: hash, Size: size})
	return err
}

func putBatchToNode(cfg config.Config, pool *NodePool, node config.NodeEndpoint, items []pendingDedupChunk) error {
	if len(items) == 0 {
		return nil
	}
	client, err := pool.Get(node)
	if err != nil {
		return err
	}
	to := time.Duration(cfg.RpcTimeoutMs) * time.Millisecond
	if to <= 0 {
		to = 3 * time.Second
	}
	reqChunks := make([]*rpc.PutChunkItem, 0, len(items))
	for _, item := range items {
		reqChunks = append(reqChunks, &rpc.PutChunkItem{
			Hash: item.hash,
			Data: item.data,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	_, err = client.BatchPut(ctx, &rpc.BatchPutRequest{Chunks: reqChunks})
	return err
}

func putBatchSyntheticToNode(cfg config.Config, pool *NodePool, node config.NodeEndpoint, items []pendingDedupChunk) error {
	if len(items) == 0 {
		return nil
	}
	client, err := pool.Get(node)
	if err != nil {
		return err
	}
	to := time.Duration(cfg.RpcTimeoutMs) * time.Millisecond
	if to <= 0 {
		to = 3 * time.Second
	}
	reqChunks := make([]*rpc.PutSyntheticChunkItem, 0, len(items))
	for _, item := range items {
		reqChunks = append(reqChunks, &rpc.PutSyntheticChunkItem{
			Hash: item.hash,
			Size: item.length,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()
	_, err = client.BatchPutSynthetic(ctx, &rpc.BatchPutSyntheticRequest{Chunks: reqChunks})
	return err
}

func FlushAll(cfg config.Config, pool *NodePool) error {
	var failed []string
	flushTimeout := time.Duration(cfg.RpcTimeoutMs) * time.Millisecond
	if flushTimeout <= 0 {
		flushTimeout = 3 * time.Second
	}
	// Flush may need to persist buffered containers to disk and can be much slower
	// than Put/Get RPCs on large datasets.
	if flushTimeout < 2*time.Minute {
		flushTimeout = 2 * time.Minute
	}

	isRetryableFlushErr := func(err error) bool {
		if err == nil {
			return false
		}
		if st, ok := status.FromError(err); ok {
			return st.Code() == codes.DeadlineExceeded || st.Code() == codes.Unavailable
		}
		msg := strings.ToLower(err.Error())
		return strings.Contains(msg, "deadlineexceeded") ||
			strings.Contains(msg, "context deadline exceeded") ||
			strings.Contains(msg, "unavailable") ||
			strings.Contains(msg, "eof")
	}

	flushOne := func(n config.NodeEndpoint) {
		client, err := pool.Get(n)
		if err != nil {
			failed = append(failed, fmt.Sprintf("%s(connect): %v", n.ID, err))
			return
		}

		const maxAttempts = 3
		var lastErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
			_, err := client.Flush(ctx, &rpc.FlushRequest{})
			cancel()
			if err == nil {
				return
			}
			lastErr = err
			if attempt == maxAttempts || !isRetryableFlushErr(err) {
				break
			}
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		failed = append(failed, fmt.Sprintf("%s(flush): %v", n.ID, lastErr))
	}

	for _, n := range cfg.EdgeNodes {
		flushOne(n)
	}
	if cfg.CloudNode.ID != "" {
		flushOne(cfg.CloudNode)
	}
	if len(failed) > 0 {
		return fmt.Errorf("flush failed on nodes: %s", strings.Join(failed, "; "))
	}
	return nil
}
