package manager

import (
	"bufio"
	"dedup-system/config"
	"dedup-system/syntheticdata"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type syntheticFileInfo struct {
	ChunkBytes     int                 `json:"chunk_bytes"`
	UniFingerprint []string            `json:"uni_fingerprint"`
	UniSize        []int64             `json:"uni_size"`
	Files          []syntheticFileItem `json:"files"`
}

type syntheticFileItem struct {
	Path       string  `json:"path"`
	ChunkIDs   []int   `json:"chunk_ids"`
	ChunkSizes []int64 `json:"chunk_sizes"`
}

type syntheticWriteMode string

const (
	syntheticWriteManagerBytes syntheticWriteMode = "synthetic_manager"
	syntheticWriteNodeGenerated syntheticWriteMode = "synthetic_edge"
)

func RunSyntheticDedupDistributed(cfg config.Config, fileInfoPath string, datasetPrefix string) error {
	return RunSyntheticDedupDistributedManager(cfg, fileInfoPath, datasetPrefix)
}

func RunSyntheticDedupDistributedManager(cfg config.Config, fileInfoPath string, datasetPrefix string) error {
	return runSyntheticDedupDistributed(cfg, fileInfoPath, datasetPrefix, syntheticWriteManagerBytes)
}

func RunSyntheticDedupDistributedEdge(cfg config.Config, fileInfoPath string, datasetPrefix string) error {
	return runSyntheticDedupDistributed(cfg, fileInfoPath, datasetPrefix, syntheticWriteNodeGenerated)
}

func runSyntheticDedupDistributed(cfg config.Config, fileInfoPath string, datasetPrefix string, writeMode syntheticWriteMode) error {
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
	if err := os.MkdirAll(cfg.RecipePath, 0o755); err != nil {
		return err
	}
	if cfg.GlobalMetaPath == "" {
		cfg.GlobalMetaPath = cfg.MetaPath
	}

	info, err := loadSyntheticFileInfo(fileInfoPath)
	if err != nil {
		return err
	}
	if len(info.Files) == 0 {
		return fmt.Errorf("fileinfo contains no files: %s", fileInfoPath)
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
		FileStats:      make([]DedupFileStats, 0, len(info.Files)),
		FailedFiles:    make([]TaskFileError, 0),
	}
	addPhase(stats.PhaseDurations, DedupPhaseSetup, time.Since(setupStart))

	batchSize := cfg.DedupBatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	processFile := func(file syntheticFileItem) error {
		filePhaseStart := time.Now()
		defer func() {
			addPhase(stats.PhaseDurations, DedupPhaseFileProcessing, time.Since(filePhaseStart))
		}()

		relPath := normalizeSyntheticRelativePath(file.Path)
		if relPath == "" {
			return fmt.Errorf("empty file path in fileinfo")
		}
		fullInputPath := filepath.Join(datasetPrefix, filepath.FromSlash(relPath))

		fileStart := time.Now()
		fstat := DedupFileStats{
			FilePath: fullInputPath,
		}

		safe, err := GenerateRecipeName(fullInputPath)
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

			networkSent := syntheticNetworkBytes(writeMode, p.hash, p.length)
			fstat.NetworkSentBytes += networkSent
			stats.NetworkSentBytes += networkSent
			if replicatedToCloud {
				fstat.NetworkSentBytes += networkSent
				stats.NetworkSentBytes += networkSent
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
			putBatch := putBatchToNode
			putOne := putToNode
			placeWithoutReplication := placeWithoutCloudReplication
			if writeMode == syntheticWriteNodeGenerated {
				putBatch = putBatchSyntheticToNode
			}
			if err := putBatch(cfg, pool, endpoint, list); err != nil {
				if placementEnabled {
					return fmt.Errorf("batch put failed in strict placement mode: %w", err)
				}

				for _, p := range list {
					var (
						nodeNumber int
						err2       error
					)
					if writeMode == syntheticWriteNodeGenerated {
						nodeNumber, err2 = placeWithoutCloudReplicationSynthetic(cfg, pool, placer, endpoint, p.hash, p.length)
					} else {
						nodeNumber, err2 = placeWithoutReplication(cfg, pool, placer, endpoint, p.hash, p.data)
					}
					if err2 != nil {
						return err2
					}
					if cfg.CloudChunkReplication {
						if writeMode == syntheticWriteNodeGenerated {
							if err3 := putSyntheticToNode(cfg, pool, cfg.CloudNode, p.hash, p.length); err3 != nil {
								return fmt.Errorf("cloud replica synthetic put failed: %w", err3)
							}
						} else if err3 := putOne(cfg, pool, cfg.CloudNode, p.hash, p.data); err3 != nil {
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
			if cfg.CloudChunkReplication {
				if writeMode == syntheticWriteNodeGenerated {
					if err := putBatchSyntheticToNode(cfg, pool, cfg.CloudNode, list); err != nil {
						return fmt.Errorf("cloud replica synthetic batch put failed: %w", err)
					}
				} else if err := putBatchToNode(cfg, pool, cfg.CloudNode, list); err != nil {
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
				_ = key
			}
			return nil
		}

		if len(file.ChunkSizes) > 0 && len(file.ChunkSizes) != len(file.ChunkIDs) {
			return fmt.Errorf("chunk_sizes length mismatch for %s: ids=%d sizes=%d", relPath, len(file.ChunkIDs), len(file.ChunkSizes))
		}

		for idx, chunkID := range file.ChunkIDs {
			metaLookupStart := time.Now()
			hash, uniqueLen, err := info.chunkMeta(chunkID)
			if err != nil {
				addPhase(stats.PhaseDurations, DedupPhaseMetaLookup, time.Since(metaLookupStart))
				return fmt.Errorf("%s chunk_id=%d: %w", relPath, chunkID, err)
			}
			logicalLen := uniqueLen
			if len(file.ChunkSizes) > 0 {
				logicalLen = file.ChunkSizes[idx]
			}
			if logicalLen <= 0 {
				logicalLen = uniqueLen
			}
			addPhase(stats.PhaseDurations, DedupPhaseChunkReadHash, time.Since(metaLookupStart))

			fstat.Chunks++
			stats.TotalChunks++

			recipeWriteStart := time.Now()
			if _, err := w.WriteString(hash + "\n"); err != nil {
				addPhase(stats.PhaseDurations, DedupPhaseRecipeWrite, time.Since(recipeWriteStart))
				return err
			}
			addPhase(stats.PhaseDurations, DedupPhaseRecipeWrite, time.Since(recipeWriteStart))

			fstat.OriginalBytes += logicalLen
			stats.TotalOriginalBytes += logicalLen

			metaCheckStart := time.Now()
			if _, exists := gm[hash]; exists {
				addPhase(stats.PhaseDurations, DedupPhaseMetaLookup, time.Since(metaCheckStart))
				continue
			}
			if _, exists := pendingHashes[hash]; exists {
				addPhase(stats.PhaseDurations, DedupPhaseMetaLookup, time.Since(metaCheckStart))
				continue
			}
			addPhase(stats.PhaseDurations, DedupPhaseMetaLookup, time.Since(metaCheckStart))

			fstat.UniqueBytes += uniqueLen
			stats.TotalUniqueBytes += uniqueLen

			placementStart := time.Now()
			chosen := config.NodeEndpoint{}
			skipChunk := false
			if placementEnabled {
				if nid, ok := placementTable[hash]; ok {
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
				if cfg.CloudChunkReplication {
					cloudWriteStart := time.Now()
					if writeMode == syntheticWriteNodeGenerated {
						if err := putSyntheticToNode(cfg, pool, cfg.CloudNode, hash, uniqueLen); err != nil {
							addPhase(stats.PhaseDurations, DedupPhaseNetworkPut, time.Since(cloudWriteStart))
							return fmt.Errorf("cloud-only synthetic put failed for unmapped chunk: %w", err)
						}
					} else {
						data := syntheticChunkBytes(hash, uniqueLen)
						if err := putToNode(cfg, pool, cfg.CloudNode, hash, data); err != nil {
							addPhase(stats.PhaseDurations, DedupPhaseNetworkPut, time.Since(cloudWriteStart))
							return fmt.Errorf("cloud-only put failed for unmapped chunk: %w", err)
						}
					}
					addPhase(stats.PhaseDurations, DedupPhaseNetworkPut, time.Since(cloudWriteStart))
					markStored(pendingDedupChunk{hash: hash, length: uniqueLen}, CloudNodeNumber, false)
				} else {
					gm[hash] = CloudNodeNumber
				}
				addPhase(stats.PhaseDurations, DedupPhasePlacement, time.Since(placementStart))
				continue
			}
			if chosen.ID == "" {
				addPhase(stats.PhaseDurations, DedupPhasePlacement, time.Since(placementStart))
				return fmt.Errorf("no edge placement target available")
			}
			addPhase(stats.PhaseDurations, DedupPhasePlacement, time.Since(placementStart))

			p := pendingDedupChunk{
				hash:   hash,
				length: uniqueLen,
			}
			if writeMode == syntheticWriteManagerBytes {
				p.data = syntheticChunkBytes(hash, uniqueLen)
			}
			pendingHashes[hash] = struct{}{}
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

	fmt.Printf("[DedupSynthetic] batch started: files=%d mode=%s\n", len(info.Files), writeMode)
	nextProgressPct := 20
	for i, file := range info.sortedFiles() {
		if err := processFile(file); err != nil {
			return err
		}
		pct := int(float64(i+1) * 100.0 / float64(len(info.Files)))
		if pct >= nextProgressPct || i+1 == len(info.Files) {
			if pct > 100 {
				pct = 100
			}
			fmt.Printf("[DedupSynthetic] progress: %d%% (%d/%d files)\n", pct, i+1, len(info.Files))
			for nextProgressPct <= pct {
				nextProgressPct += 20
			}
		}
	}

	fmt.Printf("[DedupSynthetic] flushing storage nodes...\n")
	flushStart := time.Now()
	if err := FlushAll(cfg, pool); err != nil {
		addPhase(stats.PhaseDurations, DedupPhaseFlushNodes, time.Since(flushStart))
		return err
	}
	addPhase(stats.PhaseDurations, DedupPhaseFlushNodes, time.Since(flushStart))
	fmt.Printf("[DedupSynthetic] flush completed.\n")

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
	fmt.Printf("[DedupSynthetic] stats saved: %s\n", statsPath)
	stats.report(cfg, placementEnabled, len(placementTable))
	return nil
}

func loadSyntheticFileInfo(path string) (*syntheticFileInfo, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var info syntheticFileInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("parse fileinfo json %s: %w", path, err)
	}
	if len(info.UniFingerprint) != len(info.UniSize) {
		return nil, fmt.Errorf("fileinfo unique arrays mismatch: uni_fingerprint=%d uni_size=%d", len(info.UniFingerprint), len(info.UniSize))
	}
	return &info, nil
}

func (info *syntheticFileInfo) chunkMeta(chunkID int) (string, int64, error) {
	if chunkID <= 0 || chunkID > len(info.UniFingerprint) {
		return "", 0, fmt.Errorf("invalid chunk id %d", chunkID)
	}
	idx := chunkID - 1
	hash := strings.TrimSpace(info.UniFingerprint[idx])
	if hash == "" {
		return "", 0, fmt.Errorf("empty hash for chunk id %d", chunkID)
	}
	size := info.UniSize[idx]
	if size <= 0 {
		return "", 0, fmt.Errorf("invalid unique size for chunk id %d", chunkID)
	}
	return hash, size, nil
}

func (info *syntheticFileInfo) sortedFiles() []syntheticFileItem {
	files := make([]syntheticFileItem, len(info.Files))
	copy(files, info.Files)
	sort.Slice(files, func(i, j int) bool {
		return normalizeSyntheticRelativePath(files[i].Path) < normalizeSyntheticRelativePath(files[j].Path)
	})
	return files
}

func normalizeSyntheticRelativePath(path string) string {
	trimmed := strings.TrimSpace(strings.ReplaceAll(path, "\\", "/"))
	trimmed = strings.TrimPrefix(trimmed, "/")
	if trimmed == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Clean(trimmed))
}

func syntheticChunkBytes(hash string, size int64) []byte {
	return syntheticdata.ChunkBytes(hash, size)
}

func syntheticNetworkBytes(writeMode syntheticWriteMode, hash string, size int64) int64 {
	if writeMode == syntheticWriteNodeGenerated {
		return int64(len(strings.TrimSpace(hash))) + 8
	}
	if size > 0 {
		return size
	}
	return 0
}
