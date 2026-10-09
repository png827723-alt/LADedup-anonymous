package node

import (
	"context"
	"dedup-system/config"
	"dedup-system/meta"
	"dedup-system/rpc"
	"dedup-system/store"
	"dedup-system/syntheticdata"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fullFileSubDir = "_fullfiles"
const fullFileStreamChunkSize = 2 * 1024 * 1024
const containerRestoreStatsFileName = "container_restore_stats.json"

// StorageNode implements rpc.StorageNodeServer.
// It is used by both Edge Storage Node and Cloud Server.
type StorageNode struct {
	rpc.UnimplementedStorageNodeServer

	cfg    config.Config
	mu     sync.RWMutex
	index  meta.DedupIndex
	engine store.StorageEngine

	statsMu            sync.Mutex
	runtime            runtimeStats
	statsFile          string
	containerStatsFile string
}

func shouldLoadLocalIndex(cfg config.Config) bool {
	// Restore in block mode can fetch directly by hash-derived path, so reloading the
	// local index is not required there. Container mode still needs the persisted
	// local index to resolve chunk -> container mappings after restart.
	if strings.EqualFold(strings.TrimSpace(cfg.StorageGranularity), "container") {
		return true
	}
	return cfg.LoadPreviousIndex
}

func NewStorageNode(cfg config.Config) (*StorageNode, error) {
	if cfg.StoragePath == "" {
		return nil, fmt.Errorf("storage_path is required")
	}
	if err := os.MkdirAll(cfg.StoragePath, 0755); err != nil {
		return nil, err
	}

	idx := make(meta.DedupIndex)
	var eng store.StorageEngine
	statsFile := ""
	containerStatsFile := ""

	chunkStorageEnabled := strings.TrimSpace(cfg.MetaPath) != ""
	if !chunkStorageEnabled && !strings.EqualFold(strings.TrimSpace(cfg.Role), "cloud") {
		return nil, fmt.Errorf("meta_path is required for %s role", strings.TrimSpace(cfg.Role))
	}
	if chunkStorageEnabled {
		metaDir := filepath.Dir(cfg.MetaPath)
		if metaDir == "" {
			metaDir = "."
		}
		if err := os.MkdirAll(metaDir, 0755); err != nil {
			return nil, fmt.Errorf("create meta dir %s failed: %w", metaDir, err)
		}
		if shouldLoadLocalIndex(cfg) {
			if b, err := os.ReadFile(cfg.MetaPath); err == nil {
				_ = json.Unmarshal(b, &idx)
			}
		}
		eng = store.NewStorageEngine(cfg, idx)

		statsDir := strings.TrimSpace(cfg.NodeStatsPath)
		if statsDir == "" {
			base := filepath.Dir(cfg.StoragePath)
			if base == "" || base == "." {
				base = cfg.StoragePath
			}
			statsDir = filepath.Join(base, "stats")
		}
		statsFile = filepath.Join(statsDir, nodeStatsFileName)
		containerStatsFile = filepath.Join(statsDir, containerRestoreStatsFileName)
	}

	return &StorageNode{
		cfg:                cfg,
		index:              idx,
		engine:             eng,
		runtime:            newRuntimeStats(),
		statsFile:          statsFile,
		containerStatsFile: containerStatsFile,
	}, nil
}

func (n *StorageNode) hasChunkStorage() bool {
	return n.engine != nil
}

func (n *StorageNode) persistIndexLocked() error {
	if !n.hasChunkStorage() {
		return nil
	}
	if n.cfg.MetaPath == "" {
		return nil
	}
	metaDir := filepath.Dir(n.cfg.MetaPath)
	if metaDir == "" {
		metaDir = "."
	}
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return fmt.Errorf("create meta dir %s failed: %w", metaDir, err)
	}
	b, err := json.MarshalIndent(n.index, "", "  ")
	if err != nil {
		return err
	}
	// atomic-ish write
	tmp := n.cfg.MetaPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, n.cfg.MetaPath)
}

func (n *StorageNode) fullFileDir() string {
	return filepath.Join(n.cfg.StoragePath, fullFileSubDir)
}

func sanitizeFullFileName(name string) (string, error) {
	clean := strings.TrimSpace(name)
	if clean == "" {
		return "", fmt.Errorf("full file name is empty")
	}
	if strings.Contains(clean, "/") || strings.Contains(clean, "\\") {
		return "", fmt.Errorf("full file name must not contain path separators: %q", clean)
	}
	if clean == "." || clean == ".." {
		return "", fmt.Errorf("invalid full file name: %q", clean)
	}
	return clean, nil
}

func (n *StorageNode) UploadFullFile(stream rpc.StorageNode_UploadFullFileServer) error {
	var (
		name  string
		f     *os.File
		tmp   string
		final string
		total int64
	)
	cleanup := func() {
		if f != nil {
			_ = f.Close()
		}
		if tmp != "" {
			_ = os.Remove(tmp)
		}
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			cleanup()
			return err
		}
		if chunk == nil {
			continue
		}

		if name == "" {
			safe, err := sanitizeFullFileName(chunk.Name)
			if err != nil {
				cleanup()
				return err
			}
			name = safe
			if err := os.MkdirAll(n.fullFileDir(), 0755); err != nil {
				cleanup()
				return err
			}
			final = filepath.Join(n.fullFileDir(), name)
			tmp = fmt.Sprintf("%s.tmp.%d", final, time.Now().UnixNano())
			f, err = os.Create(tmp)
			if err != nil {
				cleanup()
				return err
			}
		} else if chunk.Name != "" && chunk.Name != name {
			cleanup()
			return fmt.Errorf("inconsistent full file name in stream: %q vs %q", name, chunk.Name)
		}

		if len(chunk.Data) == 0 {
			continue
		}
		nw, err := f.Write(chunk.Data)
		if err != nil {
			cleanup()
			return err
		}
		total += int64(nw)
	}

	if name == "" {
		return fmt.Errorf("empty upload stream")
	}
	if f != nil {
		if err := f.Close(); err != nil {
			cleanup()
			return err
		}
		f = nil
	}
	_ = os.Remove(final)
	if err := os.Rename(tmp, final); err != nil {
		cleanup()
		return err
	}
	tmp = ""
	return stream.SendAndClose(&rpc.UploadFullFileResponse{BytesReceived: total})
}

func (n *StorageNode) DownloadFullFile(req *rpc.DownloadFullFileRequest, stream rpc.StorageNode_DownloadFullFileServer) error {
	if req == nil {
		return fmt.Errorf("nil request")
	}
	name, err := sanitizeFullFileName(req.Name)
	if err != nil {
		return err
	}
	path := filepath.Join(n.fullFileDir(), name)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return status.Error(codes.NotFound, "full file not found")
		}
		return err
	}
	defer f.Close()

	buf := make([]byte, fullFileStreamChunkSize)
	for {
		nr, err := f.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if nr == 0 {
			continue
		}
		if err := stream.Send(&rpc.DownloadFullFileChunk{Data: buf[:nr]}); err != nil {
			return err
		}
	}
}

func (n *StorageNode) PutChunk(ctx context.Context, req *rpc.PutChunkRequest) (*rpc.PutChunkResponse, error) {
	if !n.hasChunkStorage() {
		return nil, status.Error(codes.Unimplemented, "chunk storage disabled on this node: PutChunk unavailable")
	}
	reqStart := time.Now()
	phases := make(map[string]time.Duration, 4)

	if req == nil || req.Hash == "" {
		return nil, fmt.Errorf("empty request")
	}
	if err := n.storeChunkDataLocked(req.Hash, req.Data, phases); err != nil {
		return nil, err
	}
	phases[dedupPhaseRequestTotal] = time.Since(reqStart)
	n.recordDedupStats(int64(len(req.Data)), phases[dedupPhaseRequestTotal], phases)

	return &rpc.PutChunkResponse{AlreadyExists: false}, nil
}

func (n *StorageNode) BatchPut(ctx context.Context, req *rpc.BatchPutRequest) (*rpc.BatchPutResponse, error) {
	if !n.hasChunkStorage() {
		return nil, status.Error(codes.Unimplemented, "chunk storage disabled on this node: BatchPut unavailable")
	}
	reqStart := time.Now()
	phases := make(map[string]time.Duration, 4)

	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	accepted := 0
	var bytesIn int64

	for _, item := range req.Chunks {
		if item == nil || item.Hash == "" {
			continue
		}
		if err := n.storeChunkData(item.Hash, item.Data, phases); err != nil {
			return nil, err
		}

		accepted++
		bytesIn += int64(len(item.Data))
	}

	phases[dedupPhaseRequestTotal] = time.Since(reqStart)
	n.recordDedupStats(bytesIn, phases[dedupPhaseRequestTotal], phases)
	return &rpc.BatchPutResponse{AcceptedChunks: int32(accepted)}, nil
}

func (n *StorageNode) PutSyntheticChunk(ctx context.Context, req *rpc.PutSyntheticChunkRequest) (*rpc.PutSyntheticChunkResponse, error) {
	if !n.hasChunkStorage() {
		return nil, status.Error(codes.Unimplemented, "chunk storage disabled on this node: PutSyntheticChunk unavailable")
	}
	reqStart := time.Now()
	phases := make(map[string]time.Duration, 4)

	if req == nil || req.Hash == "" || req.Size <= 0 {
		return nil, fmt.Errorf("invalid synthetic put request")
	}

	data := syntheticdata.ChunkBytes(req.Hash, req.Size)
	if err := n.storeChunkDataLocked(req.Hash, data, phases); err != nil {
		return nil, err
	}
	phases[dedupPhaseRequestTotal] = time.Since(reqStart)
	n.recordDedupStats(req.Size, phases[dedupPhaseRequestTotal], phases)
	return &rpc.PutSyntheticChunkResponse{AlreadyExists: false}, nil
}

func (n *StorageNode) BatchPutSynthetic(ctx context.Context, req *rpc.BatchPutSyntheticRequest) (*rpc.BatchPutSyntheticResponse, error) {
	if !n.hasChunkStorage() {
		return nil, status.Error(codes.Unimplemented, "chunk storage disabled on this node: BatchPutSynthetic unavailable")
	}
	reqStart := time.Now()
	phases := make(map[string]time.Duration, 4)

	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	accepted := 0
	var bytesIn int64
	for _, item := range req.Chunks {
		if item == nil || item.Hash == "" || item.Size <= 0 {
			continue
		}
		data := syntheticdata.ChunkBytes(item.Hash, item.Size)
		if err := n.storeChunkData(item.Hash, data, phases); err != nil {
			return nil, err
		}
		accepted++
		bytesIn += item.Size
	}

	phases[dedupPhaseRequestTotal] = time.Since(reqStart)
	n.recordDedupStats(bytesIn, phases[dedupPhaseRequestTotal], phases)
	return &rpc.BatchPutSyntheticResponse{AcceptedChunks: int32(accepted)}, nil
}

func (n *StorageNode) storeChunkDataLocked(hash string, data []byte, phases map[string]time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.storeChunkData(hash, data, phases)
}

func (n *StorageNode) storeChunkData(hash string, data []byte, phases map[string]time.Duration) error {
	chunkWrapStart := time.Now()
	chunk := &meta.Chunk{Length: uint32(len(data)), Data: data}
	phases[dedupPhaseChunkWrap] += time.Since(chunkWrapStart)

	putStart := time.Now()
	loc, _, err := n.engine.Put(chunk, hash)
	phases[dedupPhaseEnginePut] += time.Since(putStart)
	if err != nil {
		return err
	}

	indexStart := time.Now()
	if strings.ToLower(n.cfg.StorageGranularity) == "block" {
		n.index[hash] = "0"
	} else {
		n.index[hash] = loc
	}
	phases[dedupPhaseIndexUpdate] += time.Since(indexStart)
	return nil
}

func (n *StorageNode) BatchGet(ctx context.Context, req *rpc.BatchGetRequest) (*rpc.BatchGetResponse, error) {
	if !n.hasChunkStorage() {
		return nil, status.Error(codes.Unimplemented, "chunk storage disabled on this node: BatchGet unavailable")
	}
	reqStart := time.Now()
	phases := make(map[string]time.Duration, 4)
	var bytesOut int64
	reqChunks := 0
	foundChunks := 0
	missingChunks := 0

	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	resp := &rpc.BatchGetResponse{Chunks: make([]*rpc.ChunkData, 0, len(req.Hashes))}

	for _, h := range req.Hashes {
		if h == "" {
			continue
		}
		reqChunks++

		var data []byte
		var err error

		getStart := time.Now()
		if strings.ToLower(n.cfg.StorageGranularity) == "block" {
			// Block mode uses hash-derived filenames, so restore can read directly by hash
			// without relying on an in-memory local index rebuilt at startup.
			data, err = n.engine.Get(h, h)
		} else {
			idxLookupStart := time.Now()
			n.mu.RLock()
			loc, ok := n.index[h]
			n.mu.RUnlock()
			phases[restorePhaseIndexLookup] += time.Since(idxLookupStart)
			if !ok {
				respPackStart := time.Now()
				resp.Chunks = append(resp.Chunks, &rpc.ChunkData{Hash: h, Found: false, Error: "missing"})
				phases[restorePhaseResponsePack] += time.Since(respPackStart)
				missingChunks++
				continue
			}
			// container 模式：loc 是 containerID
			data, err = n.engine.Get(h, loc)
		}
		phases[restorePhaseEngineGet] += time.Since(getStart)
		if err != nil {
			respPackStart := time.Now()
			resp.Chunks = append(resp.Chunks, &rpc.ChunkData{Hash: h, Found: false, Error: err.Error()})
			phases[restorePhaseResponsePack] += time.Since(respPackStart)
			missingChunks++
			continue
		}
		respPackStart := time.Now()
		resp.Chunks = append(resp.Chunks, &rpc.ChunkData{Hash: h, Found: true, Data: data})
		phases[restorePhaseResponsePack] += time.Since(respPackStart)
		bytesOut += int64(len(data))
		foundChunks++
	}
	phases[restorePhaseRequestTotal] = time.Since(reqStart)
	resp.Metrics = &rpc.BatchGetMetrics{
		RequestChunks:        int32(reqChunks),
		FoundChunks:          int32(foundChunks),
		MissingChunks:        int32(missingChunks),
		BytesReturned:        bytesOut,
		IndexLookupSeconds:   phases[restorePhaseIndexLookup].Seconds(),
		EngineGetSeconds:     phases[restorePhaseEngineGet].Seconds(),
		ResponseBuildSeconds: phases[restorePhaseResponsePack].Seconds(),
		HandlerTotalSeconds:  phases[restorePhaseRequestTotal].Seconds(),
	}
	n.recordRestoreStats(bytesOut, phases[restorePhaseRequestTotal], phases)
	return resp, nil
}

func (n *StorageNode) Flush(ctx context.Context, _ *rpc.FlushRequest) (*rpc.FlushResponse, error) {
	if !n.hasChunkStorage() {
		return &rpc.FlushResponse{}, nil
	}
	flushStart := time.Now()
	phases := make(map[string]time.Duration, 3)

	n.mu.Lock()
	defer n.mu.Unlock()
	engineFlushStart := time.Now()
	if err := n.engine.Flush(); err != nil {
		phases[flushPhaseEngineFlush] += time.Since(engineFlushStart)
		return nil, err
	}
	phases[flushPhaseEngineFlush] += time.Since(engineFlushStart)
	// flush 时持久化 index.json
	indexPersistStart := time.Now()
	if err := n.persistIndexLocked(); err != nil {
		phases[flushPhaseIndexSave] += time.Since(indexPersistStart)
		return nil, err
	}
	phases[flushPhaseIndexSave] += time.Since(indexPersistStart)
	phases[flushPhaseTotal] = time.Since(flushStart)
	n.recordFlushStats(phases[flushPhaseTotal], phases)
	if err := n.persistNodeStats(); err != nil {
		return nil, err
	}
	return &rpc.FlushResponse{}, nil
}

func (n *StorageNode) Ping(ctx context.Context, _ *rpc.PingRequest) (*rpc.PingResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &rpc.PingResponse{NodeId: n.cfg.NodeID, Role: n.cfg.Role, StorageGranularity: n.cfg.StorageGranularity}, nil
}

func (n *StorageNode) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.hasChunkStorage() {
		return nil
	}
	_ = n.persistIndexLocked()
	_ = n.persistNodeStats()
	_ = n.persistContainerRestoreStats()
	return n.engine.Close()
}

type containerReadStatsProvider interface {
	SnapshotReadStats() store.ContainerReadStats
}

type ContainerRestoreStatsSnapshot struct {
	NodeID               string  `json:"node_id"`
	Role                 string  `json:"role"`
	StorageGranularity   string  `json:"storage_granularity"`
	GeneratedAt          string  `json:"generated_at"`
	CacheHits            int64   `json:"cache_hits"`
	CacheMisses          int64   `json:"cache_misses"`
	CacheHitRate         float64 `json:"cache_hit_rate"`
	UsefulBytes          int64   `json:"useful_bytes"`
	ContainerLoadedBytes int64   `json:"container_loaded_bytes"`
	MetaReadBytes        int64   `json:"meta_read_bytes"`
	TotalReadBytes       int64   `json:"total_read_bytes"`
	UsefulRatio          float64 `json:"useful_ratio"`
	ReadAmplification    float64 `json:"read_amplification"`
}

func (n *StorageNode) buildContainerRestoreSnapshot() (ContainerRestoreStatsSnapshot, bool) {
	provider, ok := n.engine.(containerReadStatsProvider)
	if !ok || provider == nil {
		return ContainerRestoreStatsSnapshot{}, false
	}
	stats := provider.SnapshotReadStats()
	totalReads := stats.ContainerLoadedBytes + stats.MetaReadBytes
	totalReq := stats.CacheHits + stats.CacheMisses
	hitRate := 0.0
	if totalReq > 0 {
		hitRate = float64(stats.CacheHits) / float64(totalReq)
	}
	usefulRatio := 0.0
	if stats.ContainerLoadedBytes > 0 {
		usefulRatio = float64(stats.UsefulBytes) / float64(stats.ContainerLoadedBytes)
	}
	amplification := 0.0
	if stats.UsefulBytes > 0 {
		amplification = float64(totalReads) / float64(stats.UsefulBytes)
	}
	return ContainerRestoreStatsSnapshot{
		NodeID:               n.cfg.NodeID,
		Role:                 n.cfg.Role,
		StorageGranularity:   n.cfg.StorageGranularity,
		GeneratedAt:          time.Now().Format(time.RFC3339),
		CacheHits:            stats.CacheHits,
		CacheMisses:          stats.CacheMisses,
		CacheHitRate:         hitRate,
		UsefulBytes:          stats.UsefulBytes,
		ContainerLoadedBytes: stats.ContainerLoadedBytes,
		MetaReadBytes:        stats.MetaReadBytes,
		TotalReadBytes:       totalReads,
		UsefulRatio:          usefulRatio,
		ReadAmplification:    amplification,
	}, true
}

func writeJSONAtomic(path string, v any) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
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

func (n *StorageNode) persistContainerRestoreStats() error {
	if strings.TrimSpace(n.containerStatsFile) == "" {
		return nil
	}
	snapshot, ok := n.buildContainerRestoreSnapshot()
	if !ok {
		return nil
	}
	return writeJSONAtomic(n.containerStatsFile, snapshot)
}

func (n *StorageNode) recordDedupStats(bytes int64, total time.Duration, phases map[string]time.Duration) {
	n.statsMu.Lock()
	defer n.statsMu.Unlock()
	n.runtime.Dedup.add(bytes, total, phases)
}

func (n *StorageNode) recordRestoreStats(bytes int64, total time.Duration, phases map[string]time.Duration) {
	n.statsMu.Lock()
	n.runtime.Restore.add(bytes, total, phases)
	n.statsMu.Unlock()
	_ = n.persistContainerRestoreStats()
}

func (n *StorageNode) recordFlushStats(total time.Duration, phasesTotal map[string]time.Duration) {
	n.statsMu.Lock()
	defer n.statsMu.Unlock()
	n.runtime.Flush.add(0, total, phasesTotal)
}

func (n *StorageNode) buildNodePerfSnapshotLocked() NodePerfSnapshot {
	uptime := safeSec(time.Since(n.runtime.StartedAt))
	return NodePerfSnapshot{
		NodeID:       n.cfg.NodeID,
		Role:         n.cfg.Role,
		GeneratedAt:  time.Now().Format(time.RFC3339),
		UptimeSecond: uptime,
		Dedup:        taskSnapshot(n.runtime.Dedup, dedupPhaseOrder, dedupPhaseOther),
		Restore:      taskSnapshot(n.runtime.Restore, restorePhaseOrder, restorePhaseOther),
		Flush:        taskSnapshot(n.runtime.Flush, flushPhaseOrder, flushPhaseOther),
	}
}

func (n *StorageNode) persistNodeStats() error {
	if strings.TrimSpace(n.statsFile) == "" {
		return nil
	}
	n.statsMu.Lock()
	snapshot := n.buildNodePerfSnapshotLocked()
	statsFile := n.statsFile
	n.statsMu.Unlock()
	return persistSnapshotAtomic(statsFile, snapshot)
}

// DefaultRPCSettings applies sensible defaults for distributed settings.
func DefaultRPCSettings(cfg *config.Config) {
	if cfg.DedupBatchSize <= 0 {
		cfg.DedupBatchSize = 128
	}
	if cfg.RestoreBatchSize <= 0 {
		cfg.RestoreBatchSize = 128
	}
	if cfg.RestoreWindowSize <= 0 {
		cfg.RestoreWindowSize = 4096
	}
	if cfg.RestoreCloudFullThreshold <= 0 {
		cfg.RestoreCloudFullThreshold = 1
	}
	if cfg.RpcTimeoutMs <= 0 {
		cfg.RpcTimeoutMs = 3000
	}
	if cfg.RPCMaxMessageBytes <= 0 {
		cfg.RPCMaxMessageBytes = 64 * 1024 * 1024
	}
	if cfg.PlacementStrategy == "" {
		cfg.PlacementStrategy = "round_robin"
	}
	if cfg.NodeID == "" {
		cfg.NodeID = fmt.Sprintf("node-%d", time.Now().UnixNano())
	}
}
