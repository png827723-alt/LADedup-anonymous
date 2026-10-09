package manager

import (
	"bufio"
	"context"
	"dedup-system/config"
	"dedup-system/rpc"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const restoreOutputBufferSize = 8 * 1024 * 1024

type RestoreStats struct {
	TotalChunks            int
	TotalBytes             int64
	CloudFullDownloadFiles int
	PhaseDurations         map[string]time.Duration
	FileStats              []RestoreFileStats
	NodeStats              []RestoreNodeStats
	Start                  time.Time
}

type RestoreRequest struct {
	InputNameOrPath string
	OutputPath      string
}

type restoreRunOptions struct {
	ForceCloudFull bool
}

func addPhaseBoth(totalPhases map[string]time.Duration, filePhases map[string]time.Duration, phase string, d time.Duration) {
	addPhase(totalPhases, phase, d)
	addPhase(filePhases, phase, d)
}

func (s *RestoreStats) report() {
	d := time.Since(s.Start)
	sec := d.Seconds()
	if sec <= 0 {
		sec = 0.001
	}
	mb := float64(s.TotalBytes) / 1024 / 1024
	thr := mb / sec

	fmt.Println("\n========================================================")
	fmt.Println("        分布式恢复完成报告 (DISTRIBUTED RESTORE)        ")
	fmt.Println("========================================================")
	fmt.Printf(" 文件数量          : %d\n", len(s.FileStats))
	fmt.Printf(" 云端全量恢复文件数: %d\n", s.CloudFullDownloadFiles)
	if len(s.FileStats) == 1 && strings.TrimSpace(s.FileStats[0].OutputPath) != "" {
		fmt.Printf(" 输出文件          : %s\n", s.FileStats[0].OutputPath)
	}
	fmt.Printf(" 恢复块数          : %d\n", s.TotalChunks)
	fmt.Printf(" 恢复总大小        : %.2f MB (%d bytes)\n", mb, s.TotalBytes)
	fmt.Printf(" 耗时              : %.4f s\n", sec)
	fmt.Printf(" 读取吞吐量        : %.2f MB/s\n", thr)
	printPhaseBreakdown(buildPhaseBreakdown(sec, s.PhaseDurations, restorePhaseOrder, RestorePhaseOther))
	fmt.Println("========================================================")
}

func RunRestoreDistributed(cfg config.Config, inputNameOrPath, outputPath string) error {
	return RunRestoreDistributedBatch(cfg, []RestoreRequest{
		{
			InputNameOrPath: inputNameOrPath,
			OutputPath:      outputPath,
		},
	})
}

func RunRestoreCloudOnly(cfg config.Config, inputNameOrPath, outputPath string) error {
	return runRestoreDistributedBatch(cfg, []RestoreRequest{
		{
			InputNameOrPath: inputNameOrPath,
			OutputPath:      outputPath,
		},
	}, restoreRunOptions{ForceCloudFull: true})
}

func RunRestoreCloudOnlyBatch(cfg config.Config, requests []RestoreRequest) error {
	return runRestoreDistributedBatch(cfg, requests, restoreRunOptions{ForceCloudFull: true})
}

func RunRestoreDistributedBatch(cfg config.Config, requests []RestoreRequest) error {
	return runRestoreDistributedBatch(cfg, requests, restoreRunOptions{})
}

func runRestoreDistributedBatch(cfg config.Config, requests []RestoreRequest, opts restoreRunOptions) error {
	if len(requests) == 0 {
		return fmt.Errorf("restore requests is empty")
	}

	runStart := time.Now()
	setupStart := runStart

	if !cfg.DistributedEnabled {
		return fmt.Errorf("distributed_enabled must be true for manager")
	}
	if cfg.GlobalMetaPath == "" {
		cfg.GlobalMetaPath = cfg.MetaPath
	}
	if cfg.RecipePath == "" {
		cfg.RecipePath = "./recipes"
	}

	var gm map[string]int
	if !opts.ForceCloudFull {
		gm, _ = LoadGlobalMeta(cfg.GlobalMetaPath)
	}
	if cfg.CloudNode.ID == "" {
		return fmt.Errorf("cloud_node must be configured for reliable restore")
	}

	pool := NewNodePool(cfg)
	defer pool.Close()
	warmupEndpoints := allStorageEndpoints(cfg, true)
	if opts.ForceCloudFull {
		warmupEndpoints = []config.NodeEndpoint{cfg.CloudNode}
	}
	if err := pool.Warmup(warmupEndpoints); err != nil {
		return err
	}

	stats := &RestoreStats{
		Start:          runStart,
		PhaseDurations: make(map[string]time.Duration),
		FileStats:      make([]RestoreFileStats, 0, len(requests)),
		NodeStats:      make([]RestoreNodeStats, 0),
	}
	addPhase(stats.PhaseDurations, RestorePhaseSetup, time.Since(setupStart))
	runNodeAgg := make(map[string]*RestoreNodeStats)
	fmt.Printf("[Restore] batch started: files=%d\n", len(requests))
	nextProgressPct := 20

	for i, req := range requests {
		if strings.TrimSpace(req.InputNameOrPath) == "" {
			return fmt.Errorf("invalid restore request[%d]: empty input", i)
		}
		if !cfg.RestoreDiscardOutput && strings.TrimSpace(req.OutputPath) == "" {
			return fmt.Errorf("invalid restore request[%d]: empty output", i)
		}
		if err := restoreOneFile(cfg, pool, gm, req, stats, runNodeAgg, opts); err != nil {
			return err
		}
		pct := int(float64(i+1) * 100.0 / float64(len(requests)))
		if pct >= nextProgressPct || i+1 == len(requests) {
			if pct > 100 {
				pct = 100
			}
			fmt.Printf("[Restore] progress: %d%% (%d/%d files)\n", pct, i+1, len(requests))
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
	fmt.Printf("[Restore] stats saved: %s\n", statsPath)

	stats.report()
	return nil
}

func restoreOneFile(cfg config.Config, pool *NodePool, gm map[string]int, req RestoreRequest, stats *RestoreStats, runNodeAgg map[string]*RestoreNodeStats, opts restoreRunOptions) error {
	recipePath, err := ResolveRecipePath(cfg.RecipePath, req.InputNameOrPath)
	if err != nil {
		return err
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
		RecipePath: recipePath,
		OutputPath: req.OutputPath,
	}
	if cfg.RestoreDiscardOutput {
		fileStats.OutputPath = ""
	}
	fileNodeAgg := make(map[string]*RestoreNodeStats)
	filePhases := make(map[string]time.Duration)

	recipeScanStart := time.Now()
	hashes, err := loadRecipeHashes(recipePath)
	if err != nil {
		return err
	}
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseRecipeScan, time.Since(recipeScanStart))

	policyCheckStart := time.Now()
	var policy restoreSourcePolicy
	if opts.ForceCloudFull {
		threshold := cfg.RestoreCloudFullThreshold
		if threshold <= 0 {
			threshold = 1
		}
		policy = restoreSourcePolicy{
			NonEdgeChunks:     len(hashes),
			CloudFullDownload: true,
			Threshold:         threshold,
		}
	} else {
		policy = decideRestoreSourcePolicy(cfg, gm, hashes)
	}
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhasePolicyCheck, time.Since(policyCheckStart))
	fileStats.NonEdgeChunks = policy.NonEdgeChunks
	fileStats.CloudFullDownload = policy.CloudFullDownload
	fileStats.CloudFullThreshold = policy.Threshold
	if policy.CloudFullDownload {
		stats.CloudFullDownloadFiles++
		if opts.ForceCloudFull {
			fmt.Printf("[Restore] cloud full download (forced): recipe=%s chunks=%d\n", recipePath, len(hashes))
		} else {
			fmt.Printf("[Restore] cloud full download: recipe=%s non_edge_chunks=%d threshold=%d\n", recipePath, policy.NonEdgeChunks, policy.Threshold)
		}
	}

	if policy.CloudFullDownload {
		fullNames, err := fullFileObjectNameCandidates(req.InputNameOrPath, recipePath)
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
			window := hashes[start:end]
			windowErr := restoreWindow(cfg, pool, gm, window, bufferedOut, stats, &fileStats, fileNodeAgg, runNodeAgg, filePhases)
			if windowErr != nil {
				return windowErr
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

func newRestoreOutputWriter(cfg config.Config, outputPath string) (*bufio.Writer, func() error, error) {
	if cfg.RestoreDiscardOutput {
		return bufio.NewWriterSize(io.Discard, restoreOutputBufferSize), func() error { return nil }, nil
	}
	if strings.TrimSpace(outputPath) == "" {
		return nil, nil, fmt.Errorf("restore output path is empty")
	}
	out, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, err
	}
	return bufio.NewWriterSize(out, restoreOutputBufferSize), out.Close, nil
}

func loadRecipeHashes(recipePath string) ([]string, error) {
	f, err := os.Open(recipePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	hashes := make([]string, 0, 1024)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		h := strings.TrimSpace(scanner.Text())
		if h == "" {
			continue
		}
		hashes = append(hashes, h)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return hashes, nil
}

type restoreSourcePolicy struct {
	NonEdgeChunks     int
	CloudFullDownload bool
	Threshold         int
}

func decideRestoreSourcePolicy(cfg config.Config, gm map[string]int, hashes []string) restoreSourcePolicy {
	threshold := cfg.RestoreCloudFullThreshold
	if threshold <= 0 {
		threshold = 1
	}
	nonEdgeChunks := 0
	for _, h := range hashes {
		nid, ok := gm[h]
		if !ok {
			nonEdgeChunks++
			continue
		}
		if _, ok := findEndpointByNumber(cfg.EdgeNodes, nid); !ok {
			nonEdgeChunks++
		}
	}
	return restoreSourcePolicy{
		NonEdgeChunks:     nonEdgeChunks,
		CloudFullDownload: nonEdgeChunks >= threshold,
		Threshold:         threshold,
	}
}

func uniqueHashes(hashes []string) []string {
	unique := make([]string, 0, len(hashes))
	seen := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		unique = append(unique, h)
	}
	return unique
}

func restoreWindow(cfg config.Config, pool *NodePool, gm map[string]int, hashes []string, out io.Writer, stats *RestoreStats, fileStats *RestoreFileStats, fileNodeAgg map[string]*RestoreNodeStats, runNodeAgg map[string]*RestoreNodeStats, filePhases map[string]time.Duration) error {
	windowPrepStart := time.Now()
	// Deduplicate within window for fewer RPCs, but keep order for output.
	unique := uniqueHashes(hashes)

	// group by node
	byNode := make(map[int][]string)
	missing := make([]string, 0)
	for _, h := range unique {
		nid, ok := gm[h]
		if !ok {
			missing = append(missing, h)
			continue
		}
		if _, ok := findEndpointByNumber(cfg.EdgeNodes, nid); !ok {
			missing = append(missing, h)
			continue
		}
		byNode[nid] = append(byNode[nid], h)
	}
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseWindowPrep, time.Since(windowPrepStart))

	dataMap := make(map[string][]byte, len(unique))

	// fetch from edge nodes concurrently (per-node parallel RPC)
	type nodeTask struct {
		nid      int
		endpoint config.NodeEndpoint
		hashes   []string
	}
	tasks := make([]nodeTask, 0, len(byNode))
	for nid, list := range byNode {
		if nid == CloudNodeNumber {
			missing = append(missing, list...)
			continue
		}
		endpoint, ok := findEndpointByNumber(cfg.EdgeNodes, nid)
		if !ok {
			// unknown edge in mapping => treat as missing.
			missing = append(missing, list...)
			continue
		}
		tasks = append(tasks, nodeTask{nid: nid, endpoint: endpoint, hashes: list})
	}

	edgeFetchStart := time.Now()
	var wg sync.WaitGroup
	resCh := make(chan fetchResult, len(tasks))
	for _, t := range tasks {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := batchFetchResult(cfg, pool, t.endpoint, t.nid, t.hashes)
			res.nid = t.nid
			resCh <- res
		}()
	}
	go func() {
		wg.Wait()
		close(resCh)
	}()

	for res := range resCh {
		mergeNodeStat(fileNodeAgg, res.nodeStat)
		mergeNodeStat(runNodeAgg, res.nodeStat)
		if res.err != nil {
			// edge node failure in chunk-restore path
			missing = append(missing, byNode[res.nid]...)
			continue
		}
		for h, b := range res.found {
			dataMap[h] = b
		}
		missing = append(missing, res.missing...)
	}
	addPhaseBoth(stats.PhaseDurations, filePhases, RestorePhaseEdgeFetch, time.Since(edgeFetchStart))

	if len(missing) > 0 {
		mseen := make(map[string]struct{}, len(missing))
		muniq := make([]string, 0, len(missing))
		for _, h := range missing {
			if _, ok := mseen[h]; ok {
				continue
			}
			mseen[h] = struct{}{}
			muniq = append(muniq, h)
		}
		return fmt.Errorf("edge chunk restore failed, missing chunks on edge: %d", len(muniq))
	}

	// write in original order
	writeStart := time.Now()
	for _, h := range hashes {
		b, ok := dataMap[h]
		if !ok {
			return fmt.Errorf("missing chunk data after fetch: %s", h)
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

func findEndpoint(list []config.NodeEndpoint, id string) (config.NodeEndpoint, bool) {
	for _, e := range list {
		if e.ID == id {
			return e, true
		}
	}
	return config.NodeEndpoint{}, false
}

type fetchResult struct {
	nid      int
	found    map[string][]byte
	missing  []string
	err      error
	nodeStat RestoreNodeStats
}

func batchFetchResult(cfg config.Config, pool *NodePool, endpoint config.NodeEndpoint, nodeNumber int, hashes []string) fetchResult {
	res := fetchResult{
		found: make(map[string][]byte, len(hashes)),
		nodeStat: RestoreNodeStats{
			NodeID:     endpoint.ID,
			NodeNumber: nodeNumber,
		},
	}
	client, err := pool.Get(endpoint)
	if err != nil {
		res.err = err
		return res
	}
	bs := cfg.RestoreBatchSize
	if bs <= 0 {
		bs = 128
	}
	to := time.Duration(cfg.RpcTimeoutMs) * time.Millisecond
	if to <= 0 {
		to = 3 * time.Second
	}
	for i := 0; i < len(hashes); i += bs {
		end := i + bs
		if end > len(hashes) {
			end = len(hashes)
		}
		batch := hashes[i:end]
		ctx, cancel := context.WithTimeout(context.Background(), to)
		rpcStart := time.Now()
		rsp, err := client.BatchGet(ctx, &rpc.BatchGetRequest{Hashes: batch})
		rpcSec := time.Since(rpcStart).Seconds()
		cancel()
		if err != nil {
			res.err = err
			return res
		}
		res.nodeStat.Batches++
		res.nodeStat.RpcRoundTripSeconds += rpcSec
		if rsp.Metrics != nil {
			res.nodeStat.ChunksRequested += int(rsp.Metrics.RequestChunks)
			res.nodeStat.ChunksFound += int(rsp.Metrics.FoundChunks)
			res.nodeStat.MissingChunks += int(rsp.Metrics.MissingChunks)
			res.nodeStat.BytesReturned += rsp.Metrics.BytesReturned
			res.nodeStat.ServerTotalSeconds += rsp.Metrics.HandlerTotalSeconds
			res.nodeStat.ExtractSeconds += rsp.Metrics.EngineGetSeconds
			transfer := rpcSec - rsp.Metrics.HandlerTotalSeconds
			if transfer < 0 {
				transfer = 0
			}
			res.nodeStat.TransferEstimateSeconds += transfer
		} else {
			res.nodeStat.ChunksRequested += len(batch)
			res.nodeStat.TransferEstimateSeconds += rpcSec
		}
		for _, c := range rsp.Chunks {
			if c == nil {
				continue
			}
			if c.Found {
				res.found[c.Hash] = c.Data
				if rsp.Metrics == nil {
					res.nodeStat.ChunksFound++
					res.nodeStat.BytesReturned += int64(len(c.Data))
				}
			} else {
				res.missing = append(res.missing, c.Hash)
				if rsp.Metrics == nil {
					res.nodeStat.MissingChunks++
				}
			}
		}
	}
	return res
}

func mergeNodeStat(dst map[string]*RestoreNodeStats, s RestoreNodeStats) {
	if dst == nil {
		return
	}
	if s.NodeID == "" && s.NodeNumber == 0 {
		return
	}
	key := fmt.Sprintf("%d|%s", s.NodeNumber, s.NodeID)
	cur, ok := dst[key]
	if !ok {
		cp := s
		dst[key] = &cp
		return
	}
	cur.Batches += s.Batches
	cur.ChunksRequested += s.ChunksRequested
	cur.ChunksFound += s.ChunksFound
	cur.MissingChunks += s.MissingChunks
	cur.BytesReturned += s.BytesReturned
	cur.RpcRoundTripSeconds += s.RpcRoundTripSeconds
	cur.ServerTotalSeconds += s.ServerTotalSeconds
	cur.TransferEstimateSeconds += s.TransferEstimateSeconds
	cur.ExtractSeconds += s.ExtractSeconds
}

func nodeStatsList(m map[string]*RestoreNodeStats) []RestoreNodeStats {
	out := make([]RestoreNodeStats, 0, len(m))
	for _, s := range m {
		v := *s
		v.RpcRoundTrip = RestoreTimingMetric{
			Seconds: v.RpcRoundTripSeconds,
		}
		v.ServerTotal = RestoreTimingMetric{
			Seconds: v.ServerTotalSeconds,
		}
		v.TransferEstimate = RestoreTimingMetric{
			Seconds: v.TransferEstimateSeconds,
		}
		v.Extract = RestoreExtractMetric{
			Seconds: v.ExtractSeconds,
		}

		if v.Batches > 0 {
			b := float64(v.Batches)
			v.RpcRoundTrip.AvgSeconds = v.RpcRoundTripSeconds / b
			v.ServerTotal.AvgSeconds = v.ServerTotalSeconds / b
			v.TransferEstimate.AvgSeconds = v.TransferEstimateSeconds / b
			v.Extract.AvgSeconds = v.ExtractSeconds / b
		}
		if v.RpcRoundTripSeconds > 0 {
			v.ServerTotal.Percentage = v.ServerTotalSeconds * 100.0 / v.RpcRoundTripSeconds
			v.TransferEstimate.Percentage = v.TransferEstimateSeconds * 100.0 / v.RpcRoundTripSeconds
			v.Extract.Percentage = v.ExtractSeconds * 100.0 / v.RpcRoundTripSeconds
		}
		if v.ServerTotalSeconds > 0 {
			v.Extract.InServerPercentage = v.ExtractSeconds * 100.0 / v.ServerTotalSeconds
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeNumber == out[j].NodeNumber {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].NodeNumber < out[j].NodeNumber
	})
	return out
}
