package manager

import (
	"context"
	"dedup-system/config"
	"dedup-system/rpc"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fullFileUploadChunkSize = 2 * 1024 * 1024
const fullFileRPCTimeout = 6 * time.Hour

var errCloudFullFileNotFound = errors.New("cloud full file not found")

func fullFileObjectNameCandidates(inputPath string, recipePath string) ([]string, error) {
	if strings.TrimSpace(inputPath) == "" && strings.TrimSpace(recipePath) == "" {
		return nil, fmt.Errorf("empty input and recipe path")
	}

	candidates := make([]string, 0, 3)
	seen := make(map[string]struct{}, 3)
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		candidates = append(candidates, name)
	}

	if strings.TrimSpace(inputPath) != "" {
		legacyRecipeName, err := generateLegacyRecipeName(inputPath)
		if err != nil {
			return nil, err
		}
		add(strings.TrimSuffix(legacyRecipeName, ".recipe"))

		hashRecipeName, err := GenerateRecipeName(inputPath)
		if err != nil {
			return nil, err
		}
		add(strings.TrimSuffix(hashRecipeName, ".recipe"))
	}

	if strings.TrimSpace(recipePath) != "" {
		base := filepath.Base(strings.TrimSpace(recipePath))
		add(strings.TrimSuffix(base, ".recipe"))
	}

	return candidates, nil
}

func fullFileObjectNameFromInputPath(inputPath string) (string, error) {
	legacyRecipeName, err := generateLegacyRecipeName(inputPath)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(legacyRecipeName, ".recipe"), nil
}

func uploadFullFileToCloud(cfg config.Config, pool *NodePool, inputPath string) (string, error) {
	if strings.TrimSpace(inputPath) == "" {
		return "", fmt.Errorf("empty input path")
	}
	name, err := fullFileObjectNameFromInputPath(inputPath)
	if err != nil {
		return "", err
	}
	client, err := pool.Get(cfg.CloudNode)
	if err != nil {
		return "", err
	}
	f, err := os.Open(inputPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), fullFileRPCTimeout)
	defer cancel()
	stream, err := client.UploadFullFile(ctx)
	if err != nil {
		return "", err
	}

	sentHeader := false
	buf := make([]byte, fullFileUploadChunkSize)
	for {
		nr, rerr := f.Read(buf)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = stream.CloseSend()
			return "", rerr
		}
		msg := &rpc.UploadFullFileChunk{Data: buf[:nr]}
		if !sentHeader {
			msg.Name = name
			sentHeader = true
		}
		if err := stream.Send(msg); err != nil {
			_ = stream.CloseSend()
			return "", err
		}
	}
	if !sentHeader {
		if err := stream.Send(&rpc.UploadFullFileChunk{Name: name}); err != nil {
			_ = stream.CloseSend()
			return "", err
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return "", err
	}
	return name, nil
}

func downloadFullFileFromCloud(cfg config.Config, pool *NodePool, objectName string, out io.Writer, logicalChunks int) (int64, RestoreNodeStats, error) {
	stat := RestoreNodeStats{
		NodeID:     cfg.CloudNode.ID,
		NodeNumber: CloudNodeNumber,
	}
	client, err := pool.Get(cfg.CloudNode)
	if err != nil {
		return 0, stat, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), fullFileRPCTimeout)
	defer cancel()

	start := time.Now()
	stream, err := client.DownloadFullFile(ctx, &rpc.DownloadFullFileRequest{Name: objectName})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return 0, stat, errCloudFullFileNotFound
		}
		return 0, stat, err
	}

	var total int64
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				return 0, stat, errCloudFullFileNotFound
			}
			return 0, stat, err
		}
		if msg == nil || len(msg.Data) == 0 {
			continue
		}
		nw, werr := out.Write(msg.Data)
		if werr != nil {
			return 0, stat, werr
		}
		total += int64(nw)
		stat.Batches++
	}

	elapsed := time.Since(start).Seconds()
	stat.ChunksRequested = logicalChunks
	stat.ChunksFound = logicalChunks
	stat.BytesReturned = total
	stat.RpcRoundTripSeconds = elapsed
	stat.TransferEstimateSeconds = elapsed
	return total, stat, nil
}

func downloadFullFileFromCloudAny(cfg config.Config, pool *NodePool, objectNames []string, out io.Writer, logicalChunks int) (string, int64, RestoreNodeStats, error) {
	if len(objectNames) == 0 {
		return "", 0, RestoreNodeStats{}, fmt.Errorf("empty full file object names")
	}

	var lastErr error
	for _, objectName := range objectNames {
		bytesFetched, stat, err := downloadFullFileFromCloud(cfg, pool, objectName, out, logicalChunks)
		if err == nil {
			return objectName, bytesFetched, stat, nil
		}
		if !errors.Is(err, errCloudFullFileNotFound) {
			return objectName, bytesFetched, stat, err
		}
		lastErr = err
	}

	if lastErr == nil {
		lastErr = errCloudFullFileNotFound
	}
	return objectNames[len(objectNames)-1], 0, RestoreNodeStats{}, lastErr
}

func RunCloudSync(cfg config.Config, inputPath string) error {
	if !cfg.DistributedEnabled {
		return fmt.Errorf("distributed_enabled must be true for manager")
	}
	if cfg.CloudNode.ID == "" || cfg.CloudNode.Addr == "" {
		return fmt.Errorf("cloud_node must be configured for cloud-sync")
	}
	if cfg.RecipePath == "" {
		cfg.RecipePath = "./recipes"
	}

	pool := NewNodePool(cfg)
	defer pool.Close()
	if err := pool.Warmup([]config.NodeEndpoint{cfg.CloudNode}); err != nil {
		return err
	}

	files, err := collectDedupInputFiles(inputPath, cfg.RecipePath)
	if err != nil {
		return err
	}
	fmt.Printf("[CloudSync] upload started: files=%d\n", len(files))
	nextProgressPct := 20

	for i, f := range files {
		name, err := uploadFullFileToCloud(cfg, pool, f.Path)
		if err != nil {
			return fmt.Errorf("cloud-sync failed for %s: %w", f.Path, err)
		}
		if i < 3 {
			fmt.Printf("[CloudSync] uploaded: %s -> %s\n", f.Path, name)
		}
		total := len(files)
		if total > 0 {
			pct := int(float64(i+1) * 100.0 / float64(total))
			if pct >= nextProgressPct || i+1 == total {
				if pct > 100 {
					pct = 100
				}
				fmt.Printf("[CloudSync] progress: %d%% (%d/%d files)\n", pct, i+1, total)
				for nextProgressPct <= pct {
					nextProgressPct += 20
				}
			}
		}
	}
	fmt.Printf("[CloudSync] upload completed.\n")
	return nil
}
