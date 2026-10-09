package store

import (
	"dedup-system/config"
	"dedup-system/meta"
	"fmt"
)

type StorageEngine interface {
	Put(chunk *meta.Chunk, dedupID string) (string, bool, error)
	Get(hash string, locationID string) ([]byte, error)
	// Flush persists any buffered data (e.g., a partially-filled container)
	// without closing the engine.
	Flush() error
	Close() error
}

func NewStorageEngine(cfg config.Config, index meta.DedupIndex) StorageEngine {
	if cfg.StorageGranularity == "block" {
		fmt.Println("[Store] Mode: BLOCK (Single File Mode not applicable)")
		return NewBlockStorage(cfg)
	}

	modeStr := "Multi-File"
	if cfg.SingleFileMode {
		modeStr = "Single-File (Fixed Size Padding)"
	}
	fmt.Printf("[Store] Mode: CONTAINER [%s]\n", modeStr)

	return NewContainerStorage(cfg, index)
}
