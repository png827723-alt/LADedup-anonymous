package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// ====== Common storage/dedup settings ======
	StorageGranularity string `yaml:"storage_granularity"`
	ChunkingMethod     string `yaml:"chunking_method"`
	ChunkSize          int    `yaml:"chunk_size"`

	ContainerDataSize int `yaml:"container_data_size"`
	MetaReservedSize  int `yaml:"meta_reserved_size"`

	// === 缓存设置 ===
	CacheSize      int `yaml:"cache_size"`       // 容器缓存数量
	BlockCacheSize int `yaml:"block_cache_size"` // 块缓存数量

	// === 存储模式设置 ===
	SingleFileMode bool `yaml:"single_file_mode"` // true: 单文件合并存储, false: 多文件独立存储

	PersistData bool `yaml:"persist_data"`
	// LoadPreviousIndex controls whether a storage node reloads its persisted local
	// index in block mode on startup, which is mainly useful for dedup continuation.
	// Restore does not require it in block mode because manager reads global_meta and
	// storage nodes fetch blocks directly by hash-derived path. Container mode still
	// reloads its local index automatically because restore needs chunk->container
	// resolution after restart.
	LoadPreviousIndex bool   `yaml:"load_previous_index"`
	StoragePath       string `yaml:"storage_path"`
	// MetaPath is used by storage nodes to persist their local index.
	MetaPath string `yaml:"meta_path"`
	// NodeStatsPath is used by storage nodes to persist runtime stage timing stats.
	NodeStatsPath string `yaml:"node_stats_path"`
	RecipePath    string `yaml:"recipe_path"`
	// StatsPath is manager-only. It stores dedup/restore JSON statistics.
	StatsPath string `yaml:"stats_path"`

	// ====== Distributed settings ======
	DistributedEnabled bool   `yaml:"distributed_enabled"`
	Role               string `yaml:"role"` // manager | edge | cloud
	NodeID             string `yaml:"node_id"`
	ListenAddr         string `yaml:"listen_addr"`

	// Manager-only: global metadata path.
	// Saved format is direct JSON object: {"<chunk_hash>": <node_number>, ...}
	GlobalMetaPath string `yaml:"global_meta_path"`

	PlacementStrategy string `yaml:"placement_strategy"` // round_robin | random
	// PlacementJSON is an optional JSON file path containing "chunk_hash -> edge_node_id"
	// mapping (or an object with top-level "hash2edge_node_id"). When set, manager
	// uses this file in strict placement-table mode.
	PlacementJSON string `yaml:"placement_json"`
	// PlacementDir is an optional directory that contains a user-provided mapping table
	// of "chunk_hash -> edge_node_id". If the directory does not exist, manager will
	// place chunks by PlacementStrategy on edge nodes.
	// If the directory exists and placement table mode is enabled:
	//   - mapped + resolvable hash -> mapped edge endpoint
	//   - unmapped/unresolvable hash -> skipped (no fallback)
	PlacementDir           string `yaml:"placement_dir"`
	CloudChunkReplication  bool   `yaml:"cloud_chunk_replication"`
	DedupMode              string `yaml:"dedup_mode"` // real | synthetic_edge | synthetic_manager | synthetic(alias synthetic_edge)
	SyntheticFileInfoJSON  string `yaml:"synthetic_fileinfo_json"`
	SyntheticDatasetPrefix string `yaml:"synthetic_dataset_prefix"`
	DedupBatchSize         int    `yaml:"dedup_batch_size"`    // default 128
	RestoreBatchSize       int    `yaml:"restore_batch_size"`  // default 128
	RestoreWindowSize      int    `yaml:"restore_window_size"` // number of recipe hashes per fetch window
	// If a file has at least this many chunks not placed on edge nodes,
	// manager will skip edge fetch and download the whole file from cloud.
	RestoreCloudFullThreshold int `yaml:"restore_cloud_full_threshold"` // default 1
	// When true, manager reconstructs restore data successfully but does not
	// materialize the result into an output file on disk.
	RestoreDiscardOutput bool `yaml:"restore_discard_output"`
	RpcTimeoutMs         int  `yaml:"rpc_timeout_ms"`
	RPCMaxMessageBytes   int  `yaml:"rpc_max_message_bytes"`

	EdgeNodes []NodeEndpoint `yaml:"edge_nodes"`
	CloudNode NodeEndpoint   `yaml:"cloud_node"`
}

type NodeEndpoint struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

func LoadConfigFromFile(path string) (Config, error) {
	var cfg Config
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return cfg, fmt.Errorf("config file not found: %s", path)
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("failed to read config: %v", err)
	}
	if err := yaml.Unmarshal(bytes, &cfg); err != nil {
		return cfg, fmt.Errorf("failed to parse yaml: %v", err)
	}
	return cfg, nil
}
