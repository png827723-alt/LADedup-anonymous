package meta

// Global Index: SHA-1 (string) -> LocationID (string)
type DedupIndex map[string]string

// GlobalChunkIndex is maintained by the management node.
// It maps chunk SHA-1 (hex) -> storage node number.
// Node numbers:
//   - Edge nodes: 1..N (according to cfg.edge_nodes order)
//   - Cloud-only fallback: 0
type GlobalChunkIndex map[string]int

// Chunk: 纯粹的数据分块信息
type Chunk struct {
	Offset uint64 // Absolute offset in the stream
	Length uint32 // Chunk size in bytes
	Hash   uint64 // Gear fingerprint (Weak Hash) from FastCDC
	Data   []byte // Chunk data
}

type Recipe struct {
	OriginalSize int64    `json:"original_size"`
	ChunkHashes  []string `json:"chunk_hashes"`
}

type ContainerMetaEntry struct {
	Fingerprint [20]byte
	Length      uint32
	Offset      uint32
}

type ContainerHeader struct {
	ContainerID int64
	ChunkNumber uint32
	DataSize    uint32
}
