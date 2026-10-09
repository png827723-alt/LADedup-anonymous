#!/bin/bash

# =================================================================
#  Go Dedup System (Final Optimized Structure)
# =================================================================

PROJECT_NAME="dedup-system"

echo ">>> [1/7] Cleaning & Creating Project: $PROJECT_NAME"
rm -rf $PROJECT_NAME
mkdir -p $PROJECT_NAME
cd $PROJECT_NAME
mkdir -p config meta chunker store

if [ ! -f go.mod ]; then
    go mod init $PROJECT_NAME
    echo "    Go module initialized."
    echo "    Downloading fastcdc..."
    go get github.com/kalbasit/fastcdc
fi

# =================================================================
#  2. Config
# =================================================================
cat << 'EOF' > config/config.go
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	StorageGranularity string `json:"storage_granularity"` 
	ChunkingMethod    string `json:"chunking_method"`     
	ChunkSize         int    `json:"chunk_size"`          
	ContainerDataSize int    `json:"container_data_size"` 
	MetaReservedSize  int    `json:"meta_reserved_size"`  
	CacheSize         int    `json:"cache_size"`          
	PersistData       bool   `json:"persist_data"`
	LoadPreviousIndex bool   `json:"load_previous_index"`
	StoragePath       string `json:"storage_path"`
	MetaPath          string `json:"meta_path"`
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
	if err := json.Unmarshal(bytes, &cfg); err != nil {
		return cfg, fmt.Errorf("failed to parse json: %v", err)
	}
	return cfg, nil
}
EOF

cat << 'EOF' > config.json
{
  "storage_granularity": "container",
  "chunking_method": "fastcdc",
  "chunk_size": 16384,
  "container_data_size": 4194304,
  "meta_reserved_size": 32768,
  "cache_size": 5,
  "persist_data": true,
  "load_previous_index": true,
  "storage_path": "./storage_data",
  "meta_path": "./meta.json"
}
EOF

# =================================================================
#  3. Meta (Chunk 结构体已移除 String Hash)
# =================================================================
echo ">>> [3/7] Generating Meta..."
cat << 'EOF' > meta/types.go
package meta

// Global Index: SHA-1 (string) -> LocationID (string)
type DedupIndex map[string]string

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
EOF

# =================================================================
#  4. Chunker (只负责切分)
# =================================================================
echo ">>> [4/7] Generating Chunker..."
cat << 'EOF' > chunker/chunker.go
package chunker

import (
	"bufio"
	"dedup-system/meta"
	"io"

	"github.com/kalbasit/fastcdc"
)

type Chunker interface {
	Next() (*meta.Chunk, error)
}

func NewChunker(method string, r io.Reader, size int) Chunker {
	if method == "fastcdc" {
		// 适配开源库参数
		opts := []fastcdc.Option{
			fastcdc.WithMinSize(size / 4),
			fastcdc.WithTargetSize(size),
			fastcdc.WithMaxSize(size * 4),
		}
		impl, _ := fastcdc.NewChunker(r, opts...)
		return &FastCDCAdapter{impl: impl}
	}
	return &FixedChunker{
		reader:    bufio.NewReader(r),
		chunkSize: size,
		curOffset: 0,
	}
}

// --- Fixed Chunker ---
type FixedChunker struct {
	reader    *bufio.Reader
	chunkSize int
	curOffset uint64
}

func (c *FixedChunker) Next() (*meta.Chunk, error) {
	buf := make([]byte, c.chunkSize)
	n, err := io.ReadFull(c.reader, buf)
	if n > 0 {
		chunk := &meta.Chunk{
			Offset: c.curOffset,
			Length: uint32(n),
			Hash:   0, // 固定分块没有 Gear Hash
			Data:   buf[:n],
		}
		c.curOffset += uint64(n)
		return chunk, nil
	}
	if err == io.EOF { return nil, io.EOF }
	return nil, err
}

// --- FastCDC Adapter ---
type FastCDCAdapter struct {
	impl *fastcdc.Chunker
}

func (c *FastCDCAdapter) Next() (*meta.Chunk, error) {
	cdcChunk, err := c.impl.Next()
	if err != nil {
		return nil, err
	}

	// 直接封装 fastcdc 的返回值
	// 注意：如果 kalbasit/fastcdc 版本不暴露 weak hash，这里可以填 0
	// 假设我们只关心 Offset, Length, Data
	return &meta.Chunk{
		Offset: uint64(cdcChunk.Offset),
		Length: uint32(cdcChunk.Length),
		Hash:   0, // 这里填 0 即可，因为我们要的是 SHA-1 (在 Main 里算)
		Data:   cdcChunk.Data,
	}, nil
}
EOF

# =================================================================
#  5. Storage Engine (接收 SHA-1 参数)
# =================================================================
echo ">>> [5/7] Generating Storage Engine..."
mkdir -p store

cat << 'EOF' > store/store.go
package store

import (
	"dedup-system/config"
	"dedup-system/meta"
)

type StorageEngine interface {
	// Put 显式接收 dedupID (SHA-1)
	Put(chunk *meta.Chunk, dedupID string) (string, bool, error)
	Get(hash string, locationID string) ([]byte, error)
	Close() error
}

func NewStorageEngine(cfg config.Config, index meta.DedupIndex) StorageEngine {
	if cfg.StorageGranularity == "block" {
		return NewBlockStorage(cfg, index)
	}
	return NewContainerStorage(cfg, index)
}
EOF

cat << 'EOF' > store/block_store.go
package store

import (
	"dedup-system/config"
	"dedup-system/meta"
	"os"
	"path/filepath"
)

type BlockStorage struct {
	cfg   config.Config
	index meta.DedupIndex
}

func NewBlockStorage(cfg config.Config, index meta.DedupIndex) *BlockStorage {
	return &BlockStorage{cfg: cfg, index: index}
}

func (s *BlockStorage) Put(chunk *meta.Chunk, dedupID string) (string, bool, error) {
	if loc, ok := s.index[dedupID]; ok {
		return loc, true, nil
	}
	filename := dedupID
	path := filepath.Join(s.cfg.StoragePath, filename)
	if _, err := os.Stat(path); err == nil {
		s.index[dedupID] = filename
		return filename, true, nil
	}
	if s.cfg.PersistData {
		if err := os.WriteFile(path, chunk.Data, 0644); err != nil {
			return "", false, err
		}
	}
	s.index[dedupID] = filename
	return filename, false, nil
}

func (s *BlockStorage) Get(hash string, locationID string) ([]byte, error) {
	path := filepath.Join(s.cfg.StoragePath, locationID)
	return os.ReadFile(path)
}

func (s *BlockStorage) Close() error { return nil }
EOF

cat << 'EOF' > store/container_store.go
package store

import (
	"bytes"
	"container/list"
	"dedup-system/config"
	"dedup-system/meta"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	HeaderSize = 16 
	EntrySize  = 28 
)

type LoadedContainer struct {
	ContainerID string
	RawData     []byte
	LocalIndex  map[string]meta.ContainerMetaEntry 
}

type LRUCache struct {
	capacity int
	cache    map[string]*list.Element
	lruList  *list.List
	lock     sync.Mutex
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		lruList:  list.New(),
	}
}

func (c *LRUCache) Get(id string) (*LoadedContainer, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if elem, ok := c.cache[id]; ok {
		c.lruList.MoveToFront(elem)
		return elem.Value.(*LoadedContainer), true
	}
	return nil, false
}

func (c *LRUCache) Put(id string, item *LoadedContainer) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if elem, ok := c.cache[id]; ok {
		c.lruList.MoveToFront(elem)
		elem.Value = item
		return
	}
	if c.lruList.Len() >= c.capacity {
		back := c.lruList.Back()
		if back != nil {
			rm := back.Value.(*LoadedContainer)
			delete(c.cache, rm.ContainerID)
			c.lruList.Remove(back)
		}
	}
	elem := c.lruList.PushFront(item)
	c.cache[id] = elem
}

type ContainerStorage struct {
	cfg            config.Config
	index          meta.DedupIndex
	cache          *LRUCache
	currentID      int64
	currentBuffer  []byte
	currentEntries []meta.ContainerMetaEntry
}

func NewContainerStorage(cfg config.Config, index meta.DedupIndex) *ContainerStorage {
	return &ContainerStorage{
		cfg:            cfg,
		index:          index,
		cache:          NewLRUCache(cfg.CacheSize),
		currentID:      1, 
		currentBuffer:  make([]byte, 0, cfg.ContainerDataSize),
		currentEntries: make([]meta.ContainerMetaEntry, 0),
	}
}

func toFingerprint(hexHash string) [20]byte {
	bytes, _ := hex.DecodeString(hexHash)
	var fp [20]byte
	copy(fp[:], bytes) 
	return fp
}

func (s *ContainerStorage) isOverflow(newChunkSize int) bool {
	if len(s.currentBuffer) + newChunkSize > s.cfg.ContainerDataSize { return true }
	if HeaderSize + (len(s.currentEntries) * EntrySize) + EntrySize > s.cfg.MetaReservedSize { return true }
	return false
}

func (s *ContainerStorage) Put(chunk *meta.Chunk, dedupID string) (string, bool, error) {
	if cid, ok := s.index[dedupID]; ok { return cid, true, nil }
	
	if s.isOverflow(len(chunk.Data)) {
		if err := s.flushContainer(); err != nil { return "", false, err }
	}
	
	entry := meta.ContainerMetaEntry{
		Fingerprint: toFingerprint(dedupID), // 使用传入的 SHA-1
		Length:      uint32(len(chunk.Data)),
		Offset:      uint32(len(s.currentBuffer)),
	}
	s.currentBuffer = append(s.currentBuffer, chunk.Data...)
	s.currentEntries = append(s.currentEntries, entry)
	
	cidStr := fmt.Sprintf("container_%d", s.currentID)
	s.index[dedupID] = cidStr
	return cidStr, false, nil
}

func (s *ContainerStorage) flushContainer() error {
	if len(s.currentBuffer) == 0 { return nil }
	cidStr := fmt.Sprintf("container_%d", s.currentID)
	if s.cfg.PersistData {
		path := filepath.Join(s.cfg.StoragePath, cidStr)
		header := meta.ContainerHeader{
			ContainerID: s.currentID,
			ChunkNumber: uint32(len(s.currentEntries)),
			DataSize:    uint32(len(s.currentBuffer)),
		}
		metaBuf := new(bytes.Buffer)
		binary.Write(metaBuf, binary.BigEndian, header.ContainerID)
		binary.Write(metaBuf, binary.BigEndian, header.ChunkNumber)
		binary.Write(metaBuf, binary.BigEndian, header.DataSize)
		for _, e := range s.currentEntries {
			metaBuf.Write(e.Fingerprint[:])
			binary.Write(metaBuf, binary.BigEndian, e.Length)
			binary.Write(metaBuf, binary.BigEndian, e.Offset)
		}
		paddingSize := s.cfg.MetaReservedSize - metaBuf.Len()
		if paddingSize > 0 { metaBuf.Write(make([]byte, paddingSize)) }
		f, err := os.Create(path)
		if err != nil { return err }
		defer f.Close()
		f.Write(s.currentBuffer)
		f.Write(metaBuf.Bytes())
		localIdx := make(map[string]meta.ContainerMetaEntry)
		for _, e := range s.currentEntries {
			fpHex := hex.EncodeToString(e.Fingerprint[:])
			localIdx[fpHex] = e
		}
		dataCopy := make([]byte, len(s.currentBuffer))
		copy(dataCopy, s.currentBuffer)
		s.cache.Put(cidStr, &LoadedContainer{ContainerID: cidStr, RawData: dataCopy, LocalIndex: localIdx})
	}
	s.currentID++
	s.currentBuffer = make([]byte, 0, s.cfg.ContainerDataSize)
	s.currentEntries = make([]meta.ContainerMetaEntry, 0)
	return nil
}

func (s *ContainerStorage) Get(hash string, containerID string) ([]byte, error) {
	loaded, hit := s.cache.Get(containerID)
	if !hit {
		path := filepath.Join(s.cfg.StoragePath, containerID)
		f, err := os.Open(path)
		if err != nil { return nil, err }
		defer f.Close()
		info, _ := f.Stat()
		fileSize := info.Size()
		metaStart := fileSize - int64(s.cfg.MetaReservedSize)
		if metaStart < 0 { metaStart = 0 }
		rawData := make([]byte, metaStart)
		f.ReadAt(rawData, 0)
		metaBytes := make([]byte, s.cfg.MetaReservedSize)
		f.ReadAt(metaBytes, metaStart)
		r := bytes.NewReader(metaBytes)
		var id int64
		var num, dSize uint32
		binary.Read(r, binary.BigEndian, &id)
		binary.Read(r, binary.BigEndian, &num)
		binary.Read(r, binary.BigEndian, &dSize)
		localIdx := make(map[string]meta.ContainerMetaEntry)
		for i := 0; i < int(num); i++ {
			var e meta.ContainerMetaEntry
			io.ReadFull(r, e.Fingerprint[:])
			binary.Read(r, binary.BigEndian, &e.Length)
			binary.Read(r, binary.BigEndian, &e.Offset)
			fpHex := hex.EncodeToString(e.Fingerprint[:])
			localIdx[fpHex] = e
		}
		loaded = &LoadedContainer{ContainerID: containerID, RawData: rawData, LocalIndex: localIdx}
		s.cache.Put(containerID, loaded)
	}
	entry, ok := loaded.LocalIndex[hash]
	if !ok { return nil, fmt.Errorf("chunk missing in container local index") }
	if int(entry.Offset+entry.Length) > len(loaded.RawData) { return nil, fmt.Errorf("data offset out of bounds") }
	result := make([]byte, entry.Length)
	copy(result, loaded.RawData[entry.Offset : entry.Offset+entry.Length])
	return result, nil
}

func (s *ContainerStorage) Close() error { return s.flushContainer() }
EOF

# =================================================================
#  7. Main (Main Loop Calc SHA1)
# =================================================================
echo ">>> [7/7] Generating Main..."
cat << 'EOF' > main.go
package main

import (
	"crypto/sha1"
	"dedup-system/chunker"
	"dedup-system/config"
	"dedup-system/meta"
	"dedup-system/store"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type PerfStats struct {
	TotalOriginalSize int64
	TotalStoredSize   int64 
	FileCount         int
	StartTime         time.Time
}

func DedupOneFile(fpath string, cfg config.Config, engine store.StorageEngine, stats *PerfStats) error {
	f, err := os.Open(fpath)
	if err != nil { return err }
	defer f.Close()

	ck := chunker.NewChunker(cfg.ChunkingMethod, f, cfg.ChunkSize)
	recipe := meta.Recipe{ChunkHashes: []string{}}
	
	for {
		// 1. 获取基础分块 (含 Data, Offset, Length, WeakHash)
		chunk, err := ck.Next()
		if err == io.EOF { break }
		if err != nil { return err }

		// 2. [主程序计算强哈希] 
		// 这里是计算 SHA-1 的唯一位置
		sha1Sum := sha1.Sum(chunk.Data)
		sha1Str := hex.EncodeToString(sha1Sum[:])

		// 3. 统计 & Recipe
		chunkLen := int64(len(chunk.Data))
		recipe.OriginalSize += chunkLen
		stats.TotalOriginalSize += chunkLen
		recipe.ChunkHashes = append(recipe.ChunkHashes, sha1Str)

		// 4. 存入引擎 (传入数据块 + 强哈希ID)
		_, found, err := engine.Put(chunk, sha1Str)
		if err != nil { return err }
		if !found {
			stats.TotalStoredSize += chunkLen
		}
	}

	rBytes, _ := json.MarshalIndent(recipe, "", " ")
	return os.WriteFile(fpath+".recipe", rBytes, 0644)
}

func RunDedup(cfg config.Config, inputPath string) error {
	os.MkdirAll(cfg.StoragePath, 0755)

	index := make(meta.DedupIndex)
	if cfg.LoadPreviousIndex {
		if idxBytes, err := os.ReadFile(cfg.MetaPath); err == nil {
			json.Unmarshal(idxBytes, &index)
		}
	}

	engine := store.NewStorageEngine(cfg, index)
	defer engine.Close()

	stats := &PerfStats{StartTime: time.Now()}

	walkFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil { return err }
		if info.IsDir() { return nil }
		if strings.HasSuffix(path, ".recipe") || strings.Contains(path, cfg.StoragePath) {
			return nil
		}
		if err := DedupOneFile(path, cfg, engine, stats); err != nil {
			fmt.Printf("Error processing %s: %v\n", path, err)
		} else {
			stats.FileCount++
		}
		return nil
	}

	info, err := os.Stat(inputPath)
	if err != nil { return err }

	if info.IsDir() {
		filepath.Walk(inputPath, walkFunc)
	} else {
		DedupOneFile(inputPath, cfg, engine, stats)
		stats.FileCount = 1
	}

	idxBytes, _ := json.MarshalIndent(index, "", " ")
	os.WriteFile(cfg.MetaPath, idxBytes, 0644)

	duration := time.Since(stats.StartTime)
	seconds := duration.Seconds()
	mbOriginal := float64(stats.TotalOriginalSize) / 1024 / 1024
	mbStored := float64(stats.TotalStoredSize) / 1024 / 1024
	throughput := mbOriginal / seconds
	
	ratio := 1.0
	if stats.TotalStoredSize > 0 {
		ratio = float64(stats.TotalOriginalSize) / float64(stats.TotalStoredSize)
	} else if stats.TotalOriginalSize > 0 {
		ratio = 9999.0 
	}

	fmt.Println("\n========================================================")
	fmt.Println("               DEDUPLICATION REPORT                     ")
	fmt.Println("========================================================")
	fmt.Printf(" Engine            : %s\n", cfg.StorageGranularity)
	fmt.Printf(" Files Processed   : %d\n", stats.FileCount)
	fmt.Printf(" Original Data Size: %.2f MB\n", mbOriginal)
	fmt.Printf(" Physical New Data : %.2f MB\n", mbStored)
	fmt.Printf(" Deduplication Ratio: %.2f : 1\n", ratio)
	fmt.Printf(" Processing Time   : %.4f s\n", seconds)
	fmt.Printf(" Dedup Throughput  : %.2f MB/s\n", throughput)
	fmt.Println("========================================================")

	return nil
}

func Restore(cfg config.Config, recipePath string, outputPath string) error {
	iBytes, err := os.ReadFile(cfg.MetaPath)
	if err != nil { return fmt.Errorf("missing meta.json") }
	var index meta.DedupIndex
	json.Unmarshal(iBytes, &index)

	rBytes, err := os.ReadFile(recipePath)
	if err != nil { return fmt.Errorf("missing recipe") }
	var recipe meta.Recipe
	json.Unmarshal(rBytes, &recipe)

	engine := store.NewStorageEngine(cfg, index)
	defer engine.Close()

	outF, err := os.Create(outputPath)
	if err != nil { return err }
	defer outF.Close()

	start := time.Now()
	for _, hash := range recipe.ChunkHashes {
		locID, ok := index[hash]
		if !ok { return fmt.Errorf("chunk %s missing in index", hash) }
		data, err := engine.Get(hash, locID)
		if err != nil { return err }
		outF.Write(data)
	}

	duration := time.Since(start)
	seconds := duration.Seconds()
	mbRestored := float64(recipe.OriginalSize) / 1024 / 1024
	throughput := mbRestored / seconds

	fmt.Println("\n========================================================")
	fmt.Println("                  RESTORE REPORT                        ")
	fmt.Println("========================================================")
	fmt.Printf(" Restored File     : %s\n", outputPath)
	fmt.Printf(" Restore Throughput: %.2f MB/s\n", throughput)
	fmt.Println("========================================================")

	return nil
}

func main() {
	configFile := "config.json"
	if len(os.Args) > 1 { configFile = os.Args[1] }
	cfg, err := config.LoadConfigFromFile(configFile)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}

	inputDir := "./test_docs"
	os.MkdirAll(inputDir, 0755)
	
	testFile := filepath.Join(inputDir, "cdc_test.txt")
	f, _ := os.Create(testFile)
	payload := []byte("The FastCDC algorithm is content defined chunking... ")
	for i := 0; i < 50000; i++ {
		f.Write(payload)
		if i % 100 == 0 { f.WriteString("BREAK_POINT") }
	}
	f.Close()

	if err := RunDedup(cfg, inputDir); err != nil { panic(err) }
	if err := Restore(cfg, testFile+".recipe", "restored_cdc.txt"); err != nil { panic(err) }
}
EOF

echo "=========================================================="
echo " BUILD COMPLETE: dedup-system"
echo "=========================================================="