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
	"sync/atomic"
)

const (
	HeaderSize   = 16
	EntrySize    = 28
	MainFileName = "storage_bundle.data"
)

// --- LRU Cache ---
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

// --- Container Storage ---
type ContainerStorage struct {
	cfg            config.Config
	index          meta.DedupIndex
	cache          *LRUCache
	currentID      int64
	currentBuffer  []byte
	currentEntries []meta.ContainerMetaEntry
	mainFile       *os.File
	readStats      containerReadStatsCounter
}

type ContainerReadStats struct {
	CacheHits            int64 `json:"cache_hits"`
	CacheMisses          int64 `json:"cache_misses"`
	UsefulBytes          int64 `json:"useful_bytes"`
	ContainerLoadedBytes int64 `json:"container_loaded_bytes"`
	MetaReadBytes        int64 `json:"meta_read_bytes"`
}

type containerReadStatsCounter struct {
	cacheHits            atomic.Int64
	cacheMisses          atomic.Int64
	usefulBytes          atomic.Int64
	containerLoadedBytes atomic.Int64
	metaReadBytes        atomic.Int64
}

func NewContainerStorage(cfg config.Config, index meta.DedupIndex) *ContainerStorage {
	cs := &ContainerStorage{
		cfg:            cfg,
		index:          index,
		cache:          NewLRUCache(cfg.CacheSize),
		currentID:      1,
		currentBuffer:  make([]byte, 0, cfg.ContainerDataSize),
		currentEntries: make([]meta.ContainerMetaEntry, 0),
	}

	if cfg.SingleFileMode {
		// 打开或创建主文件
		// 注意：不要使用 O_APPEND，因为我们需要 Seek(End) 只是为了追加，
		// 但 Read 的时候需要 Seek 到中间。
		mainPath := filepath.Join(cfg.StoragePath, MainFileName)
		f, err := os.OpenFile(mainPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			fmt.Printf("Error opening main file: %v\n", err)
		}
		cs.mainFile = f

		// 通过文件总大小反推当前的 ContainerID
		// 每个容器在磁盘上的物理占用 = DataSize + MetaSize (严格固定)
		fixedTotalSize := int64(cfg.ContainerDataSize + cfg.MetaReservedSize)

		info, _ := f.Stat()
		if info.Size() > 0 {
			// 如果文件有内容，计算已有的容器数量
			// ID 从 1 开始，所以是 Count + 1
			cs.currentID = (info.Size() / fixedTotalSize) + 1

			// 移动指针到文件末尾，准备追加写入
			cs.mainFile.Seek(0, io.SeekEnd)
		}
	} else {
		// 多文件模式 (Demo 逻辑: 如果有索引，ID从1000开始，防止覆盖)
		if len(index) > 0 {
			cs.currentID = 1000
		}
	}

	return cs
}

func toFingerprint(hexHash string) [20]byte {
	bytes, _ := hex.DecodeString(hexHash)
	var fp [20]byte
	copy(fp[:], bytes)
	return fp
}

func (s *ContainerStorage) isOverflow(newChunkSize int) bool {
	if len(s.currentBuffer)+newChunkSize > s.cfg.ContainerDataSize {
		return true
	}
	if HeaderSize+(len(s.currentEntries)*EntrySize)+EntrySize > s.cfg.MetaReservedSize {
		return true
	}
	return false
}

func (s *ContainerStorage) Put(chunk *meta.Chunk, dedupID string) (string, bool, error) {
	if cid, ok := s.index[dedupID]; ok {
		return cid, true, nil
	}

	if s.isOverflow(len(chunk.Data)) {
		if err := s.flushContainer(); err != nil {
			return "", false, err
		}
	}

	entry := meta.ContainerMetaEntry{
		Fingerprint: toFingerprint(dedupID),
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
	if len(s.currentBuffer) == 0 {
		return nil
	}

	cidStr := fmt.Sprintf("container_%d", s.currentID)

	if s.cfg.PersistData {
		// 1. 构建元数据区 (内存构建)
		header := meta.ContainerHeader{
			ContainerID: s.currentID,
			ChunkNumber: uint32(len(s.currentEntries)),
			DataSize:    uint32(len(s.currentBuffer)), // 记录真实数据大小
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
		// 填充元数据区
		paddingMeta := s.cfg.MetaReservedSize - metaBuf.Len()
		if paddingMeta > 0 {
			metaBuf.Write(make([]byte, paddingMeta))
		}

		// 2. 写入磁盘
		if s.cfg.SingleFileMode {
			if s.mainFile == nil {
				return fmt.Errorf("main file not open")
			}

			// A. 写入真实数据
			if _, err := s.mainFile.Write(s.currentBuffer); err != nil {
				return err
			}

			// B. [核心] 零填充数据区 (Zero Padding)
			// 确保写入的字节数严格等于 cfg.ContainerDataSize
			paddingLen := s.cfg.ContainerDataSize - len(s.currentBuffer)
			if paddingLen > 0 {
				zeros := make([]byte, paddingLen)
				if _, err := s.mainFile.Write(zeros); err != nil {
					return err
				}
			}

			// C. 写入固定大小的元数据区
			if _, err := s.mainFile.Write(metaBuf.Bytes()); err != nil {
				return err
			}

		} else {
			// 多文件模式：直接追加，不强制填充 (节省空间)
			fullData := append(s.currentBuffer, metaBuf.Bytes()...)
			path := filepath.Join(s.cfg.StoragePath, cidStr)
			if err := os.MkdirAll(s.cfg.StoragePath, 0755); err != nil {
				return err
			}
			if err := os.WriteFile(path, fullData, 0644); err != nil {
				return err
			}
		}

		// 3. 更新缓存
		localIdx := make(map[string]meta.ContainerMetaEntry)
		for _, e := range s.currentEntries {
			fpHex := hex.EncodeToString(e.Fingerprint[:])
			localIdx[fpHex] = e
		}
		dataCopy := make([]byte, len(s.currentBuffer))
		copy(dataCopy, s.currentBuffer)

		s.cache.Put(cidStr, &LoadedContainer{
			ContainerID: cidStr,
			RawData:     dataCopy,
			LocalIndex:  localIdx,
		})
	}

	s.currentID++
	s.currentBuffer = make([]byte, 0, s.cfg.ContainerDataSize)
	s.currentEntries = make([]meta.ContainerMetaEntry, 0)

	return nil
}

func (s *ContainerStorage) Get(hash string, containerID string) ([]byte, error) {
	loaded, hit := s.cache.Get(containerID)
	if hit {
		s.readStats.cacheHits.Add(1)
	}
	if !hit {
		s.readStats.cacheMisses.Add(1)
		var rawData []byte
		var metaBytes []byte
		// var err error

		if s.cfg.SingleFileMode {
			// [IO 优化读取]
			// 1. 解析 ID
			var id int64
			_, err := fmt.Sscanf(containerID, "container_%d", &id)
			if err != nil {
				return nil, fmt.Errorf("invalid container id: %s", containerID)
			}

			if s.mainFile == nil {
				return nil, fmt.Errorf("main file closed")
			}

			// 2. 计算物理偏移
			fixedContainerSize := int64(s.cfg.ContainerDataSize + s.cfg.MetaReservedSize)
			containerStartOffset := (id - 1) * fixedContainerSize

			// 3. 先读元数据 (位于容器末尾)
			metaOffset := containerStartOffset + int64(s.cfg.ContainerDataSize)
			metaBytes = make([]byte, s.cfg.MetaReservedSize)

			if _, err := s.mainFile.ReadAt(metaBytes, metaOffset); err != nil {
				return nil, fmt.Errorf("read meta failed (ID=%d): %v", id, err)
			}

			// 4. 解析元数据头部，获取真实 DataSize
			// Header 结构: ID(8) + Num(4) + DataSize(4)
			// DataSize 位于偏移量 12 (8+4)
			var realDataSize uint32
			rTemp := bytes.NewReader(metaBytes)
			rTemp.Seek(12, io.SeekStart)
			binary.Read(rTemp, binary.BigEndian, &realDataSize)

			// 5. 只读取有效数据区 (跳过零填充)
			rawData = make([]byte, realDataSize)
			if _, err := s.mainFile.ReadAt(rawData, containerStartOffset); err != nil {
				return nil, fmt.Errorf("read data failed: %v", err)
			}

		} else {
			// 多文件模式
			path := filepath.Join(s.cfg.StoragePath, containerID)
			f, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			defer f.Close()

			info, _ := f.Stat()
			fileSize := info.Size()

			metaStart := fileSize - int64(s.cfg.MetaReservedSize)
			if metaStart < 0 {
				metaStart = 0
			}

			rawData = make([]byte, metaStart)
			f.ReadAt(rawData, 0)

			metaBytes = make([]byte, s.cfg.MetaReservedSize)
			f.ReadAt(metaBytes, metaStart)
		}
		s.readStats.containerLoadedBytes.Add(int64(len(rawData)))
		s.readStats.metaReadBytes.Add(int64(len(metaBytes)))

		// 构建缓存对象
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

		loaded = &LoadedContainer{
			ContainerID: containerID,
			RawData:     rawData,
			LocalIndex:  localIdx,
		}
		s.cache.Put(containerID, loaded)
	}

	// 从 LoadedContainer 中提取具体块
	entry, ok := loaded.LocalIndex[hash]
	if !ok {
		return nil, fmt.Errorf("chunk missing in container local index")
	}
	if int(entry.Offset+entry.Length) > len(loaded.RawData) {
		return nil, fmt.Errorf("data offset out of bounds")
	}

	result := make([]byte, entry.Length)
	copy(result, loaded.RawData[entry.Offset:entry.Offset+entry.Length])
	s.readStats.usefulBytes.Add(int64(entry.Length))
	return result, nil
}

func (s *ContainerStorage) SnapshotReadStats() ContainerReadStats {
	return ContainerReadStats{
		CacheHits:            s.readStats.cacheHits.Load(),
		CacheMisses:          s.readStats.cacheMisses.Load(),
		UsefulBytes:          s.readStats.usefulBytes.Load(),
		ContainerLoadedBytes: s.readStats.containerLoadedBytes.Load(),
		MetaReadBytes:        s.readStats.metaReadBytes.Load(),
	}
}

func (s *ContainerStorage) Flush() error {
	return s.flushContainer()
}

func (s *ContainerStorage) Close() error {
	err := s.flushContainer()
	if s.cfg.SingleFileMode && s.mainFile != nil {
		s.mainFile.Close()
	}
	return err
}
