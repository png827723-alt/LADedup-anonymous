package store

import (
	"container/list"
	"dedup-system/config"
	"dedup-system/meta"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ==========================================
// 1. LRU 缓存实现 (针对 []byte)
// ==========================================

// cacheEntry 用于在 List 中同时保存 Key 和 Value
// 这样在淘汰链表尾部时，才能反向删除 Map 中的 Key
type cacheEntry struct {
	key  string
	data []byte
}

type BlockCache struct {
	capacity int
	cache    map[string]*list.Element // Key -> List节点
	lruList  *list.List               // 双向链表
	lock     sync.Mutex
}

func NewBlockCache(capacity int) *BlockCache {
	return &BlockCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		lruList:  list.New(),
	}
}

func (c *BlockCache) Get(key string) ([]byte, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem) // 命中缓存，移到头部（最近使用）
		return elem.Value.(*cacheEntry).data, true
	}
	return nil, false
}

func (c *BlockCache) Put(key string, data []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 1. 如果已存在，更新数据并移到头部
	if elem, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(elem)
		elem.Value.(*cacheEntry).data = data
		return
	}

	// 2. 如果不存在，判断是否需要淘汰
	if c.lruList.Len() >= c.capacity {
		back := c.lruList.Back()
		if back != nil {
			// 移除链表尾部
			entry := back.Value.(*cacheEntry)
			delete(c.cache, entry.key) // 删除 Map 映射
			c.lruList.Remove(back)     // 删除 List 节点
		}
	}

	// 3. 插入新数据到头部
	newEntry := &cacheEntry{key: key, data: data}
	elem := c.lruList.PushFront(newEntry)
	c.cache[key] = elem
}

// ==========================================
// 2. BlockStorage 实现 (带缓存)
// ==========================================

type BlockStorage struct {
	cfg config.Config
}

func blockBucketName(id string) string {
	if len(id) >= 2 {
		return id[:2]
	}
	if id != "" {
		return id
	}
	return "_"
}

func blockRelativePath(id string) string {
	clean := strings.TrimSpace(id)
	if clean == "" {
		return ""
	}
	if strings.Contains(clean, string(os.PathSeparator)) {
		return clean
	}
	return filepath.Join(blockBucketName(clean), clean)
}

func blockAbsolutePath(root, id string) string {
	return filepath.Join(root, blockRelativePath(id))
}

func NewBlockStorage(cfg config.Config) *BlockStorage {
	// 直接使用配置文件中的 BlockCacheSize
	size := cfg.BlockCacheSize

	// 安全检查：如果用户忘了在 yaml 里配这个参数，默认为 0
	// 给一个合理的默认值，防止 map 频繁扩容或逻辑错误
	if size <= 0 {
		fmt.Println("[Warning] block_cache_size 未配置或为0，使用默认值 1000")
		size = 1000
	}

	return &BlockStorage{
		cfg: cfg,
	}
}

func (s *BlockStorage) Put(chunk *meta.Chunk, dedupID string) (string, bool, error) {
	if dedupID == "" {
		return "", false, fmt.Errorf("empty dedupID")
	}

	relPath := blockRelativePath(dedupID)
	path := filepath.Join(s.cfg.StoragePath, relPath)

	// 确保存储目录存在（保险起见）
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", false, err
	}

	// 直接落盘（覆盖写）
	if err := os.WriteFile(path, chunk.Data, 0644); err != nil {
		return "", false, err
	}
	return relPath, false, nil
}

func (s *BlockStorage) Get(hash string, locationID string) ([]byte, error) {
	if locationID == "" {
		return nil, fmt.Errorf("empty locationID")
	}
	path := filepath.Join(s.cfg.StoragePath, blockRelativePath(locationID))
	return os.ReadFile(path)
}

func (s *BlockStorage) Flush() error { return nil }

func (s *BlockStorage) Close() error { return nil }
