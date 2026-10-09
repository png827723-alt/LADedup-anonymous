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
			fastcdc.WithMinSize(uint32(size / 4)),
			fastcdc.WithTargetSize(uint32(size)),
			fastcdc.WithMaxSize(uint32(size * 4)),
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
	if err == io.EOF {
		return nil, io.EOF
	}
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
		Offset: cdcChunk.Offset,
		Length: cdcChunk.Length,
		Hash:   cdcChunk.Hash,
		Data:   cdcChunk.Data,
	}, nil
}
