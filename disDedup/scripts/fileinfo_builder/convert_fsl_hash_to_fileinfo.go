package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	hashFileMagic = 0xDEADDEAD
	maxPathSize   = 4096
	maxSysIDLen   = 4096

	chunkMethodFixed    = 1
	chunkMethodVariable = 2

	varAlgoRandom = 1
	varAlgoSimple = 2
	varAlgoRabin  = 3
)

type fileEntry struct {
	Path       string   `json:"path"`
	ChunkIDs   []uint32 `json:"chunk_ids"`
	ChunkSizes []uint32 `json:"chunk_sizes"`
	Heat       float64  `json:"heat"`
	HeatDSize  float64  `json:"heat_dsize"`
}

type datasetMeta struct {
	FormatVersion  string      `json:"format_version"`
	ChunkBytes     uint32      `json:"chunk_bytes"`
	TotalSize      uint64      `json:"total_size"`
	UniFingerprint []string    `json:"uni_fingerprint"`
	UniSize        []uint32    `json:"uni_size"`
	Files          []fileEntry `json:"files"`
}

type headerInfo struct {
	RootPath    string
	Files       uint64
	Chunks      uint64
	HashSize    uint32
	ChunkMethod uint32
	ChunkBytes  uint32
	TotalSize   uint64
}

func readU32(r io.Reader) (uint32, error) {
	var v uint32
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

func readU64(r io.Reader) (uint64, error) {
	var v uint64
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

func readBytes(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func trimCString(b []byte) string {
	if i := bytesIndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

func bytesIndexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

func inferChunkBytes(chunkMethod uint32, chunkParams []byte) uint32 {
	if chunkMethod == chunkMethodFixed {
		return binary.LittleEndian.Uint32(chunkParams[:4])
	}
	if chunkMethod == chunkMethodVariable && len(chunkParams) >= 44 {
		algo := binary.LittleEndian.Uint32(chunkParams[0:4])
		minSize := binary.LittleEndian.Uint32(chunkParams[36:40])
		maxSize := binary.LittleEndian.Uint32(chunkParams[40:44])
		if algo == varAlgoRabin || algo == varAlgoSimple {
			bits := binary.LittleEndian.Uint32(chunkParams[24:28])
			if bits > 0 && bits < 31 {
				return 1 << bits
			}
		}
		if minSize > 0 && maxSize >= minSize {
			return (minSize + maxSize) / 2
		}
	}
	return 8192
}

func parseHeader(r io.Reader) (*headerInfo, error) {
	magic, err := readU32(r)
	if err != nil {
		return nil, err
	}
	if magic != hashFileMagic {
		return nil, fmt.Errorf("unexpected magic: %#x", magic)
	}
	version, err := readU32(r)
	if err != nil {
		return nil, err
	}
	if version != 7 {
		return nil, fmt.Errorf("unsupported hash file version: %d", version)
	}
	files, err := readU64(r)
	if err != nil {
		return nil, err
	}
	rootRaw, err := readBytes(r, maxPathSize)
	if err != nil {
		return nil, err
	}
	chunks, err := readU64(r)
	if err != nil {
		return nil, err
	}
	chunkMethod, err := readU32(r)
	if err != nil {
		return nil, err
	}
	chunkParams, err := readBytes(r, 44)
	if err != nil {
		return nil, err
	}
	if _, err := readU32(r); err != nil { // hashing method
		return nil, err
	}
	hashSize, err := readU32(r)
	if err != nil {
		return nil, err
	}
	if _, err := readBytes(r, maxSysIDLen); err != nil {
		return nil, err
	}
	if _, err := readU64(r); err != nil { // start_time
		return nil, err
	}
	if _, err := readU64(r); err != nil { // end_time
		return nil, err
	}
	totalSize, err := readU64(r)
	if err != nil {
		return nil, err
	}
	return &headerInfo{
		RootPath:    trimCString(rootRaw),
		Files:       files,
		Chunks:      chunks,
		HashSize:    hashSize,
		ChunkMethod: chunkMethod,
		ChunkBytes:  inferChunkBytes(chunkMethod, chunkParams),
		TotalSize:   totalSize,
	}, nil
}

func makeRelPath(root, full string) string {
	if strings.HasPrefix(full, root) {
		rel := strings.TrimPrefix(full, root)
		return strings.TrimPrefix(rel, "/")
	}
	return strings.TrimPrefix(full, "/")
}

func main() {
	var (
		inputHash     string
		outputJSON    string
		zipfS         float64
		progressEvery int
		pretty        bool
	)

	flag.StringVar(&inputHash, "input-hash", "", "input .hash or .hash.anon file")
	flag.StringVar(&outputJSON, "output-json", "", "output mean_go_v1 json")
	flag.Float64Var(&zipfS, "zipf-s", 0.7, "Zipf distribution exponent")
	flag.IntVar(&progressEvery, "progress-every", 5000, "print progress every N files")
	flag.BoolVar(&pretty, "pretty", false, "pretty-print json")
	flag.Parse()

	if inputHash == "" || outputJSON == "" {
		fmt.Fprintln(os.Stderr, "missing required flags: --input-hash and --output-json")
		os.Exit(2)
	}
	if zipfS <= 0 {
		fmt.Fprintln(os.Stderr, "--zipf-s must be positive")
		os.Exit(2)
	}
	if progressEvery <= 0 {
		progressEvery = 5000
	}

	f, err := os.Open(inputHash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open input hash: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 4<<20)
	hdr, err := parseHeader(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse header: %v\n", err)
		os.Exit(1)
	}

	hashSizeBytes := int(hdr.HashSize / 8)
	if hashSizeBytes <= 0 {
		fmt.Fprintf(os.Stderr, "invalid hash size: %d\n", hdr.HashSize)
		os.Exit(1)
	}

	files := make([]fileEntry, 0, hdr.Files)
	uniMap := make(map[string]uint32, hdr.Chunks/2)
	uniFP := make([]string, 0, hdr.Chunks/2)
	uniSize := make([]uint32, 0, hdr.Chunks/2)

	for i := uint64(0); i < hdr.Files; i++ {
		fileSize, err := readU64(r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read file_size for file %d: %v\n", i, err)
			os.Exit(1)
		}
		if _, err := readU64(r); err != nil { // blocks
			fmt.Fprintf(os.Stderr, "read blocks for file %d: %v\n", i, err)
			os.Exit(1)
		}
		if _, err := readU32(r); err != nil { // uid
			fmt.Fprintf(os.Stderr, "read uid for file %d: %v\n", i, err)
			os.Exit(1)
		}
		if _, err := readU32(r); err != nil { // gid
			fmt.Fprintf(os.Stderr, "read gid for file %d: %v\n", i, err)
			os.Exit(1)
		}
		for j := 0; j < 7; j++ { // perm, atime, mtime, ctime, hardlinks, deviceid, inodenum
			if _, err := readU64(r); err != nil {
				fmt.Fprintf(os.Stderr, "read file metadata for file %d: %v\n", i, err)
				os.Exit(1)
			}
		}
		numChunks, err := readU64(r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read chunk count for file %d: %v\n", i, err)
			os.Exit(1)
		}
		pathLen, err := readU32(r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read path length for file %d: %v\n", i, err)
			os.Exit(1)
		}
		targetPathLen, err := readU32(r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read target path length for file %d: %v\n", i, err)
			os.Exit(1)
		}
		pathRaw, err := readBytes(r, int(pathLen))
		if err != nil {
			fmt.Fprintf(os.Stderr, "read path for file %d: %v\n", i, err)
			os.Exit(1)
		}
		if targetPathLen > 0 {
			if _, err := readBytes(r, int(targetPathLen)); err != nil {
				fmt.Fprintf(os.Stderr, "read target path for file %d: %v\n", i, err)
				os.Exit(1)
			}
		}
		relPath := makeRelPath(hdr.RootPath, string(pathRaw))

		ids := make([]uint32, 0, numChunks)
		sizes := make([]uint32, 0, numChunks)
		for c := uint64(0); c < numChunks; c++ {
			var chunkSize uint32
			if hdr.ChunkMethod == chunkMethodVariable {
				chunkSize, err = readU32(r)
				if err != nil {
					fmt.Fprintf(os.Stderr, "read chunk size for file %d chunk %d: %v\n", i, c, err)
					os.Exit(1)
				}
			} else {
				chunkSize = hdr.ChunkBytes
			}
			hashRaw, err := readBytes(r, hashSizeBytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "read chunk hash for file %d chunk %d: %v\n", i, c, err)
				os.Exit(1)
			}
			if _, err := readBytes(r, 1); err != nil { // cratio
				fmt.Fprintf(os.Stderr, "read chunk cratio for file %d chunk %d: %v\n", i, c, err)
				os.Exit(1)
			}
			fp := hex.EncodeToString(hashRaw)
			id, ok := uniMap[fp]
			if !ok {
				id = uint32(len(uniFP) + 1)
				uniMap[fp] = id
				uniFP = append(uniFP, fp)
				uniSize = append(uniSize, chunkSize)
			} else if uniSize[id-1] != chunkSize {
				fmt.Fprintf(os.Stderr, "chunk size mismatch for fingerprint %s: %d vs %d\n", fp, uniSize[id-1], chunkSize)
				os.Exit(1)
			}
			ids = append(ids, id)
			sizes = append(sizes, chunkSize)
		}

		files = append(files, fileEntry{
			Path:       relPath,
			ChunkIDs:   ids,
			ChunkSizes: sizes,
		})

		if (i+1)%uint64(progressEvery) == 0 || i+1 == hdr.Files {
			fmt.Fprintf(os.Stderr, "processed %d/%d files, unique_chunks=%d\n", i+1, hdr.Files, len(uniFP))
		}
		_ = fileSize
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	for i := range files {
		rank := float64(i + 1)
		files[i].Heat = 1.0 / math.Pow(rank, zipfS)
		var fileBytes uint64
		for _, sz := range files[i].ChunkSizes {
			fileBytes += uint64(sz)
		}
		if fileBytes > 0 {
			files[i].HeatDSize = files[i].Heat / float64(fileBytes)
		}
	}

	meta := datasetMeta{
		FormatVersion:  "mean_go_v1",
		ChunkBytes:     hdr.ChunkBytes,
		TotalSize:      hdr.TotalSize,
		UniFingerprint: uniFP,
		UniSize:        uniSize,
		Files:          files,
	}

	if err := os.MkdirAll(filepath.Dir(outputJSON), 0o755); err != nil && filepath.Dir(outputJSON) != "." {
		fmt.Fprintf(os.Stderr, "create output dir: %v\n", err)
		os.Exit(1)
	}
	out, err := os.Create(outputJSON)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output json: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	if pretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(meta); err != nil {
		fmt.Fprintf(os.Stderr, "encode json: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "saved %s (files=%d, unique_chunks=%d, total_size=%d, chunk_bytes=%d)\n",
		outputJSON, len(files), len(uniFP), hdr.TotalSize, hdr.ChunkBytes)
}
