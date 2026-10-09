package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type FileEntry struct {
	Path       string  `json:"path"`
	ChunkIDs   []int   `json:"chunk_ids"`
	ChunkSizes []int64 `json:"chunk_sizes"`
	Heat       float64 `json:"heat"`
	HeatDSize  float64 `json:"heat_dsize"`
}

type DatasetMeta struct {
	FormatVersion  string      `json:"format_version"`
	ChunkingMethod string      `json:"chunking_method"`
	ChunkBytes     int         `json:"chunk_bytes"`
	TotalSize      int64       `json:"total_size"`
	UniFingerprint []string    `json:"uni_fingerprint"`
	UniSize        []int64     `json:"uni_size"`
	Files          []FileEntry `json:"files"`
}

func main() {
	var (
		datasetRoot   string
		outputJSON    string
		chunkBytes    int
		popularityCSV string
		zipfS         float64
		zipfRandom    bool
		zipfSeed      int64
		progressEvery int
		pretty        bool
	)

	flag.StringVar(&datasetRoot, "dataset-root", "", "root directory of raw dataset (required)")
	flag.StringVar(&outputJSON, "output-json", "datasets/fileInfo-fixed-src.json", "output JSON path")
	flag.IntVar(&chunkBytes, "chunk-bytes", 4096, "fixed chunk size in bytes")
	flag.StringVar(&popularityCSV, "popularity-csv", "", "optional CSV with columns file,heat (overrides default Zipf heat)")
	flag.Float64Var(&zipfS, "zipf-s", 0.7, "Zipf distribution exponent for default file heat assignment")
	flag.BoolVar(&zipfRandom, "zipf-random", false, "randomize Zipf rank assignment across files")
	flag.Int64Var(&zipfSeed, "zipf-seed", 0, "random seed for --zipf-random (0 means current time)")
	flag.IntVar(&progressEvery, "progress-every", 500, "print progress every N files")
	flag.BoolVar(&pretty, "pretty", false, "pretty-print JSON output")
	flag.Parse()

	if datasetRoot == "" {
		exitf("missing required flag: --dataset-root")
	}
	if chunkBytes <= 0 {
		exitf("--chunk-bytes must be positive")
	}
	if zipfS <= 0 {
		exitf("--zipf-s must be positive")
	}
	if progressEvery <= 0 {
		progressEvery = 500
	}

	rootAbs, err := filepath.Abs(datasetRoot)
	if err != nil {
		exitf("resolve dataset root: %v", err)
	}
	st, err := os.Stat(rootAbs)
	if err != nil || !st.IsDir() {
		exitf("dataset root does not exist or is not dir: %s", rootAbs)
	}

	heatMap, err := loadHeatMap(popularityCSV)
	if err != nil {
		exitf("load popularity csv: %v", err)
	}

	files, err := walkFiles(rootAbs)
	if err != nil {
		exitf("scan dataset: %v", err)
	}
	if len(files) == 0 {
		exitf("no files found in dataset root: %s", rootAbs)
	}
	zipfHeat, err := buildZipfHeatMap(rootAbs, files, zipfS, zipfRandom, zipfSeed)
	if err != nil {
		exitf("build zipf heat map: %v", err)
	}

	fpToID := make(map[string]int)
	uniFP := make([]string, 0, 1024)
	uniSize := make([]int64, 0, 1024)
	entries := make([]FileEntry, 0, len(files))
	var totalSize int64

	for i, path := range files {
		rel, err := filepath.Rel(rootAbs, path)
		if err != nil {
			exitf("relative path for %s: %v", path, err)
		}
		rel = filepath.ToSlash(rel)

		ids, sizes, fsize, err := chunkFileFixed(path, chunkBytes, fpToID, &uniFP, &uniSize)
		if err != nil {
			exitf("chunk file %s: %v", path, err)
		}

		heat := zipfHeat[rel]
		if v, ok := heatMap[rel]; ok {
			heat = v
		}

		var fileBytes int64
		for _, s := range sizes {
			fileBytes += s
		}

		hd := 0.0
		if fileBytes > 0 {
			hd = heat / float64(fileBytes)
		}

		entries = append(entries, FileEntry{
			Path:       rel,
			ChunkIDs:   ids,
			ChunkSizes: sizes,
			Heat:       heat,
			HeatDSize:  hd,
		})

		totalSize += fsize
		if (i+1)%progressEvery == 0 || i+1 == len(files) {
			fmt.Printf("processed %d/%d files, unique chunks=%d\n", i+1, len(files), len(uniFP))
		}
	}

	meta := DatasetMeta{
		FormatVersion:  "dstp_fixed_go_v1",
		ChunkingMethod: "fixed",
		ChunkBytes:     chunkBytes,
		TotalSize:      totalSize,
		UniFingerprint: uniFP,
		UniSize:        uniSize,
		Files:          entries,
	}

	if err := os.MkdirAll(filepath.Dir(outputJSON), 0o755); err != nil && filepath.Dir(outputJSON) != "." {
		exitf("create output dir: %v", err)
	}

	f, err := os.Create(outputJSON)
	if err != nil {
		exitf("create output file: %v", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	if pretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(meta); err != nil {
		exitf("write json: %v", err)
	}

	fmt.Printf("saved %s (files=%d, unique_chunks=%d, total_size=%d)\n",
		outputJSON, len(entries), len(uniFP), totalSize)
}

func buildZipfHeatMap(rootAbs string, files []string, s float64, randomize bool, seed int64) (map[string]float64, error) {
	out := make(map[string]float64, len(files))
	rels := make([]string, 0, len(files))
	for _, path := range files {
		rel, err := filepath.Rel(rootAbs, path)
		if err != nil {
			return nil, err
		}
		rels = append(rels, filepath.ToSlash(rel))
	}
	if randomize {
		if seed == 0 {
			seed = time.Now().UnixNano()
		}
		rng := rand.New(rand.NewSource(seed))
		rng.Shuffle(len(rels), func(i, j int) {
			rels[i], rels[j] = rels[j], rels[i]
		})
		fmt.Printf("zipf random enabled: seed=%d\n", seed)
	}
	for i, rel := range rels {
		rank := float64(i + 1)
		out[rel] = 1.0 / math.Pow(rank, s)
	}
	return out, nil
}

func walkFiles(root string) ([]string, error) {
	files := make([]string, 0, 1024)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func loadHeatMap(csvPath string) (map[string]float64, error) {
	res := make(map[string]float64)
	if csvPath == "" {
		return res, nil
	}

	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		if sc.Err() != nil {
			return nil, sc.Err()
		}
		return nil, fmt.Errorf("empty csv")
	}
	header := parseCSVLine(sc.Text())

	fileIdx, heatIdx := -1, -1
	for i, h := range header {
		h = strings.TrimSpace(h)
		if h == "file" {
			fileIdx = i
		}
		if h == "heat" {
			heatIdx = i
		}
	}
	if fileIdx < 0 || heatIdx < 0 {
		return nil, fmt.Errorf("csv must contain columns file,heat")
	}

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		rec := parseCSVLine(line)
		if fileIdx >= len(rec) || heatIdx >= len(rec) {
			continue
		}
		rel := filepath.ToSlash(strings.TrimSpace(rec[fileIdx]))
		if rel == "" {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rec[heatIdx]), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid heat for %s: %w", rel, err)
		}
		res[rel] = v
	}
	if sc.Err() != nil {
		return nil, sc.Err()
	}
	return res, nil
}

func parseCSVLine(line string) []string {
	out := make([]string, 0, 8)
	cur := strings.Builder{}
	inQuote := false
	for i := 0; i < len(line); i++ {
		ch := line[i]
		if ch == '"' {
			if inQuote && i+1 < len(line) && line[i+1] == '"' {
				cur.WriteByte('"')
				i++
			} else {
				inQuote = !inQuote
			}
			continue
		}
		if ch == ',' && !inQuote {
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		}
		cur.WriteByte(ch)
	}
	out = append(out, strings.TrimSpace(cur.String()))
	return out
}

func chunkFileFixed(
	path string,
	chunkBytes int,
	fpToID map[string]int,
	uniFP *[]string,
	uniSize *[]int64,
) ([]int, []int64, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, 0, err
	}
	defer f.Close()

	ids := make([]int, 0, 16)
	sizes := make([]int64, 0, 16)
	buf := make([]byte, chunkBytes)
	var fsize int64

	for {
		n, err := io.ReadFull(f, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, nil, 0, err
		}
		if n > 0 {
			sum := sha1.Sum(buf[:n])
			fp := hex.EncodeToString(sum[:])
			id, ok := fpToID[fp]
			if !ok {
				id = len(*uniFP) + 1
				fpToID[fp] = id
				*uniFP = append(*uniFP, fp)
				*uniSize = append(*uniSize, int64(n))
			}
			ids = append(ids, id)
			sizes = append(sizes, int64(n))
			fsize += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}

	return ids, sizes, fsize, nil
}

func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
