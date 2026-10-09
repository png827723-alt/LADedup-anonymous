package manager

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// LoadPlacementTableFromConfig loads placement mapping according to config-level inputs.
// Rule:
//   - placementJSON provided: enable strict placement-table mode from that file.
//   - placementJSON empty: disable placement-table mode and fall back to placement_strategy.
func LoadPlacementTableFromConfig(placementDir, placementJSON string) (map[string]string, bool, string, error) {
	jsonPath := strings.TrimSpace(placementJSON)
	if jsonPath != "" {
		st, err := os.Stat(jsonPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, false, "", fmt.Errorf("placement_json not found: %s", jsonPath)
			}
			return nil, false, "", err
		}
		if st.IsDir() {
			return nil, false, "", fmt.Errorf("placement_json must be a file, got directory: %s", jsonPath)
		}

		m, err := loadPlacementJSON(jsonPath)
		if err != nil {
			return nil, true, jsonPath, err
		}
		return m, true, jsonPath, nil
	}
	return nil, false, "", nil
}

// LoadPlacementTable loads a user-provided mapping table of "chunk_hash -> edge_node_id"
// from a directory. Supported formats:
//   - JSON: a single JSON object {"<hash>": "<node_id>", ...}
//   - Text/CSV: each non-empty line contains "<hash> <node_id>" or "<hash>,<node_id>"
//
// If the directory does not exist, (nil, false, nil) is returned.
// If the directory exists but has no supported files, (emptyMap, true, nil) is returned.
func LoadPlacementTable(dir string) (map[string]string, bool, error) {
	if dir == "" {
		dir = "./placement"
	}
	st, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !st.IsDir() {
		return nil, false, fmt.Errorf("placement_dir is not a directory: %s", dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, err
	}

	result := make(map[string]string)
	loadedAny := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		lower := strings.ToLower(name)
		path := filepath.Join(dir, name)

		switch {
		case strings.HasSuffix(lower, ".json"):
			m, err := loadPlacementJSON(path)
			if err != nil {
				return nil, true, err
			}
			for k, v := range m {
				if k == "" || v == "" {
					continue
				}
				result[k] = v
			}
			loadedAny = true
		case strings.HasSuffix(lower, ".txt"), strings.HasSuffix(lower, ".csv"), strings.HasSuffix(lower, ".tsv"), strings.HasSuffix(lower, ".map"):
			m, err := loadPlacementText(path)
			if err != nil {
				return nil, true, err
			}
			for k, v := range m {
				if k == "" || v == "" {
					continue
				}
				result[k] = v
			}
			loadedAny = true
		default:
			// ignore other files
		}
	}

	if !loadedAny {
		// placement dir exists, but user didn't provide any table.
		return result, true, nil
	}
	return result, true, nil
}

func loadPlacementJSON(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Support both:
	// 1) plain map {"<hash>":"<node_id>", ...}
	// 2) compact result json containing {"hash2edge_node_id": {...}, ...}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("parse placement json %s: %w", path, err)
	}

	src := raw
	if v, ok := raw["hash2edge_node_id"]; ok {
		if inner, ok2 := v.(map[string]any); ok2 {
			src = inner
		}
	}

	out := make(map[string]string, len(src))
	for k, v := range src {
		h := strings.TrimSpace(k)
		if h == "" {
			continue
		}
		switch x := v.(type) {
		case string:
			s := strings.TrimSpace(x)
			if s != "" {
				out[h] = s
			}
		case float64:
			out[h] = strconv.Itoa(int(x))
		case int:
			out[h] = strconv.Itoa(x)
		case int64:
			out[h] = strconv.FormatInt(x, 10)
		case json.Number:
			out[h] = x.String()
		default:
			// ignore unsupported value types
		}
	}
	return out, nil
}

func loadPlacementText(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := make(map[string]string)
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// allow comma separated
		line = strings.ReplaceAll(line, ",", " ")
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("invalid placement line %s:%d: %q", path, lineNo, line)
		}
		h := strings.TrimSpace(fields[0])
		nid := strings.TrimSpace(fields[1])
		if h == "" || nid == "" {
			continue
		}
		m[h] = nid
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return m, nil
}
