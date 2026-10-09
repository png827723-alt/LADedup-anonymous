package manager

import (
	"dedup-system/meta"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func parseNodeNumber(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case float64:
		return int(x), true
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return int(i), true
		}
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, false
		}
		if i, err := strconv.Atoi(s); err == nil {
			return i, true
		}
		ls := strings.ToLower(s)
		if ls == "cloud" {
			return CloudNodeNumber, true
		}
		if strings.HasPrefix(ls, "edge") {
			if i, err := strconv.Atoi(strings.TrimPrefix(ls, "edge")); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}

func LoadGlobalMeta(path string) (meta.GlobalChunkIndex, error) {
	m := make(meta.GlobalChunkIndex)
	if path == "" {
		return m, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return m, nil
	}

	// New format: direct map {"<hash>": <node_number>, ...}
	if err := json.Unmarshal(b, &m); err == nil {
		return m, nil
	}

	// Backward compatibility:
	// old wrapped format {"hash_to_node": {...}} or mixed dynamic map values.
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err == nil {
		if hv, ok := raw["hash_to_node"]; ok {
			if wrapped, ok := hv.(map[string]any); ok {
				for h, v := range wrapped {
					if n, ok := parseNodeNumber(v); ok {
						m[h] = n
					}
				}
				return m, nil
			}
		}
		for h, v := range raw {
			if n, ok := parseNodeNumber(v); ok {
				m[h] = n
			}
		}
		return m, nil
	}
	return m, nil
}

func SaveGlobalMeta(path string, m meta.GlobalChunkIndex) error {
	if path == "" || m == nil {
		return nil
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
