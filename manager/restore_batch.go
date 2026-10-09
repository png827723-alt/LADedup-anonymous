package manager

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func LoadRestoreRequests(path string) ([]RestoreRequest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reqs := make([]RestoreRequest, 0)
	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}

		input, output, err := parseRestoreRequestLine(raw)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, lineNo, err)
		}
		reqs = append(reqs, RestoreRequest{
			InputNameOrPath: input,
			OutputPath:      output,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(reqs) == 0 {
		return nil, fmt.Errorf("no valid restore requests found in %s", path)
	}
	return reqs, nil
}

func parseRestoreRequestLine(line string) (string, string, error) {
	if strings.Contains(line, "\t") {
		parts := strings.Split(line, "\t")
		if len(parts) == 1 {
			in := strings.TrimSpace(parts[0])
			if in == "" {
				return "", "", fmt.Errorf("invalid request line: input cannot be empty")
			}
			return in, "", nil
		}
		if len(parts) >= 2 {
			first := strings.TrimSpace(parts[0])
			if _, err := strconv.ParseFloat(first, 64); err == nil {
				in := strings.TrimSpace(parts[1])
				if in == "" {
					return "", "", fmt.Errorf("invalid request line: input cannot be empty")
				}
				out := ""
				if len(parts) >= 3 {
					out = strings.TrimSpace(strings.Join(parts[2:], "\t"))
				}
				return in, out, nil
			}
		}
		if len(parts) == 2 {
			in := strings.TrimSpace(parts[0])
			if in == "" {
				return "", "", fmt.Errorf("invalid request line: input cannot be empty")
			}
			out := strings.TrimSpace(parts[1])
			return in, out, nil
		}
		return "", "", fmt.Errorf("invalid request line, expected '<recipe_or_original_path>[<TAB><output_path>]' or '<arrival_sec><TAB><input_path>[<TAB><output_path>]'")
	}

	parts := strings.Fields(line)
	if len(parts) == 1 {
		in := strings.TrimSpace(parts[0])
		if in == "" {
			return "", "", fmt.Errorf("invalid request line: input cannot be empty")
		}
		return in, "", nil
	}
	if len(parts) == 2 {
		if _, err := strconv.ParseFloat(parts[0], 64); err == nil {
			in := strings.TrimSpace(parts[1])
			if in == "" {
				return "", "", fmt.Errorf("invalid request line: input cannot be empty")
			}
			return in, "", nil
		}
		in := strings.TrimSpace(parts[0])
		if in == "" {
			return "", "", fmt.Errorf("invalid request line: input cannot be empty")
		}
		out := strings.TrimSpace(parts[1])
		return in, out, nil
	}

	// Also support whitespace-separated arrival trace line:
	//   <arrival_sec> <input_path> [<output_path>]
	if len(parts) >= 3 {
		if _, err := strconv.ParseFloat(parts[0], 64); err == nil {
			in := strings.TrimSpace(parts[1])
			if in == "" {
				return "", "", fmt.Errorf("invalid request line: input cannot be empty")
			}
			out := strings.TrimSpace(parts[2])
			return in, out, nil
		}
	}
	return "", "", fmt.Errorf("invalid request line, expected '<recipe_or_original_path>[<TAB><output_path>]' or '<arrival_sec><TAB><input_path>[<TAB><output_path>]'")
}
