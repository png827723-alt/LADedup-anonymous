package manager

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const maxRecipeBaseNameLen = 96

func GenerateRecipeName(originalPath string) (string, error) {
	absPath, err := filepath.Abs(originalPath)
	if err != nil {
		return "", err
	}
	sum := sha1.Sum([]byte(absPath))
	base := sanitizeRecipeBase(filepath.Base(absPath))
	if len(base) > maxRecipeBaseNameLen {
		base = base[:maxRecipeBaseNameLen]
	}
	return fmt.Sprintf("%s-%s.recipe", base, hex.EncodeToString(sum[:])), nil
}

func generateLegacyRecipeName(originalPath string) (string, error) {
	absPath, err := filepath.Abs(originalPath)
	if err != nil {
		return "", err
	}
	safeName := strings.ReplaceAll(absPath, ":", "")
	safeName = strings.ReplaceAll(safeName, "/", "-")
	safeName = strings.ReplaceAll(safeName, "\\", "-")
	return safeName + ".recipe", nil
}

func sanitizeRecipeBase(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	lastDash := false
	for _, r := range name {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		switch {
		case isAlphaNum || r == '.' || r == '_':
			b.WriteRune(r)
			lastDash = false
		case r == '-':
			if !lastDash {
				b.WriteRune(r)
				lastDash = true
			}
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	base := strings.Trim(b.String(), "-.")
	if base == "" {
		return "file"
	}
	return base
}

func ResolveRecipePath(recipeDir string, inputNameOrPath string) (string, error) {
	tryA := filepath.Join(recipeDir, inputNameOrPath)
	if _, err := os.Stat(tryA); err == nil {
		return tryA, nil
	}
	safe, err := GenerateRecipeName(inputNameOrPath)
	if err == nil {
		tryB := filepath.Join(recipeDir, safe)
		if _, err := os.Stat(tryB); err == nil {
			return tryB, nil
		}
	}
	legacy, legacyErr := generateLegacyRecipeName(inputNameOrPath)
	if legacyErr == nil {
		tryC := filepath.Join(recipeDir, legacy)
		if _, err := os.Stat(tryC); err == nil {
			return tryC, nil
		}
		return "", fmt.Errorf("recipe not found, tried: %s and %s and %s", tryA, filepath.Join(recipeDir, safe), tryC)
	}
	if err != nil {
		return "", err
	}
	return "", fmt.Errorf("recipe not found, tried: %s and %s", tryA, filepath.Join(recipeDir, safe))
}
