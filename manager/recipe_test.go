package manager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateRecipeNameShortAndStable(t *testing.T) {
	t.Helper()

	longName := strings.Repeat("segment-", 40) + "?p=" + strings.Repeat("value", 40) + ".html"
	inputPath := filepath.Join(t.TempDir(), longName)

	nameA, err := GenerateRecipeName(inputPath)
	if err != nil {
		t.Fatalf("GenerateRecipeName returned error: %v", err)
	}
	nameB, err := GenerateRecipeName(inputPath)
	if err != nil {
		t.Fatalf("GenerateRecipeName returned error on second call: %v", err)
	}

	if nameA != nameB {
		t.Fatalf("GenerateRecipeName should be stable, got %q and %q", nameA, nameB)
	}
	if len(filepath.Base(nameA)) > 255 {
		t.Fatalf("recipe filename too long: %d", len(filepath.Base(nameA)))
	}
	if !strings.HasSuffix(nameA, ".recipe") {
		t.Fatalf("recipe filename should end with .recipe: %q", nameA)
	}
}

func TestResolveRecipePathFallsBackToLegacyName(t *testing.T) {
	t.Helper()

	recipeDir := t.TempDir()
	inputPath := filepath.Join(recipeDir, strings.Repeat("x", 40)+".html")

	legacyName, err := generateLegacyRecipeName(inputPath)
	if err != nil {
		t.Fatalf("generateLegacyRecipeName returned error: %v", err)
	}
	legacyPath := filepath.Join(recipeDir, legacyName)
	if err := os.WriteFile(legacyPath, []byte("legacy"), 0644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	resolved, err := ResolveRecipePath(recipeDir, inputPath)
	if err != nil {
		t.Fatalf("ResolveRecipePath returned error: %v", err)
	}
	if resolved != legacyPath {
		t.Fatalf("ResolveRecipePath should return legacy path, got %q want %q", resolved, legacyPath)
	}
}
