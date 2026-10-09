package manager

import "testing"

func TestFullFileObjectNameCandidatesPreferLegacyInputPath(t *testing.T) {
	inputPath := "/input/github_repo/org/repo/v1/source_code.zip"
	recipePath := "/data/recipe/source_code.zip-0123456789abcdef.recipe"

	candidates, err := fullFileObjectNameCandidates(inputPath, recipePath)
	if err != nil {
		t.Fatalf("fullFileObjectNameCandidates returned error: %v", err)
	}
	if len(candidates) != 3 {
		t.Fatalf("unexpected candidate count: got %d want 3", len(candidates))
	}

	legacyRecipeName, err := generateLegacyRecipeName(inputPath)
	if err != nil {
		t.Fatalf("generateLegacyRecipeName returned error: %v", err)
	}
	hashedRecipeName, err := GenerateRecipeName(inputPath)
	if err != nil {
		t.Fatalf("GenerateRecipeName returned error: %v", err)
	}

	wantLegacy := legacyRecipeName[:len(legacyRecipeName)-len(".recipe")]
	wantHashed := hashedRecipeName[:len(hashedRecipeName)-len(".recipe")]
	wantRecipe := "source_code.zip-0123456789abcdef"

	if candidates[0] != wantLegacy {
		t.Fatalf("legacy candidate mismatch: got %q want %q", candidates[0], wantLegacy)
	}
	if candidates[1] != wantHashed {
		t.Fatalf("hashed candidate mismatch: got %q want %q", candidates[1], wantHashed)
	}
	if candidates[2] != wantRecipe {
		t.Fatalf("recipe candidate mismatch: got %q want %q", candidates[2], wantRecipe)
	}
}
