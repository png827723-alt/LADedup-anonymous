package manager

import "testing"

func TestParseRestoreRequestLine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		line    string
		wantIn  string
		wantOut string
		wantErr bool
	}{
		{
			name:    "input only",
			line:    "/input/a.txt",
			wantIn:  "/input/a.txt",
			wantOut: "",
		},
		{
			name:    "input and output",
			line:    "/input/a.txt\t/output/a.txt",
			wantIn:  "/input/a.txt",
			wantOut: "/output/a.txt",
		},
		{
			name:    "arrival with input only",
			line:    "0.125\t/input/a.txt",
			wantIn:  "/input/a.txt",
			wantOut: "",
		},
		{
			name:    "arrival with input and output",
			line:    "0.125\t/input/a.txt\t/output/a.txt",
			wantIn:  "/input/a.txt",
			wantOut: "/output/a.txt",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotIn, gotOut, err := parseRestoreRequestLine(tc.line)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got input=%q output=%q", gotIn, gotOut)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRestoreRequestLine(%q) error = %v", tc.line, err)
			}
			if gotIn != tc.wantIn || gotOut != tc.wantOut {
				t.Fatalf("parseRestoreRequestLine(%q) = (%q, %q), want (%q, %q)", tc.line, gotIn, gotOut, tc.wantIn, tc.wantOut)
			}
		})
	}
}
