package pprofimport

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/pprof/profile"
)

func TestISURIDESetupSmokePprofDuckDB(t *testing.T) {
	dir := os.Getenv("ISUCON_SETUP_SMOKE_DIR")
	if dir == "" {
		t.Skip("set ISUCON_SETUP_SMOKE_DIR to collected setup smoke artifacts")
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.pprof"))
	if err != nil || len(paths) != 5 {
		t.Fatalf("expected five profiles, got %d: %v", len(paths), err)
	}
	var normalized []normalizedProfile
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := profile.ParseData(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := parsed.CheckValid(); err != nil {
			t.Fatal(err)
		}
		for index := range parsed.SampleType {
			// Exercise the production adapter's filename contract in memory.
			// The persisted identity remains the actual smoke session, not a RUN.
			p, err := normalizeMetric(filepath.Join("20000101-000000", filepath.Base(path)), data, parsed, index)
			if err != nil {
				t.Fatal(err)
			}
			p.metadata.runID = filepath.Base(dir)
			normalized = append(normalized, p)
		}
	}
	outputDir := filepath.Join(dir, "profile-import")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := writeProfiles(outputDir, nil); err != nil {
		t.Fatal(err)
	}
	if err := writeGenericProfiles(outputDir, normalized); err != nil {
		t.Fatal(err)
	}
	schema, err := os.ReadFile("../../../analysis/schema/fgprof.sql")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("duckdb", "-json", "-batch")
	cmd.Env = append(os.Environ(), "ISUCON_PROFILE_DIR="+outputDir)
	cmd.Stdin = strings.NewReader(string(schema) + "\nselect profile_type, sample_type, value_unit, total_value, sample_count, duration_seconds from pprof_metadata_raw order by profile_type, sample_type;\n")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("profile DuckDB analysis failed: %v\n%s", err, output)
	}
	var rows []struct {
		ProfileType string  `json:"profile_type"`
		SampleType  string  `json:"sample_type"`
		ValueUnit   string  `json:"value_unit"`
		TotalValue  float64 `json:"total_value"`
		SampleCount int     `json:"sample_count"`
	}
	if err := json.Unmarshal(output, &rows); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, row := range rows {
		kinds[row.ProfileType] = true
		t.Logf("%s/%s: %g %s, %d samples", row.ProfileType, row.SampleType, row.TotalValue, row.ValueUnit, row.SampleCount)
	}
	if len(kinds) != 5 {
		t.Fatalf("expected five analyzed profile types, got %v", kinds)
	}
	if err := os.WriteFile(filepath.Join(dir, "pprof-duckdb-smoke.json"), output, 0644); err != nil {
		t.Fatal(err)
	}
}
