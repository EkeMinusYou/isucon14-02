package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestISURIDEParticipantResult(t *testing.T) {
	p, err := loadPatterns("../contest/bench-patterns.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ log, want string }{
		{"time=03:56:51.330 level=INFO msg=結果 pass=true スコア=671 種別エラー数=map[]\n", "SCORE: 671\nBENCHMARK_PASS\n"},
		{"time=03:56:51.330 level=INFO msg=結果 pass=false スコア=0 種別エラー数=map[]\n", "SCORE: 0\nBENCHMARK_FAIL\n"},
		{"time=03:56:51.330 level=INFO msg=負荷走行が終了しました\n", ""},
		{"SCORE: 123\nBENCHMARK_PASS\n", "SCORE: 123\nBENCHMARK_PASS\n"},
		{"SCORE: 123\nBENCHMARK_PASS\nBENCHMARK_FAIL\texit_status=7\n", "SCORE: 123\nBENCHMARK_FAIL\n"},
	} {
		w := &resultWriter{out: &bytes.Buffer{}, patterns: p}
		if _, err := w.Write([]byte(tc.log)); err != nil {
			t.Fatal(err)
		}
		if got := string(w.markers()); got != tc.want {
			t.Fatalf("got %q want %q", got, tc.want)
		}
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "runs/20261011-035537/bench.log")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Skip("saved participant RUN is absent")
	}
	var output, errors bytes.Buffer
	if code := run([]string{"-patterns", "../contest/bench-patterns.json", "-input", path}, &output, &errors); code != 0 || output.String() != "SCORE: 671\nBENCHMARK_PASS\n" {
		t.Fatalf("saved RUN: code=%d output=%q errors=%q", code, output.String(), errors.String())
	}
}
