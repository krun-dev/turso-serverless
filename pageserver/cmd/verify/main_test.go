package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func sample(name string, passed bool) document {
	return document{"kind": "test", "file": "f.sqltest", "name": name, "database": "Memory", "passed": passed, "skipped": false, "failed": !passed, "error": false}
}
func TestPairingPreservesDuplicateCaseMultiplicity(t *testing.T) {
	a := sample("a", true)
	b := sample("b", true)
	if len(differences([]document{a, a, b}, []document{a, b})) != 1 {
		t.Fatal("missing duplicate case was ignored")
	}
	if len(differences([]document{a, b}, []document{b, a})) != 0 {
		t.Fatal("ordering changed outcomes")
	}
	if len(differences([]document{a}, []document{sample("a", false)})) != 1 {
		t.Fatal("changed outcome was ignored")
	}
	c := countCases([]document{a, a, b}, func(document) bool { return true })
	if c[key(a)] != 2 {
		t.Fatal("counter dropped duplicate")
	}
}
func TestRecordsDoNotSilentlyAcceptTruncation(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "results.jsonl")
	check(os.WriteFile(p, []byte("{\"kind\":\"test\"}\n{\"kind\":"), 0600))
	defer func() {
		if recover() == nil {
			t.Fatal("truncated JSONL accepted")
		}
	}()
	records(p)
}
func TestSummaryWithRecordedCorpus(t *testing.T) {
	// Exercise the coverage verifier against the already completed, unchanged
	// corpus. Each mutation must be rejected without running another corpus.
	source := filepath.Join(must(filepath.Abs("../../..")), "evidence", "recorded-corpus-memory")
	fullPath := filepath.Join(source, "full-cloud-memory-latest.json")
	if _, err := os.Stat(fullPath); err != nil {
		t.Skip("completed corpus evidence unavailable")
	}
	full := read(fullPath)
	if full["source_commit"] != sourceCommit {
		t.Skip("recorded corpus belongs to a different Turso version")
	}
	if number(summaries(full, "remote-cloud")["total"]) != fullCorpusCases {
		t.Skip("latest evidence is a subset")
	}
	categories := []string{"full-cloud", "strict-cloud", "skipped-cloud", "persistent-skipped-cloud", "archive", "cold"}
	prepare := func() string {
		root := t.TempDir()
		check(os.MkdirAll(filepath.Join(root, "evidence"), 0700))
		for _, c := range categories {
			write(filepath.Join(root, "evidence", c+"-memory-latest.json"), read(filepath.Join(source, c+"-memory-latest.json")))
		}
		return root
	}
	t.Run("complete", func(t *testing.T) {
		if summary(prepare(), nil) != 0 {
			t.Fatal("completed corpus rejected")
		}
	})
	mutations := map[string]func(document){"filter": func(r document) { r["test_filter"] = "partial" }, "missing_count": func(r document) { summaries(r, "remote-cloud")["total"] = fullCorpusCases - 1 }, "added_failure": func(r document) { r["paired_outcome_differences"] = []any{document{"key": "extra"}} }, "updated_snapshot": func(r document) { summaries(r, "official")["snapshots_updated"] = 1 }}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			root := prepare()
			path := filepath.Join(root, "evidence/full-cloud-memory-latest.json")
			r := read(path)
			mutate(r)
			write(path, r)
			defer func() {
				if recover() == nil {
					t.Fatal("incomplete acceptance was allowed")
				}
			}()
			summary(root, nil)
		})
	}
	t.Run("mixed_safety_revision", func(t *testing.T) {
		root := prepare()
		path := filepath.Join(root, "evidence/archive-memory-latest.json")
		r := read(path)
		r["source_commit"] = "8cc059bd951551ae4b9dc31c01146a24b3e87a51"
		write(path, r)
		defer func() {
			if recover() == nil {
				t.Fatal("safety checks from a different Turso version accepted")
			}
		}()
		summary(root, nil)
	})
	t.Run("json_round_trip", func(t *testing.T) {
		raw := must(json.Marshal(full))
		var d document
		check(json.Unmarshal(raw, &d))
		if !reflect.DeepEqual(full, d) {
			t.Fatal("evidence changed")
		}
	})
}
