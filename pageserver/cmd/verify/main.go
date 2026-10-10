// verify runs the unchanged upstream SQL corpus and checks exact coverage.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	ps "turso-pageserver"
)

type document = map[string]any

const sourceCommit = "5c168be5ef6a32ac966daf5983502a4f694b37ad"
const runnerID = "pageserver-corpus-v1"
const fullCorpusCases = 16182
const strictCorpusCases = 342

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
func require(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}
func doc(v any) document {
	if v == nil {
		return document{}
	}
	return v.(map[string]any)
}
func list(v any) []any {
	if v == nil {
		return nil
	}
	return v.([]any)
}
func str(v any) string {
	if v == nil {
		return ""
	}
	return v.(string)
}
func yes(v any) bool { return v == true }
func number(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return 0
}
func read(path string) document {
	var d document
	check(json.Unmarshal(must(os.ReadFile(path)), &d))
	return d
}
func write(path string, v any) {
	raw := must(json.MarshalIndent(v, "", "  "))
	check(os.WriteFile(path, append(raw, '\n'), 0600))
}
func rootDir() string { exe := must(os.Executable()); return filepath.Dir(filepath.Dir(exe)) }
func unique() string {
	raw := make([]byte, 16)
	_, err := rand.Read(raw)
	check(err)
	return hex.EncodeToString(raw)
}
func copyTree(src, dst string) {
	check(filepath.WalkDir(src, func(p string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := must(filepath.Rel(src, p))
		target := filepath.Join(dst, rel)
		if e.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		return os.WriteFile(target, must(os.ReadFile(p)), 0600)
	}))
}
func records(path string) []document {
	f := must(os.Open(path))
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 65536), 16*1024*1024)
	out := []document{}
	for scanner.Scan() {
		var d document
		check(json.Unmarshal(scanner.Bytes(), &d))
		out = append(out, d)
	}
	check(scanner.Err())
	return out
}
func cases(report document, mode string) []document {
	all := records(filepath.Join(str(report["evidence"]), mode, "results.jsonl"))
	out := []document{}
	for _, r := range all {
		if r["kind"] == "test" {
			out = append(out, r)
		}
	}
	return out
}

type caseKey struct{ File, Name, Database string }

func key(c document) caseKey { return caseKey{str(c["file"]), str(c["name"]), str(c["database"])} }

type counter map[caseKey]int

func countCases(cases []document, include func(document) bool) counter {
	c := counter{}
	for _, v := range cases {
		if include(v) {
			c[key(v)]++
		}
	}
	return c
}
func outcomeCounts(cases []document) map[caseKey]map[string]int {
	r := map[caseKey]map[string]int{}
	for _, c := range cases {
		k := key(c)
		if r[k] == nil {
			r[k] = map[string]int{}
		}
		state := fmt.Sprintf("%v/%v/%v/%v", c["passed"], c["skipped"], c["failed"], c["error"])
		r[k][state]++
	}
	return r
}
func differences(a, b []document) []any {
	left, right := outcomeCounts(a), outcomeCounts(b)
	keys := map[caseKey]bool{}
	for k := range left {
		keys[k] = true
	}
	for k := range right {
		keys[k] = true
	}
	sorted := []caseKey{}
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool { return fmt.Sprint(sorted[i]) < fmt.Sprint(sorted[j]) })
	out := []any{}
	for _, k := range sorted {
		if !reflect.DeepEqual(left[k], right[k]) {
			out = append(out, document{"key": []string{k.File, k.Name, k.Database}, "official": left[k], "cloud": right[k]})
		}
	}
	return out
}
func admin(port int) document {
	c := must(net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 15*time.Second))
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second))
	check(json.NewEncoder(c).Encode(document{"op": "admin", "path": "control", "pos": 0, "len": 0, "extra": document{"method": "stats"}}))
	var d document
	check(json.Unmarshal(must(bufio.NewReader(c).ReadBytes('\n')), &d))
	require(d["error"] == nil, fmt.Sprint(d))
	return d
}
func cleanEnvironment() []string {
	env := []string{}
	for _, v := range os.Environ() {
		name, _, _ := strings.Cut(v, "=")
		if name == "E2E_FULL" || name == "E2E_STRICT" || name == "E2E_SKIPPED_DIAGNOSTICS" || name == "E2E_SKIPPED_PERSISTENT" {
			continue
		}
		env = append(env, v)
	}
	return append(env, "E2E_FULL=1")
}
func official(root string, args []string) int {
	f := flag.NewFlagSet("official", flag.ExitOnError)
	oss := f.Bool("oss", false, "test the configured Aliyun S3-compatible endpoint")
	strict := f.Bool("strict", false, "STRICT capability supplement")
	skipped := f.Bool("skipped-diagnostics", false, "execute upstream TODO cases, retaining failures")
	persistent := f.Bool("persistent-diagnostics", false, "repeat TODO cases as persistent databases")
	f.Parse(args)
	require(!*persistent || *skipped, "--persistent-diagnostics requires --skipped-diagnostics")
	suites := f.Args()
	if len(suites) == 0 {
		suites = []string{"sqlite-sqltests", "turso-sqltests"}
	}
	source := filepath.Join(root, ".deps", "turso")
	commit := strings.TrimSpace(string(must(exec.Command("git", "-C", source, "rev-parse", "HEAD").Output())))
	require(commit == sourceCommit, "unexpected upstream revision")
	name := "full-cloud-" + time.Now().Format("20060102-150405") + "-" + unique()
	run := filepath.Join(root, "evidence", name)
	check(os.MkdirAll(run, 0700))
	backend := "memory"
	if *oss {
		backend = "oss"
	}
	prefix := "test-runs/" + name
	if *oss {
		prefix = ps.StorageRunPrefix(name)
		require(os.Getenv("S3_ENDPOINT") != "" && os.Getenv("S3_BUCKET") != "", "S3_ENDPOINT and S3_BUCKET are required")
	}
	storage := filepath.Join(run, "remote-cloud/storage")
	check(os.MkdirAll(storage, 0700))
	cmdArgs := []string{"cloud", "--root", storage}
	if *oss {
		cmdArgs = append(cmdArgs, "--oss", "--prefix", prefix)
	}
	server := exec.Command(filepath.Join(root, "bin/pageserver"), cmdArgs...)
	serverErr := must(os.Create(filepath.Join(run, "server.stderr")))
	defer serverErr.Close()
	server.Stderr = serverErr
	out := bufio.NewReader(must(server.StdoutPipe()))
	check(server.Start())
	defer func() { server.Process.Kill(); server.Wait() }()
	var ready document
	check(json.Unmarshal(must(out.ReadBytes('\n')), &ready))
	port := number(ready["port"])
	paths := []string{}
	for _, p := range suites {
		paths = append(paths, filepath.Join(source, "sqlite/conformance", p))
	}
	report := document{"runner_id": runnerID, "source_commit": sourceCommit, "backend": backend, "requested_suites": suites, "test_filter": nil, "strict_supplement": *strict, "explicit_skips_overridden_for_diagnostics": *skipped, "persistent_diagnostic_variant": *persistent, "prefix": nil, "evidence": run, "runs": document{}, "scope": "unchanged upstream WAL SQL cases and matrices, EXPLAIN verification and annotated SQLite integrity; explicit MVCC excluded"}
	if filter := os.Getenv("E2E_FILTER"); filter != "" {
		report["test_filter"] = filter
	}
	if *oss {
		report["prefix"] = prefix
	}
	category := "full-cloud"
	if len(f.Args()) > 0 {
		category = "subset-cloud"
	}
	if *strict {
		category = "strict-cloud"
	}
	if *skipped {
		category = "skipped-cloud"
	}
	if *persistent {
		category = "persistent-skipped-cloud"
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	procs := map[string]*exec.Cmd{}
	waits := map[string]<-chan error{}
	defer func() {
		for _, p := range procs {
			if p.Process != nil {
				p.Process.Kill()
			}
		}
	}()
	for _, mode := range []string{"official", "remote-cloud"} {
		base := filepath.Join(run, mode)
		check(os.MkdirAll(filepath.Join(base, "storage"), 0700))
		copyTree(filepath.Join(source, "sqlite/conformance/database"), filepath.Join(base, "database"))
		env := cleanEnvironment()
		if *strict {
			env = append(env, "E2E_STRICT=1")
		}
		if *skipped {
			env = append(env, "E2E_SKIPPED_DIAGNOSTICS=1")
		}
		if *persistent {
			env = append(env, "E2E_SKIPPED_PERSISTENT=1")
		}
		p := exec.CommandContext(ctx, filepath.Join(root, "target/corpus/official-e2e"), append([]string{mode, filepath.Join(base, "storage"), fmt.Sprintf("127.0.0.1:%d", port)}, paths...)...)
		p.Dir = base
		p.Env = env
		stdout := must(os.Create(filepath.Join(base, "results.jsonl")))
		stderr := must(os.Create(filepath.Join(base, "stderr.log")))
		p.Stdout = stdout
		p.Stderr = stderr
		check(p.Start())
		procs[mode] = p
		ch := make(chan error, 1)
		waits[mode] = ch
		go func() { err := p.Wait(); stdout.Close(); stderr.Close(); ch <- err }()
	}
	results := map[string][]document{}
	passed := true
	for _, mode := range []string{"official", "remote-cloud"} {
		err := <-waits[mode]
		if err != nil {
			passed = false
		}
		all := records(filepath.Join(run, mode, "results.jsonl"))
		summary, scope, failures := []any{}, []any{}, []any{}
		reasons := map[string]int{}
		for _, r := range all {
			switch r["kind"] {
			case "test":
				results[mode] = append(results[mode], r)
				if yes(r["skipped"]) {
					reasons[str(r["outcome"])]++
				}
				if !yes(r["passed"]) && !yes(r["skipped"]) {
					failures = append(failures, r)
				}
			case "summary":
				summary = append(summary, r)
			case "scope":
				scope = append(scope, r)
			}
		}
		if len(summary) != 1 || len(results[mode]) != number(doc(summary[0])["total"]) {
			passed = false
		}
		doc(report["runs"])[mode] = document{"exit_code": procs[mode].ProcessState.ExitCode(), "summaries": summary, "scope": scope, "completed": len(results[mode]), "skip_reasons": reasons, "failures": failures}
	}
	diffs := differences(results["official"], results["remote-cloud"])
	report["paired_outcome_differences"] = diffs
	report["cloud_stats"] = admin(port)
	report["all_executable_cases_passed"] = passed && len(diffs) == 0
	report["duration_seconds"] = time.Since(started).Seconds()
	if ctx.Err() != nil {
		report["timeout"] = ctx.Err().Error()
	}
	write(filepath.Join(run, "report.json"), report)
	write(filepath.Join(root, "evidence", category+"-"+backend+"-latest.json"), report)
	fmt.Printf("%s: cases=%d, paired differences=%d, passed=%v; evidence=%s\n", category, len(results["remote-cloud"]), len(diffs), report["all_executable_cases_passed"], run)
	if !yes(report["all_executable_cases_passed"]) {
		return 1
	}
	return 0
}
func summaries(r document, mode string) document {
	run := doc(doc(r["runs"])[mode])
	values := list(run["summaries"])
	require(len(values) == 1, "missing/duplicate summary")
	return doc(values[0])
}

var unsupported = map[string]bool{"gencol_vacuum_virtual": true, "gencol_vacuum_virtual_with_index": true}

func readCategory(root, category, backend string) document {
	return read(filepath.Join(root, "evidence", category+"-"+backend+"-latest.json"))
}
func summary(root string, args []string) int {
	f := flag.NewFlagSet("summary", flag.ExitOnError)
	oss := f.Bool("oss", false, "S3-compatible evidence")
	f.Parse(args)
	require(f.NArg() == 0, "unexpected arguments")
	backend := "memory"
	if *oss {
		backend = "oss"
	}
	full, strict, skipped, persistent := readCategory(root, "full-cloud", backend), readCategory(root, "strict-cloud", backend), readCategory(root, "skipped-cloud", backend), readCategory(root, "persistent-skipped-cloud", backend)
	for _, r := range []document{full, strict, skipped, persistent} {
		require(r["source_commit"] == sourceCommit, "mixed upstream revision")
		require(r["backend"] == backend, "mixed object backend")
		require(reflect.DeepEqual(r["runner_id"], full["runner_id"]) && reflect.DeepEqual(r["implementation"], full["implementation"]), "mixed execution batches")
		require(len(list(r["paired_outcome_differences"])) == 0, "cloud adds outcome differences")
	}
	require(reflect.DeepEqual(full["requested_suites"], []any{"sqlite-sqltests", "turso-sqltests"}) && str(full["test_filter"]) == "", "filtered corpus cannot establish full coverage")
	require(!yes(full["strict_supplement"]) && !yes(full["explicit_skips_overridden_for_diagnostics"]), "not an original corpus run")
	require(yes(strict["strict_supplement"]) && yes(skipped["explicit_skips_overridden_for_diagnostics"]) && yes(persistent["persistent_diagnostic_variant"]), "incorrect supplemental batch")
	for _, r := range []document{full, strict, persistent} {
		require(yes(r["all_executable_cases_passed"]), "executable case failed")
		for _, mode := range []string{"official", "remote-cloud"} {
			s := summaries(r, mode)
			for _, field := range []string{"failed", "errors", "snapshots_new", "snapshots_updated"} {
				require(number(s[field]) == 0, "failure or modified snapshot")
			}
		}
	}
	for _, mode := range []string{"official", "remote-cloud"} {
		base := summaries(full, mode)
		require(number(base["total"]) == fullCorpusCases, "full WAL corpus incomplete")
		require(number(summaries(strict, mode)["total"]) == strictCorpusCases, "STRICT corpus incomplete")
		require(number(summaries(persistent, mode)["total"]) == 4, "persistent diagnostics incomplete")
		diag := summaries(skipped, mode)
		require(number(diag["total"]) == 4 && number(diag["errors"]) == 0, "diagnostic batch incomplete")
		failures := list(doc(doc(skipped["runs"])[mode])["failures"])
		names := map[string]bool{}
		for _, v := range failures {
			c := doc(v)
			names[str(c["name"])] = true
			require(strings.Contains(str(c["outcome"]), "cannot VACUUM an in-memory database"), "unexpected diagnostic failure")
		}
		require(len(failures) == 2 && reflect.DeepEqual(names, unsupported), "unexpected diagnostic cases")
		original, strictCases := cases(full, mode), cases(strict, mode)
		require(len(original) == fullCorpusCases && len(strictCases) == strictCorpusCases && len(cases(skipped, mode)) == 4 && len(cases(persistent, mode)) == 4, "JSONL coverage incomplete")
		expectedStrict := countCases(original, func(c document) bool {
			return yes(c["skipped"]) && strings.Contains(str(c["outcome"]), "requires strict")
		})
		require(reflect.DeepEqual(expectedStrict, countCases(strictCases, func(document) bool { return true })), "STRICT identities differ")
		total := countCases(original, func(document) bool { return true })
		passed := countCases(original, func(c document) bool { return yes(c["passed"]) })
		for _, r := range []document{strict, skipped} {
			for k, n := range countCases(cases(r, mode), func(c document) bool { return yes(c["passed"]) }) {
				passed[k] += n
			}
		}
		remaining := 0
		for k, n := range passed {
			require(n <= total[k], "extra or duplicated passing coverage")
		}
		remainingNames := map[string]bool{}
		for k, n := range total {
			if n > passed[k] {
				remaining += n - passed[k]
				remainingNames[k.Name] = true
			}
		}
		require(remaining == 2 && reflect.DeepEqual(remainingNames, unsupported), "missing coverage")
	}
	safety := document{}
	for category, n := range map[string]int{"archive": 24, "cold": 23} {
		r := readCategory(root, category, backend)
		require(r["source_commit"] == sourceCommit, "mixed safety suite revision")
		require(yes(r["all_passed"]) && len(doc(r["tests"])) == n, "incomplete safety suite")
		for _, v := range doc(r["tests"]) {
			require(yes(doc(v)["pass"]), "failed safety test")
		}
		safety[category] = n
	}
	base := summaries(full, "remote-cloud")
	evidence := document{"full": full["evidence"], "strict": strict["evidence"], "skipped": skipped["evidence"], "persistent": persistent["evidence"]}
	out := document{"backend": backend, "source_commit": sourceCommit, "source_release": "v0.8.2", "full_corpus": base, "strict_supplement_passed": strictCorpusCases, "originally_skipped_diagnostics": document{"passed": 2, "unsupported_memory_vacuum": []string{"gencol_vacuum_virtual", "gencol_vacuum_virtual_with_index"}}, "persistent_diagnostic_variants_passed": 4, "original_cases_passed_across_batches": number(base["passed"]) + strictCorpusCases + 2, "raw_original_zero_skip_pass": false, "supported_persistent_path_coverage_pass": true, "additional_cloud_failures": 0, "safety_tests": safety, "operations": doc(full["cloud_stats"])["operations"], "evidence": evidence, "scope": "fixed upstream ordinary WAL SQL corpus, matrices, EXPLAIN and annotated SQLite cross-checks; no distributed failover"}
	write(filepath.Join(root, "evidence", "cloud-acceptance-"+backend+"-latest.json"), out)
	fmt.Println(string(must(json.MarshalIndent(out, "", "  "))))
	return 0
}
func run() int {
	require(len(os.Args) > 1, "usage: verify official|summary [options] [suites]")
	root := rootDir()
	switch os.Args[1] {
	case "official":
		return official(root, os.Args[2:])
	case "summary":
		return summary(root, os.Args[2:])
	default:
		panic("unknown verification command")
	}
}
func main() {
	code, err := ps.Guard(run)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(code)
}
