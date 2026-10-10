package pageserver

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These suites use the real fixed-revision Turso probe through the public TCP
// protocol. External object storage is opt-in and uses an isolated run prefix.
type suite struct {
	t                  *testing.T
	root, run, backend string
	report, tests      Document
}

func integration(t *testing.T, category string, fn func(*suite)) {
	t.Helper()
	if os.Getenv("PAGESERVER_INTEGRATION") != "1" {
		t.Skip("run ./run.sh or ./run-cloud-e2e.sh for engine integration")
	}
	root := must(filepath.Abs(".."))
	if v := os.Getenv("PAGESERVER_TEST_ROOT"); v != "" {
		root = v
	}
	backend := "memory"
	if os.Getenv("PAGESERVER_TEST_OSS") == "1" {
		backend = "oss"
	}
	run := filepath.Join(root, "evidence", category+"-"+time.Now().Format("20060102-150405")+"-"+randomID())
	check(os.MkdirAll(run, 0700))
	s := &suite{t: t, root: root, run: run, backend: backend, tests: Document{}}
	s.report = Document{"source_commit": "8cc059bd951551ae4b9dc31c01146a24b3e87a51", "backend": backend, "evidence": run, "run_directory": run, "tests": s.tests, "scope": "real Turso ordinary WAL, TCP page I/O, immutable publication, archive and cold recovery; no Keeper or distributed failover"}
	defer func() {
		s.report["all_passed"] = !t.Failed()
		raw := must(json.MarshalIndent(s.report, "", "  "))
		check(os.WriteFile(filepath.Join(run, "report.json"), raw, 0600))
		name := category + "-" + backend + "-latest.json"
		if category == "wal" {
			name = "latest.json"
		}
		if category == "generation" {
			name = "generation-latest.json"
		}
		check(os.WriteFile(filepath.Join(root, "evidence", name), raw, 0600))
	}()
	_, err := Guard(func() bool { fn(s); return true })
	if err != nil {
		s.report["error"] = err.Error()
		t.Error(err)
	}
	expected := map[string]int{"wal": 10, "negative": 2, "generation": 5, "archive": 24, "cold": 23}[category]
	if len(s.tests) != expected {
		t.Errorf("incomplete %s suite: %d/%d checks", category, len(s.tests), expected)
	}
}
func (s *suite) passed(name string, details ...Document) {
	d := Document{"pass": true}
	for _, extra := range details {
		for k, v := range extra {
			d[k] = v
		}
	}
	s.tests[name] = d
}
func (s *suite) storage(category string) *ObjectStorage {
	prefix := "test-runs/" + filepath.Base(s.run) + "/" + category
	if s.backend == "oss" {
		prefix = StorageRunPrefix(filepath.Base(s.run) + "/" + category)
		s.report["prefix"] = prefix
		return ConnectWithMode("s3", os.Getenv("S3_ENDPOINT"), os.Getenv("S3_BUCKET"), prefix, StorageRegion(), "oss-forbid-overwrite")
	}
	s.report["prefix"] = prefix
	return Connect("memory", "", "", prefix, "")
}
func expectFailure(fn func(), text string) {
	_, err := Guard(func() bool { fn(); return true })
	require(err != nil, "expected rejection")
	require(text == "" || strings.Contains(err.Error(), text), fmt.Sprintf("wanted error %q, got %v", text, err))
}
func treeCopy(src, dst string) {
	check(filepath.WalkDir(src, func(p string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := must(filepath.Rel(src, p))
		target := filepath.Join(dst, rel)
		if e.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		return os.WriteFile(target, readFile(p), 0600)
	}))
}

type compute struct {
	cmd     *exec.Cmd
	input   io.WriteCloser
	output  *bufio.Reader
	stderr  *os.File
	stopped bool
}

func (p *compute) stop() {
	if !p.stopped {
		p.stopped = true
		p.cmd.Process.Kill()
		p.cmd.Wait()
		p.input.Close()
		p.stderr.Close()
	}
}
func (p *compute) send(sql string) { _, err := io.WriteString(p.input, sql+"\n"); check(err) }
func (p *compute) receive() Document {
	ch := make(chan Document, 1)
	errors := make(chan error, 1)
	go func() {
		line, err := p.output.ReadBytes('\n')
		if err != nil {
			errors <- err
			return
		}
		var r Document
		err = json.Unmarshal(line, &r)
		if err != nil {
			errors <- err
			return
		}
		ch <- r
	}()
	select {
	case r := <-ch:
		return r
	case err := <-errors:
		panic(err)
	case <-time.After(30 * time.Second):
		p.stop()
		panic(fmt.Errorf("compute response timeout"))
	}
}

type engineLab struct {
	s                  *suite
	directory, root    string
	port               int
	server             *exec.Cmd
	serverErr          *os.File
	listener           net.Listener
	store              *Store
	archive            *Archive
	manifest, database Document
	computes           []*compute
	log                []any
	negative           string
}

func newLab(s *suite, root string) *engineLab {
	if root == "" {
		root = filepath.Join(s.run, randomID(), "storage")
	}
	l := &engineLab{s: s, directory: filepath.Dir(root), root: root}
	check(os.MkdirAll(l.directory, 0700))
	l.start()
	s.t.Cleanup(l.close)
	return l
}
func coldLab(s *suite, root string, a *Archive, m, db Document) *engineLab {
	l := &engineLab{s: s, directory: root, root: root, archive: a, manifest: m, database: db}
	l.start()
	s.t.Cleanup(l.close)
	return l
}
func (l *engineLab) start() {
	if l.archive != nil {
		l.store = NewColdStore(l.root, l.archive, l.manifest, l.database, "")
		l.listener = Listen(0)
		l.port = Port(l.listener)
		go Serve(l.listener, l.store)
		return
	}
	args := []string{"server", "--root", l.root}
	if l.negative != "" {
		args = append(args, "--unsafe-"+l.negative)
	}
	l.server = exec.Command(filepath.Join(l.s.root, "bin/pageserver"), args...)
	l.serverErr = must(os.OpenFile(filepath.Join(l.directory, "server.stderr"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600))
	l.server.Stderr = l.serverErr
	out := must(l.server.StdoutPipe())
	check(l.server.Start())
	var ready Document
	check(json.Unmarshal(must(bufio.NewReader(out).ReadBytes('\n')), &ready))
	l.port = int(num(ready["port"]))
}
func (l *engineLab) shutdown() {
	for _, p := range l.computes {
		p.stop()
	}
	if l.server != nil {
		l.server.Process.Kill()
		l.server.Wait()
		l.server = nil
		l.serverErr.Close()
	}
	if l.listener != nil {
		l.listener.Close()
		l.listener = nil
	}
}
func (l *engineLab) close() {
	l.shutdown()
	if exists(l.directory) {
		raw := must(json.MarshalIndent(l.log, "", "  "))
		os.WriteFile(filepath.Join(l.directory, "sql.json"), raw, 0600)
	}
}
func (l *engineLab) restart() *compute { l.shutdown(); l.start(); return l.open("live.db", false) }
func (l *engineLab) open(path string, readonly bool) *compute {
	args := []string{fmt.Sprintf("127.0.0.1:%d", l.port), path}
	if readonly {
		args = append(args, "readonly")
	}
	cmd := exec.Command(filepath.Join(l.s.root, "target/debug/turso-pageserver-probe"), args...)
	p := &compute{cmd: cmd, input: must(cmd.StdinPipe()), output: bufio.NewReader(must(cmd.StdoutPipe())), stderr: must(os.OpenFile(filepath.Join(l.directory, "compute.stderr"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600))}
	cmd.Stderr = p.stderr
	check(cmd.Start())
	l.computes = append(l.computes, p)
	require(flag(p.receive()["ready"]), "compute not ready")
	return p
}
func (l *engineLab) sql(p *compute, sql string, ok ...bool) Document {
	p.send(sql)
	r := p.receive()
	expected := true
	if len(ok) > 0 {
		expected = ok[0]
	}
	require(flag(r["ok"]) == expected, fmt.Sprintf("SQL %.150s: %v", sql, r))
	l.log = append(l.log, Document{"sql": sql, "reply": r})
	return r
}
func (l *engineLab) scalar(p *compute, sql string) string {
	rows := list(l.sql(p, sql)["rows"])
	require(len(rows) == 1 && len(list(rows[0])) == 1, "not scalar")
	v := str(list(rows[0])[0])
	if strings.HasPrefix(v, "Numeric(") {
		v = v[8 : len(v)-1]
	}
	return v
}
func (l *engineLab) rows(p *compute, sql string) any { return l.sql(p, sql)["rows"] }
func (l *engineLab) assertScalar(p *compute, sql, want string) {
	got := l.scalar(p, sql)
	require(got == want, fmt.Sprintf("%s: want %s, got %s", sql, want, got))
}
func (l *engineLab) count(p *compute, n int) {
	l.assertScalar(p, "SELECT count(*) FROM t", fmt.Sprintf("Integer(%d)", n))
}
func (l *engineLab) integrity(p *compute) {
	require(strings.Contains(l.scalar(p, "PRAGMA integrity_check"), `value: "ok"`), "integrity_check failed")
}
func (l *engineLab) request(r Request, payload []byte) Document {
	c := must(net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", l.port), 15*time.Second))
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second))
	raw := must(json.Marshal(r))
	_, err := c.Write(append(append(raw, '\n'), payload...))
	check(err)
	var reply Document
	check(json.Unmarshal(must(bufio.NewReader(c).ReadBytes('\n')), &reply))
	return reply
}
func (l *engineLab) admin(method string, extra ...Document) Document {
	e := Document{"method": method}
	for _, d := range extra {
		for k, v := range d {
			e[k] = v
		}
	}
	r := l.request(Request{Op: "admin", Path: "control", Extra: e}, nil)
	require(r["error"] == nil, fmt.Sprint(r))
	return r
}
func (l *engineLab) latest() string {
	views := list(l.admin("stats")["views"])
	return str(views[len(views)-1])
}
func (l *engineLab) full(p *compute) {
	l.sql(p, "PRAGMA synchronous=FULL")
	l.sql(p, "PRAGMA wal_autocheckpoint=0")
}
func initialized(s *suite) (*engineLab, *compute) {
	l := newLab(s, "")
	p := l.open("live.db", false)
	l.full(p)
	l.sql(p, "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")
	vals := []string{}
	for i := 0; i < 64; i++ {
		vals = append(vals, fmt.Sprintf("(%d,'%04d%s')", i, i, strings.Repeat("x", 1000)))
	}
	l.sql(p, "INSERT INTO t VALUES "+strings.Join(vals, ","))
	l.sql(p, "PRAGMA wal_checkpoint(TRUNCATE)")
	l.admin("baseline")
	for i := 64; i < 80; i++ {
		l.sql(p, fmt.Sprintf("INSERT INTO t VALUES(%d,'archived-%d')", i, i))
	}
	return l, p
}
func checkDatabase(s *suite, root string, n int) {
	l := newLab(s, root)
	p := l.open("live.db", true)
	l.count(p, n)
	l.integrity(p)
	l.close()
}
func waitPath(path string) {
	deadline := time.Now().Add(15 * time.Second)
	for !exists(path) {
		require(time.Now().Before(deadline), "barrier timeout")
		time.Sleep(5 * time.Millisecond)
	}
}
func readStats(l *engineLab, path string) Document {
	reads, bytes := int64(0), int64(0)
	pages := Document{}
	for _, v := range list(l.admin("stats")["events"]) {
		e := doc(v)
		if str(e["op"]) == "read" && str(e["path"]) == path {
			reads++
			bytes += num(e["returned_bytes"])
			pages[fmt.Sprint(num(e["pos"])/4096+1)] = true
		}
	}
	return Document{"requests": reads, "bytes": bytes, "pages": sortedKeys(pages)}
}
func syncs(l *engineLab, path string) int {
	n := 0
	for _, v := range list(l.admin("stats")["events"]) {
		e := doc(v)
		if e["op"] == "sync" && e["path"] == path {
			n++
		}
	}
	return n
}
func assertBytes(a, b []byte) { require(bytes.Equal(a, b), "byte content differs") }
