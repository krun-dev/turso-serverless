package pageserver

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Request struct {
	Op    string   `json:"op"`
	Path  string   `json:"path"`
	Pos   int64    `json:"pos"`
	Len   int64    `json:"len"`
	Extra Document `json:"extra"`
}
type View struct {
	Base                                string
	PageSize, DBSize, Generation, Frame int64
	Pages                               map[int64]int64
	WAL                                 string
}
type Store struct {
	mu                           sync.Mutex
	Root                         string
	Generations                  []any
	Views                        map[string]*View
	ViewNames                    []string
	Events                       []any
	Connections                  int64
	Epoch                        any
	SyncedDB                     []byte
	Synced                       bool
	Fault, FaultPath             string
	ReadDelay                    int64
	UnsafeLatest, UnsafeRecovery bool
	Archive                      *Archive
	CloudManifest, Database      Document
	Reader                       *ChunkReader
	Overlays                     map[string]*Overlay
	Auxiliary                    Document
	localVerified                map[string]string
}

func NewStore(root string, unsafeLatest, unsafeRecovery bool) *Store {
	check(os.MkdirAll(root, 0700))
	s := &Store{Root: root, FaultPath: "live.db-wal", UnsafeLatest: unsafeLatest, UnsafeRecovery: unsafeRecovery, Views: map[string]*View{}, localVerified: map[string]string{}, Events: []any{}}
	if exists(filepath.Join(root, "manifest.json")) {
		m := doc(decode(readFile(filepath.Join(root, "manifest.json"))))
		require(num(m["format"]) == 1, "unknown manifest format")
		s.Generations = list(m["generations"])
	} else if exists(filepath.Join(root, "baseline.db")) {
		wal := []byte{}
		if exists(filepath.Join(root, "acked.wal")) {
			wal = readFile(filepath.Join(root, "acked.wal"))
		}
		s.Publish([]any{s.Generation(0, readFile(filepath.Join(root, "baseline.db")), wal, nil)})
	}
	s.rebuild()
	if len(s.Generations) > 0 && !unsafeRecovery {
		g := doc(s.Generations[len(s.Generations)-1])
		atomicWrite(filepath.Join(root, "live.db"), s.ObjectBytes(str(g["base"])))
		atomicWrite(filepath.Join(root, "live.db-wal"), s.ObjectBytes(str(g["wal"])))
	}
	if len(s.Generations) > 0 {
		s.refreshMirrors()
	}
	return s
}
func (s *Store) ObjectSize(name string) int64 {
	p := filepath.Join(s.Root, safePath(name))
	if exists(p) {
		return must(os.Stat(p)).Size()
	}
	require(s.Reader != nil, "publication object missing")
	return num(doc(doc(s.Database["objects"])[name])["size"])
}
func (s *Store) ObjectBytes(name string) []byte {
	require(filepath.ToSlash(filepath.Dir(name)) == "objects", "invalid manifest object")
	p := filepath.Join(s.Root, name)
	var raw []byte
	if exists(p) {
		raw = readFile(p)
	} else {
		require(s.Reader != nil, "publication object missing")
		raw = s.ObjectRange(name, 0, s.ObjectSize(name))
	}
	require(digest(raw) == strings.TrimSuffix(filepath.Base(name), filepath.Ext(name)), "object checksum mismatch")
	return raw
}
func fileSignature(path string) string {
	info := must(os.Stat(path))
	return fmt.Sprintf("%d:%d", info.Size(), info.ModTime().UnixNano())
}
func (s *Store) ObjectRange(name string, pos, length int64) []byte {
	p := filepath.Join(s.Root, safePath(name))
	if exists(p) {
		if s.Reader != nil {
			signature := fileSignature(p)
			if s.localVerified[name] != signature {
				s.ObjectBytes(name)
				s.localVerified[name] = signature
			}
		}
		return fileRange(p, pos, length)
	}
	require(s.Reader != nil, "publication object missing")
	obj := doc(doc(s.Database["objects"])[name])
	require(str(obj["sha256"]) == strings.TrimSuffix(filepath.Base(name), filepath.Ext(name)), "object identity mismatch")
	return s.Reader.Read(obj, pos, length)
}
func (s *Store) PutObject(raw []byte, suffix string) string {
	p := filepath.Join(s.Root, "objects")
	if !exists(p) {
		check(os.MkdirAll(p, 0700))
		syncDir(s.Root)
	}
	name := "objects/" + digest(raw) + "." + suffix
	if !exists(filepath.Join(s.Root, name)) {
		atomicWrite(filepath.Join(s.Root, name), raw)
	} else {
		require(bytes.Equal(readFile(filepath.Join(s.Root, name)), raw), "immutable object mismatch")
	}
	return name
}
func position(g Document) Document {
	return Document{"generation": g["number"], "epoch": g["epoch"], "frame": g["commit_frame"]}
}
func (s *Store) Generation(number int64, base any, wal []byte, covers any) Document {
	parsed := ParseWAL(wal)
	frame := int64(0)
	var size any
	if len(parsed.Commits) > 0 {
		last := parsed.Commits[len(parsed.Commits)-1]
		frame = last.Frame
		size = last.Size
	}
	var name string
	switch b := base.(type) {
	case string:
		name = b
	case []byte:
		name = s.PutObject(b, "db")
	default:
		panic("invalid baseline")
	}
	g := Document{"number": number, "base": name, "wal": s.PutObject(wal, "wal"), "epoch": parsed.Epoch, "commit_frame": frame, "dbsize": size, "covers_position": covers}
	if s.ObjectSize(name) == 0 {
		ps := parsed.PageSize
		if ps == 0 {
			ps = 4096
		}
		g["page_size"] = ps
	}
	return g
}
func (s *Store) buildViews(generations []any) (map[string]*View, []string) {
	views := map[string]*View{}
	names := []string{}
	for index, v := range generations {
		g := doc(v)
		var cover any
		if index > 0 {
			cover = position(doc(generations[index-1]))
		}
		require(num(g["number"]) == int64(index) && equal(g["covers_position"], cover), "baseline coverage position mismatch")
		var idx Document
		if s.Reader != nil {
			for _, v := range list(s.Database["page_indexes"]) {
				candidate := doc(v)
				if equal(candidate["generation"], g) {
					idx = candidate
					break
				}
			}
		}
		if idx == nil {
			base := s.ObjectRange(str(g["base"]), 0, 32)
			wal := s.ObjectBytes(str(g["wal"]))
			idx = pageIndex(g, base, wal)
			idx["base_size"] = s.ObjectSize(str(g["base"]))
		}
		ps := num(idx["page_size"])
		require(ps >= 512 && ps <= 65536 && ps&(ps-1) == 0 && num(idx["base_size"])%ps == 0, "invalid baseline size")
		require(equal(idx["epoch"], g["epoch"]), "manifest WAL epoch mismatch")
		commits := list(idx["commits"])
		cf, ds := int64(0), any(nil)
		if len(commits) > 0 {
			last := doc(commits[len(commits)-1])
			cf = num(last["frame"])
			ds = last["dbsize"]
		}
		require(cf == num(g["commit_frame"]) && equal(ds, g["dbsize"]), "manifest commit boundary mismatch")
		prefix := "view-"
		if index > 0 {
			prefix = fmt.Sprintf("view-g%d-", index)
		}
		add := func(frame, size int64, pages map[int64]int64) {
			name := fmt.Sprintf("%s%d.db", prefix, frame)
			views[name] = &View{str(g["base"]), ps, size, int64(index), frame, pages, str(g["wal"])}
			names = append(names, name)
		}
		add(0, num(idx["base_size"])/ps, map[int64]int64{})
		for _, v := range commits {
			c := doc(v)
			pages := map[int64]int64{}
			for pg, v := range doc(c["pages"]) {
				p := must(strconv.ParseInt(pg, 10, 64))
				off := num(v)
				require(p >= 1 && p <= num(c["dbsize"]) && off >= 56 && (off-56)%(ps+24) == 0 && off+ps <= 32+num(c["frame"])*(ps+24), "invalid page offset")
				pages[p] = off
			}
			add(num(c["frame"]), num(c["dbsize"]), pages)
		}
	}
	return views, names
}
func (s *Store) rebuild() {
	s.Views, s.ViewNames = s.buildViews(s.Generations)
	if len(s.Generations) > 0 {
		s.Epoch = doc(s.Generations[len(s.Generations)-1])["epoch"]
	}
}
func (s *Store) Publish(gens []any) {
	views, names := s.buildViews(gens)
	atomicWrite(filepath.Join(s.Root, "manifest.json"), encode(Document{"format": 1, "generations": gens}))
	s.Generations, s.Views, s.ViewNames = gens, views, names
	s.Epoch = doc(gens[len(gens)-1])["epoch"]
	s.refreshMirrors()
}
func (s *Store) refreshMirrors() {
	if s.Reader != nil {
		return
	}
	g := doc(s.Generations[len(s.Generations)-1])
	atomicWrite(filepath.Join(s.Root, "baseline.db"), s.ObjectBytes(str(g["base"])))
	atomicWrite(filepath.Join(s.Root, "acked.wal"), s.ObjectBytes(str(g["wal"])))
}
func (s *Store) ViewPage(v *View, page int64) []byte {
	if off, ok := v.Pages[page]; ok {
		return s.ObjectRange(v.WAL, off, v.PageSize)
	}
	return s.ObjectRange(v.Base, (page-1)*v.PageSize, v.PageSize)
}
func (s *Store) CheckpointBase() []byte {
	require(s.Synced, "WAL reset requires a synced DB image")
	v := s.Views[s.ViewNames[len(s.ViewNames)-1]]
	require(int64(len(s.SyncedDB)) == v.DBSize*v.PageSize, "checkpoint does not cover published database size")
	for page := int64(1); page <= v.DBSize; page++ {
		start := (page - 1) * v.PageSize
		require(bytes.Equal(s.SyncedDB[start:start+v.PageSize], s.ViewPage(v, page)), fmt.Sprintf("checkpoint does not cover published page %d", page))
	}
	return s.SyncedDB
}
func (s *Store) Handle(req Request, payload []byte) (Document, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type answer struct {
		reply Document
		body  []byte
	}
	a, err := Guard(func() answer { r, b := s.handle(req, payload); return answer{r, b} })
	return a.reply, a.body, err
}
func (s *Store) handle(req Request, payload []byte) (Document, []byte) {
	name := safePath(req.Path)
	require(req.Pos >= 0 && req.Pos < 1<<40 && req.Len >= 0 && req.Len <= 16*1024*1024, "invalid file range")
	p := filepath.Join(s.Root, name)
	if req.Op == "admin" {
		return s.admin(req), nil
	}
	started := time.Now()
	event := Document{"op": req.Op, "path": name, "pos": req.Pos, "len": req.Len}
	s.Events = append(s.Events, event)
	reply := Document{}
	body := []byte{}
	view := s.Views[name]
	if view != nil && s.UnsafeLatest {
		view = s.Views[s.ViewNames[len(s.ViewNames)-1]]
	}
	viewWAL := strings.HasSuffix(name, "-wal") && s.Views[strings.TrimSuffix(name, "-wal")] != nil
	if view != nil || viewWAL {
		require(req.Op == "open" || req.Op == "read" || req.Op == "size", "immutable version")
		size := int64(0)
		if view != nil {
			size = view.DBSize * view.PageSize
		}
		if req.Op == "size" {
			reply["size"] = size
		}
		if req.Op == "read" && req.Pos < size {
			end := min(size, req.Pos+req.Len)
			var joined []byte
			for page := req.Pos/view.PageSize + 1; page <= (end-1)/view.PageSize+1; page++ {
				raw := s.ViewPage(view, page)
				require(int64(len(raw)) == view.PageSize, "missing snapshot page")
				joined = append(joined, raw...)
			}
			start := req.Pos % view.PageSize
			body = joined[start : start+end-req.Pos]
		}
	} else {
		if name == "manifest.json" || strings.HasPrefix(name, "objects/") || name == "auxiliary.json" || name == "cold-bootstrap.json" || strings.HasPrefix(name, "cache/") || strings.HasPrefix(name, "coordinator/") {
			require(req.Op == "open" || req.Op == "read" || req.Op == "size", "publication objects are immutable")
			require(exists(p), "publication object does not exist")
		}
		overlay := s.Overlays[name]
		if req.Op == "open" && s.Reader != nil && overlay == nil && name != "manifest.json" && !strings.HasPrefix(name, "objects/") && name != "auxiliary.json" && name != "cold-bootstrap.json" {
			walBase := strings.TrimSuffix(name, "-wal")
			if flag(req.Extra["create"]) || (strings.HasSuffix(name, "-wal") && s.Overlays[walBase] != nil) {
				raw := []byte{}
				if exists(p) {
					raw = readFile(p)
				}
				backing := ""
				if len(raw) > 0 {
					suffix := "db"
					if strings.HasSuffix(name, "-wal") {
						suffix = "wal"
					}
					backing = s.PutObject(raw, suffix)
				}
				s.Overlays[name] = NewOverlay(s, name, backing, int64(len(raw)))
				return reply, body
			}
		}
		if overlay != nil && (req.Op == "open" || req.Op == "read" || req.Op == "size") {
			if req.Op == "read" {
				body = overlay.Read(req.Pos, req.Len)
			}
			if req.Op == "size" {
				reply["size"] = overlay.Size
			}
		} else {
			if overlay != nil && req.Op == "sync" {
				overlay.Materialize()
			}
			if overlay != nil && req.Op == "truncate" && req.Pos == 0 && name != "live.db-wal" && strings.HasSuffix(name, "-wal") && s.Overlays[strings.TrimSuffix(name, "-wal")] != nil {
				s.persistAuxiliary(name, true)
			}
			switch req.Op {
			case "open":
				if !exists(p) {
					require(flag(req.Extra["create"]), "database file does not exist")
					f := must(os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0600))
					check(f.Close())
				}
			case "read":
				body = fileRange(p, req.Pos, req.Len)
			case "write":
				require(int64(len(payload)) == req.Len, "short write payload")
				if name == "live.db" {
					s.Synced = false
				}
				f := must(os.OpenFile(p, os.O_RDWR, 0600))
				_, err := f.WriteAt(payload, req.Pos)
				f.Close()
				check(err)
				if overlay != nil {
					overlay.Wrote(req.Pos, req.Len)
				}
			case "size":
				reply["size"] = must(os.Stat(p)).Size()
			case "truncate":
				if name == "live.db" {
					s.Synced = false
				}
				if name == "live.db-wal" && req.Pos == 0 && len(s.Generations) > 0 && s.Epoch != nil {
					base := s.CheckpointBase()
					g := doc(s.Generations[len(s.Generations)-1])
					number := num(g["number"]) + 1
					s.Publish(append(append([]any{}, s.Generations...), s.Generation(number, base, []byte{}, position(g))))
					s.Synced = false
					event["generation_transition"] = number
				}
				check(os.Truncate(p, req.Pos))
				if overlay != nil {
					overlay.Truncated(req.Pos)
				}
			case "remove":
				if exists(p) {
					check(os.Remove(p))
				}
				if overlay != nil {
					delete(s.Overlays, name)
					delete(s.Auxiliary, name)
					atomicWrite(filepath.Join(s.Root, "auxiliary.json"), encode(s.Auxiliary))
				}
			case "sync":
				s.sync(name, event)
				if overlay != nil && name != "live.db" && name != "live.db-wal" {
					s.persistAuxiliary(name, false)
				}
			default:
				fail("ValueError", "unknown file operation")
			}
		}
	}
	if req.Op == "read" && s.ReadDelay > 0 {
		time.Sleep(time.Duration(s.ReadDelay) * time.Millisecond)
	}
	event["elapsed_us"] = time.Since(started).Microseconds()
	event["returned_bytes"] = len(body)
	return reply, body
}
func (s *Store) sync(name string, event Document) {
	p := filepath.Join(s.Root, name)
	fault := ""
	if name == s.FaultPath {
		fault = s.Fault
		s.Fault = ""
	}
	if fault != "" {
		event["fault"] = fault
	}
	if fault == "before" {
		fail("OSError", "injected failure before durable publication")
	}
	if fault == "delay" {
		atomicWrite(filepath.Join(s.Root, "barrier-entered"), []byte{})
		deadline := time.Now().Add(10 * time.Second)
		s.mu.Unlock()
		func() {
			defer s.mu.Lock()
			for !exists(filepath.Join(s.Root, "barrier-release")) {
				require(time.Now().Before(deadline), "barrier timeout")
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	f := must(os.Open(p))
	err := fsync(f)
	f.Close()
	check(err)
	if name == "live.db" && len(s.Generations) > 0 {
		s.SyncedDB = readFile(p)
		s.Synced = true
		g := doc(s.Generations[len(s.Generations)-1])
		if s.ObjectSize(str(g["base"])) == 0 && len(s.SyncedDB) > 0 {
			require(num(g["commit_frame"]) == 0, "empty baseline already has commits")
			number := num(g["number"]) + 1
			s.Publish(append(append([]any{}, s.Generations...), s.Generation(number, s.SyncedDB, []byte{}, position(g))))
			event["generation_transition"] = number
		}
	}
	if name == "live.db-wal" && len(s.Generations) > 0 {
		raw := readFile(p)
		parsed := ParseWAL(raw)
		g := doc(s.Generations[len(s.Generations)-1])
		var updated Document
		gens := append([]any{}, s.Generations...)
		if s.Epoch != nil && !equal(parsed.Epoch, s.Epoch) {
			require(parsed.Epoch != nil, "empty WAL requires a validated truncate transition")
			base := s.CheckpointBase()
			number := num(g["number"]) + 1
			updated = s.Generation(number, base, raw, position(g))
			gens = append(gens, updated)
			s.Synced = false
			event["generation_transition"] = number
		} else {
			updated = s.Generation(num(g["number"]), g["base"], raw, g["covers_position"])
			require(num(updated["commit_frame"]) >= num(g["commit_frame"]), "published WAL cannot shrink within a generation")
			if num(g["commit_frame"]) > 0 {
				old := s.ObjectBytes(str(g["wal"]))
				size := 32 + num(g["commit_frame"])*(parsed.PageSize+24)
				require(int64(len(raw)) >= size && int64(len(old)) >= size && bytes.Equal(raw[:size], old[:size]), "published WAL prefix cannot change")
			}
			gens[len(gens)-1] = updated
		}
		s.Publish(gens)
		event["published"] = len(parsed.Commits)
	}
	if fault == "after" {
		fail("OSError", "injected lost reply after durable publication")
	}
}
func (s *Store) admin(req Request) Document {
	method := str(req.Extra["method"])
	switch method {
	case "stats":
		return Document{"events": s.Events, "views": s.ViewNames, "epoch": s.Epoch, "generations": s.Generations, "connections": s.Connections}
	case "reset_stats":
		s.Events = []any{}
	case "fault":
		s.Fault = str(req.Extra["mode"])
		s.FaultPath = str(req.Extra["target"])
		if s.FaultPath == "" {
			s.FaultPath = "live.db-wal"
		}
	case "delay":
		s.ReadDelay = num(req.Extra["ms"])
	case "baseline":
		require(len(s.Generations) == 0, "baseline already initialized; use a covering checkpoint transition")
		s.Publish([]any{s.Generation(0, readFile(filepath.Join(s.Root, "live.db")), []byte{}, nil)})
		s.Synced = false
	case "reset_publication":
		for _, name := range []string{"manifest.json", "baseline.db", "acked.wal"} {
			os.Remove(filepath.Join(s.Root, name))
		}
		s.Generations = nil
		s.Views = map[string]*View{}
		s.ViewNames = []string{}
		s.Epoch = nil
		s.Synced = false
	case "cold_stats":
		require(s.Reader != nil, "not a cold store")
		return Document{"hits": s.Reader.Hits, "misses": s.Reader.Misses, "operations": s.Archive.Storage.Metrics()}
	default:
		fail("ValueError", "unknown administrative operation")
	}
	return Document{}
}
func sortedKeys(m Document) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
