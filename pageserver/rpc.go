package pageserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Service interface {
	Handle(Request, []byte) (Document, []byte, error)
}

func Serve(listener net.Listener, service Service) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go serveConnection(conn, service)
	}
}
func readHeader(reader *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > limit {
			return nil, fmt.Errorf("RPC header exceeds budget")
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		return line, err
	}
}
func serveConnection(conn net.Conn, service Service) {
	defer conn.Close()
	if store, ok := service.(*Store); ok {
		store.mu.Lock()
		store.Connections++
		store.mu.Unlock()
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	reader := bufio.NewReaderSize(conn, 65536)
	writer := bufio.NewWriter(conn)
	for {
		limit := 65536
		if _, ok := service.(*Controller); ok {
			limit = 16 * 1024 * 1024
		}
		line, err := readHeader(reader, limit)
		if err != nil {
			return
		}
		var req Request
		if err = json.Unmarshal(line, &req); err != nil {
			return
		}
		if req.Extra == nil {
			req.Extra = Document{}
		}
		if req.Len < 0 || req.Len > 16*1024*1024 {
			return
		}
		var payload []byte
		if req.Op == "write" {
			payload = make([]byte, req.Len)
			if _, err = io.ReadFull(reader, payload); err != nil {
				return
			}
		}
		reply, body, err := service.Handle(req, payload)
		if err != nil {
			kind := "OSError"
			if e, ok := err.(operationError); ok {
				kind = e.Kind
			}
			reply = Document{"error": err.Error(), "error_kind": kind}
			body = nil
		}
		if reply == nil {
			reply = Document{}
		}
		reply["len"] = len(body)
		if _, err = writer.Write(append(encode(reply), '\n')); err != nil {
			return
		}
		if _, err = writer.Write(body); err != nil {
			return
		}
		if err = writer.Flush(); err != nil {
			return
		}
	}
}
func Listen(port int64) net.Listener {
	return must(net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port)))
}
func Port(l net.Listener) int { return l.Addr().(*net.TCPAddr).Port }

type CloudRouter struct {
	Root                    string
	Storage                 *ObjectStorage
	mu                      sync.Mutex
	stores                  map[string]*Store
	archives                map[string]*Archive
	retired                 map[string]bool
	known                   sync.Map
	completed, hits, misses int64
	counts                  map[string]int64
}

func NewCloudRouter(root string, storage *ObjectStorage) *CloudRouter {
	check(os.MkdirAll(root, 0700))
	return &CloudRouter{Root: root, Storage: storage, stores: map[string]*Store{}, archives: map[string]*Archive{}, retired: map[string]bool{}, counts: map[string]int64{}}
}
func (r *CloudRouter) Handle(req Request, payload []byte) (Document, []byte, error) {
	type result struct {
		r Document
		b []byte
	}
	out, err := Guard(func() result { a, b := r.handle(req, payload); return result{a, b} })
	return out.r, out.b, err
}
func (r *CloudRouter) handle(req Request, payload []byte) (Document, []byte) {
	if req.Path == "control" {
		r.mu.Lock()
		defer r.mu.Unlock()
		rpc := Document{}
		for k, v := range r.counts {
			rpc[k] = v
		}
		return Document{"completed": r.completed, "operations": r.Storage.Metrics(), "rpc": rpc, "hits": r.hits, "misses": r.misses, "active": len(r.stores)}, nil
	}
	namespace, relative, ok := strings.Cut(req.Path, "/")
	require(ok && strings.HasPrefix(namespace, "case-"), "invalid case namespace")
	_, err := strconv.ParseUint(strings.TrimPrefix(namespace, "case-"), 10, 64)
	check(err)
	r.mu.Lock()
	var s *Store
	func() {
		defer r.mu.Unlock()
		for old := range r.retired {
			if !exists(filepath.Join(r.Root, old, "live.db")) {
				check(os.RemoveAll(filepath.Join(r.Root, old)))
				delete(r.retired, old)
			}
		}
		s = r.stores[namespace]
		if s == nil {
			s = NewStore(filepath.Join(r.Root, namespace), false, false)
			r.stores[namespace] = s
		}
		r.counts[req.Op]++
	}()
	req.Path = relative
	method := str(req.Extra["method"])
	if req.Op == "admin" && method == "cloud_start" {
		aux := Document{}
		fixtures := filepath.Join(r.Root, "database")
		if exists(fixtures) {
			for _, file := range must(os.ReadDir(fixtures)) {
				if filepath.Ext(file.Name()) == ".db" {
					aux["database/"+file.Name()] = s.PutObject(readFile(filepath.Join(fixtures, file.Name())), "db")
				}
			}
		}
		atomicWrite(filepath.Join(s.Root, "auxiliary.json"), encode(aux))
		archive := NewArchive(CaseStorage(r.Storage, namespace, &r.known), namespace, filepath.Join(s.Root, "coordinator"))
		archive.Flush(map[string]string{"main": s.Root}, nil)
		_, manifest := archive.Load(0)
		cold := NewColdStore(s.Root, archive, manifest, doc(doc(manifest["databases"])["main"]), filepath.Join(r.Root, "cache"))
		check(os.RemoveAll(filepath.Join(s.Root, "objects")))
		r.mu.Lock()
		r.archives[namespace], r.stores[namespace] = archive, cold
		r.mu.Unlock()
		return Document{}, nil
	}
	if req.Op == "admin" && method == "cloud_export" {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.Export()
		return Document{}, nil
	}
	if req.Op == "admin" && method == "cloud_release" {
		r.mu.Lock()
		defer r.mu.Unlock()
		blockCache.Lock()
		r.hits += s.Reader.Hits
		r.misses += s.Reader.Misses
		blockCache.Unlock()
		r.completed++
		delete(r.stores, namespace)
		r.archives[namespace].Close()
		delete(r.archives, namespace)
		r.retired[namespace] = true
		return Document{}, nil
	}
	a, b, e := s.Handle(req, payload)
	check(e)
	return a, b
}

// Controller is a loopback test interface to Store, Archive and the SDK backend.
type Controller struct {
	mu        sync.Mutex
	storages  map[string]*ObjectStorage
	archives  map[string]*Archive
	stores    map[string]*Store
	listeners map[string]net.Listener
	plans     map[string]Compaction
	overlays  map[string]*Overlay
}

func NewController() *Controller {
	return &Controller{storages: map[string]*ObjectStorage{}, archives: map[string]*Archive{}, stores: map[string]*Store{}, listeners: map[string]net.Listener{}, plans: map[string]Compaction{}, overlays: map[string]*Overlay{}}
}
func b64(raw []byte) string { return base64.StdEncoding.EncodeToString(raw) }
func unb64(v any) []byte    { return must(base64.StdEncoding.DecodeString(str(v))) }

type unsafeCreateBackend struct{ Backend }

func (b unsafeCreateBackend) Write(k string, raw []byte, create, hint bool) (bool, error) {
	return b.Backend.Write(k, raw, false, hint)
}
func (c *Controller) Handle(req Request, payload []byte) (Document, []byte, error) {
	type result struct {
		r Document
		b []byte
	}
	out, err := Guard(func() result {
		if req.Path != "control" {
			id, relative, ok := strings.Cut(req.Path, "/")
			require(ok, "invalid test store route")
			c.mu.Lock()
			s := c.stores[id]
			c.mu.Unlock()
			require(s != nil, "test store missing")
			req.Path = relative
			a, b, e := s.Handle(req, payload)
			check(e)
			return result{a, b}
		}
		return result{c.control(req.Extra), nil}
	})
	return out.r, out.b, err
}
func (c *Controller) control(extra Document) Document {
	c.mu.Lock()
	storage := c.storages[str(extra["storage"])]
	archive := c.archives[str(extra["archive"])]
	store := c.stores[str(extra["store"])]
	c.mu.Unlock()
	switch str(extra["action"]) {
	case "storage_new":
		s := Connect(str(extra["service"]), str(extra["endpoint"]), str(extra["bucket"]), str(extra["prefix"]), str(extra["region"]))
		if flag(extra["unsafe"]) {
			s.Backend = unsafeCreateBackend{s.Backend}
		}
		id := randomID()
		c.mu.Lock()
		c.storages[id] = s
		c.mu.Unlock()
		return Document{"id": id, "identity": s.Identity}
	case "storage_identity":
		storage.Identity = extra["value"]
	case "storage_metrics":
		return storage.Metrics()
	case "storage_probe":
		storage.Probe()
	case "storage_read":
		raw := storage.Read(str(extra["key"]))
		return Document{"data": b64(raw), "missing": raw == nil}
	case "storage_range":
		return Document{"data": b64(storage.ReadRange(str(extra["key"]), num(extra["offset"]), num(extra["size"])))}
	case "storage_create":
		return Document{"created": storage.Create(str(extra["key"]), unb64(extra["data"]))}
	case "storage_put":
		storage.Put(str(extra["key"]), unb64(extra["data"]))
	case "storage_hint":
		storage.Hint(str(extra["key"]), unb64(extra["data"]))
	case "storage_delete":
		storage.Delete(str(extra["key"]))
	case "storage_fault":
		storage.mu.Lock()
		storage.RangeFault = str(extra["range"])
		storage.PutBarrier = str(extra["barrier"])
		storage.mu.Unlock()
	case "archive_new":
		a := NewArchive(storage, str(extra["group"]), str(extra["state"]))
		id := randomID()
		c.mu.Lock()
		c.archives[id] = a
		c.mu.Unlock()
		return Document{"id": id}
	case "archive_close":
		archive.Close()
		c.mu.Lock()
		delete(c.archives, str(extra["archive"]))
		c.mu.Unlock()
	case "archive_config":
		archive.Fault = str(extra["fault"])
		if extra["max_catchup"] != nil {
			archive.MaxCatchup = num(extra["max_catchup"])
		}
	case "archive_load":
		head, manifest := archive.Load(num(extra["revision"]))
		return Document{"head": head, "manifest": manifest}
	case "archive_flush":
		roots := map[string]string{}
		readers := map[string]*Store{}
		for k, v := range doc(extra["roots"]) {
			roots[k] = str(v)
		}
		c.mu.Lock()
		for k, v := range doc(extra["readers"]) {
			readers[k] = c.stores[str(v)]
		}
		c.mu.Unlock()
		return archive.Flush(roots, readers)
	case "archive_restore":
		return archive.Restore(str(extra["destination"]), num(extra["revision"]))
	case "archive_compact":
		return archive.Compact()
	case "archive_compact_prepare":
		plan := archive.PrepareCompaction()
		id := randomID()
		c.mu.Lock()
		c.plans[id] = plan
		c.mu.Unlock()
		return Document{"id": id}
	case "archive_compact_finish":
		id := str(extra["plan"])
		c.mu.Lock()
		plan, ok := c.plans[id]
		delete(c.plans, id)
		c.mu.Unlock()
		require(ok, "compaction plan missing")
		return archive.FinishCompaction(plan)
	case "archive_chunk":
		return Document{"data": b64(archive.ReadChunk(str(extra["sha"]), doc(extra["extent"])))}
	case "snapshot":
		snapshot := TakeSnapshot(str(extra["root"]), store)
		objects := Document{}
		for k, v := range snapshot.Objects {
			objects[k] = b64(v)
		}
		return Document{"manifest": snapshot.Manifest, "objects": objects}
	case "store_new":
		s := NewColdStore(str(extra["root"]), archive, doc(extra["manifest"]), doc(extra["database"]), str(extra["cache"]))
		id := randomID()
		c.mu.Lock()
		c.stores[id] = s
		listener := Listen(0)
		c.listeners[id] = listener
		c.mu.Unlock()
		go Serve(listener, s)
		return Document{"id": id, "port": Port(listener)}
	case "store_state":
		store.mu.Lock()
		defer store.mu.Unlock()
		state := store.ColdState()
		views := Document{}
		for name, v := range store.Views {
			views[name] = Document{"dbsize": v.DBSize, "page_size": v.PageSize, "generation": v.Generation, "frame": v.Frame, "base": v.Base}
		}
		state["views"] = views
		state["view_names"] = store.ViewNames
		return state
	case "store_range":
		store.mu.Lock()
		defer store.mu.Unlock()
		return Document{"data": b64(store.ObjectRange(str(extra["name"]), num(extra["offset"]), num(extra["size"])))}
	case "store_bytes":
		store.mu.Lock()
		defer store.mu.Unlock()
		return Document{"data": b64(store.ObjectBytes(str(extra["name"])))}
	case "store_size":
		store.mu.Lock()
		defer store.mu.Unlock()
		return Document{"size": store.ObjectSize(str(extra["name"]))}
	case "reader_chunk":
		return Document{"data": b64(store.Reader.Chunk(str(extra["sha"])))}
	case "reader_budget":
		blockCache.Lock()
		store.Reader.Budget = num(extra["size"])
		blockCache.Unlock()
	case "store_close":
		c.mu.Lock()
		id := str(extra["store"])
		if listener := c.listeners[id]; listener != nil {
			listener.Close()
			delete(c.listeners, id)
		}
		delete(c.stores, id)
		c.mu.Unlock()
	case "overlay_new":
		o := NewOverlay(store, str(extra["name"]), str(extra["backing"]), num(extra["size"]))
		id := randomID()
		c.mu.Lock()
		c.overlays[id] = o
		c.mu.Unlock()
		return Document{"id": id}
	case "overlay_read", "overlay_truncate":
		c.mu.Lock()
		o := c.overlays[str(extra["overlay"])]
		c.mu.Unlock()
		require(o != nil, "test overlay missing")
		if str(extra["action"]) == "overlay_read" {
			return Document{"data": b64(o.Read(num(extra["offset"]), num(extra["size"])))}
		}
		o.Truncated(num(extra["size"]))
	default:
		fail("ValueError", "unknown test control action")
	}
	return Document{}
}
