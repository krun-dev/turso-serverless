package pageserver

import (
	"bytes"
	linked "container/list"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type cacheEntry struct {
	path, signature string
	raw             []byte
}

var blockCache = struct {
	sync.Mutex
	entries map[string]*linked.Element
	lru     *linked.List
	bytes   int64
}{entries: map[string]*linked.Element{}, lru: linked.New()}

const CacheBudget = 128 * 1024 * 1024

type ChunkReader struct {
	Archive              *Archive
	Manifest             Document
	Cache                string
	Budget, Hits, Misses int64
}

func NewChunkReader(archive *Archive, manifest Document, cache string) *ChunkReader {
	check(os.MkdirAll(cache, 0700))
	return &ChunkReader{Archive: archive, Manifest: manifest, Cache: cache, Budget: CacheBudget}
}
func rememberBlock(path, signature string, raw []byte) {
	if e := blockCache.entries[path]; e != nil {
		blockCache.bytes -= int64(len(e.Value.(cacheEntry).raw))
		blockCache.lru.Remove(e)
	}
	e := blockCache.lru.PushBack(cacheEntry{path, signature, raw})
	blockCache.entries[path] = e
	blockCache.bytes += int64(len(raw))
	for blockCache.bytes > CacheBudget {
		e := blockCache.lru.Front()
		entry := e.Value.(cacheEntry)
		delete(blockCache.entries, entry.path)
		blockCache.bytes -= int64(len(entry.raw))
		blockCache.lru.Remove(e)
	}
}
func (r *ChunkReader) Chunk(sha string) []byte {
	blockCache.Lock()
	defer blockCache.Unlock()
	require(len(sha) == 64 && !strings.ContainsAny(sha, "/\\"), "invalid cache digest")
	p := filepath.Join(r.Cache, sha)
	if exists(p) {
		signature := fileSignature(p)
		var raw []byte
		verified := false
		if e := blockCache.entries[p]; e != nil {
			entry := e.Value.(cacheEntry)
			if entry.signature == signature {
				raw = entry.raw
				verified = true
			}
		}
		if !verified {
			raw = readFile(p)
		}
		if verified || digest(raw) == sha {
			rememberBlock(p, signature, raw)
			r.Hits++
			return raw
		}
		check(os.Remove(p))
	}
	r.Misses++
	raw := r.Archive.ReadChunk(sha, doc(doc(r.Manifest["chunks"])[sha]))
	atomicWrite(p, raw)
	entries := must(os.ReadDir(r.Cache))
	type file struct {
		path        string
		size, mtime int64
	}
	files := []file{}
	total := int64(0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info := must(e.Info())
		files = append(files, file{filepath.Join(r.Cache, e.Name()), info.Size(), info.ModTime().UnixNano()})
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mtime < files[j].mtime })
	for _, f := range files {
		if total <= r.Budget {
			break
		}
		check(os.Remove(f.path))
		total -= f.size
	}
	return raw
}
func (r *ChunkReader) Read(obj Document, offset, length int64) []byte {
	require(offset >= 0 && length >= 0, "invalid object range")
	end := min(num(obj["size"]), offset+length)
	if offset >= end {
		return []byte{}
	}
	raw := make([]byte, 0, end-offset)
	chunks := list(obj["chunks"])
	for number := offset / ChunkSize; number <= (end-1)/ChunkSize; number++ {
		require(number < int64(len(chunks)), "missing object chunk")
		block := r.Chunk(str(chunks[number]))
		expected := min(int64(ChunkSize), num(obj["size"])-number*ChunkSize)
		require(int64(len(block)) == expected, "chunk does not cover object range")
		start, stop := max(offset, number*ChunkSize)-number*ChunkSize, min(end, (number+1)*ChunkSize)-number*ChunkSize
		raw = append(raw, block[start:stop]...)
	}
	require(int64(len(raw)) == end-offset, "missing object bytes")
	return raw
}

type Range struct{ Start, End int64 }
type Overlay struct {
	Store             *Store
	Name, Backing     string
	Size, BackingSize int64
	Ranges            []Range
}

func NewOverlay(store *Store, name, backing string, size int64) *Overlay {
	p := filepath.Join(store.Root, safePath(name))
	check(os.MkdirAll(filepath.Dir(p), 0700))
	f := must(os.OpenFile(p, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600))
	err := f.Truncate(size)
	f.Close()
	check(err)
	return &Overlay{Store: store, Name: name, Backing: backing, Size: size, BackingSize: size}
}
func (o *Overlay) Read(offset, length int64) []byte {
	end := min(o.Size, offset+length)
	if offset >= end {
		return []byte{}
	}
	raw := make([]byte, end-offset)
	boundaries := map[int64]bool{offset: true, end: true}
	for _, r := range o.Ranges {
		if r.Start < end && r.End > offset {
			boundaries[max(r.Start, offset)] = true
			boundaries[min(r.End, end)] = true
		}
	}
	cuts := []int64{}
	for p := range boundaries {
		cuts = append(cuts, p)
	}
	sort.Slice(cuts, func(i, j int) bool { return cuts[i] < cuts[j] })
	f := must(os.Open(filepath.Join(o.Store.Root, o.Name)))
	defer f.Close()
	for index := 0; index < len(cuts)-1; index++ {
		start, stop := cuts[index], cuts[index+1]
		dirty := false
		for _, r := range o.Ranges {
			if r.Start <= start && stop <= r.End {
				dirty = true
				break
			}
		}
		if dirty {
			_, err := f.ReadAt(raw[start-offset:stop-offset], start)
			check(err)
		} else if o.Backing != "" && start < o.BackingSize {
			available := min(stop, o.BackingSize) - start
			block := o.Store.ObjectRange(o.Backing, start, available)
			require(int64(len(block)) == available, "missing backing range")
			copy(raw[start-offset:], block)
		}
	}
	return raw
}
func (o *Overlay) Wrote(offset, length int64) {
	o.Size = max(o.Size, offset+length)
	ranges := append(append([]Range{}, o.Ranges...), Range{offset, offset + length})
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Start < ranges[j].Start })
	merged := []Range{}
	for _, r := range ranges {
		if len(merged) > 0 && r.Start <= merged[len(merged)-1].End {
			merged[len(merged)-1].End = max(merged[len(merged)-1].End, r.End)
		} else {
			merged = append(merged, r)
		}
	}
	o.Ranges = merged
}
func (o *Overlay) Truncated(size int64) {
	o.Size = size
	o.BackingSize = min(o.BackingSize, size)
	ranges := []Range{}
	for _, r := range o.Ranges {
		if r.Start < size {
			ranges = append(ranges, Range{r.Start, min(r.End, size)})
		}
	}
	o.Ranges = ranges
}
func (o *Overlay) Materialize() { atomicWrite(filepath.Join(o.Store.Root, o.Name), o.Read(0, o.Size)) }
func NewColdStore(root string, archive *Archive, manifest, database Document, cache string) *Store {
	check(os.MkdirAll(root, 0700))
	bootstrap := filepath.Join(root, "cold-bootstrap.json")
	binding := archive.Storage.Identity
	if exists(bootstrap) {
		pinned := doc(decode(readFile(bootstrap)))
		require(equal(doc(pinned["manifest"])["group"], manifest["group"]) && equal(doc(pinned["database"])["history_id"], database["history_id"]), "cold root belongs to a different archive/database")
		if _, ok := pinned["storage"]; !ok && binding != nil {
			_, old := archive.Load(num(doc(pinned["manifest"])["revision"]))
			require(equal(old, pinned["manifest"]), "old cold root archive binding could not be verified")
			pinned["storage"] = binding
			atomicWrite(bootstrap, encode(pinned))
		}
		require(equal(pinned["storage"], binding), "cold root object storage binding changed")
		manifest, database = doc(pinned["manifest"]), doc(pinned["database"])
	} else {
		atomicWrite(bootstrap, encode(Document{"manifest": manifest, "database": database, "storage": binding}))
	}
	s := &Store{Root: root, Archive: archive, CloudManifest: manifest, Database: doc(clone(database)), FaultPath: "live.db-wal", Overlays: map[string]*Overlay{}, localVerified: map[string]string{}, Events: []any{}}
	if cache == "" {
		cache = filepath.Join(root, "cache")
	}
	s.Reader = NewChunkReader(archive, manifest, cache)
	local := filepath.Join(root, "manifest.json")
	if exists(local) {
		saved := doc(decode(readFile(local)))
		require(num(saved["format"]) == 1, "unknown local manifest format")
		s.Generations = list(saved["generations"])
	} else {
		s.Generations = list(doc(clone(database["store_manifest"]))["generations"])
		atomicWrite(local, encode(Document{"format": 1, "generations": s.Generations}))
	}
	s.rebuild()
	require(len(s.Generations) > 0, "empty cold history")
	g := doc(s.Generations[len(s.Generations)-1])
	for name, kind := range map[string]string{"live.db": "base", "live.db-wal": "wal"} {
		obj := str(g[kind])
		s.Overlays[name] = NewOverlay(s, name, obj, s.ObjectSize(obj))
	}
	s.Auxiliary = doc(clone(database["auxiliary"]))
	if exists(filepath.Join(root, "auxiliary.json")) {
		s.Auxiliary = doc(decode(readFile(filepath.Join(root, "auxiliary.json"))))
	}
	for name, v := range s.Auxiliary {
		obj := str(v)
		s.Overlays[safePath(name)] = NewOverlay(s, name, obj, s.ObjectSize(obj))
	}
	return s
}
func (s *Store) persistAuxiliary(name string, truncate bool) {
	updated := doc(clone(s.Auxiliary))
	baseName := name
	if strings.HasSuffix(name, "-wal") && s.Overlays[strings.TrimSuffix(name, "-wal")] != nil {
		baseName = strings.TrimSuffix(name, "-wal")
	}
	base, wal := s.Overlays[baseName], s.Overlays[baseName+"-wal"]
	if base == nil {
		return
	}
	if name == baseName || updated[baseName] == nil {
		updated[baseName] = s.PutObject(base.Read(0, base.Size), "db")
	}
	raw := []byte{}
	if wal != nil {
		raw = wal.Read(0, wal.Size)
	}
	parsed := ParseWAL(raw)
	if truncate && len(parsed.Commits) > 0 {
		require(updated[baseName] != nil, "auxiliary WAL reset requires a synced baseline")
		synced := s.ObjectBytes(str(updated[baseName]))
		last := parsed.Commits[len(parsed.Commits)-1]
		require(int64(len(synced)) == last.Size*parsed.PageSize, "auxiliary checkpoint size mismatch")
		for page, offset := range last.Pages {
			start := (page - 1) * parsed.PageSize
			require(bytes.Equal(synced[start:start+parsed.PageSize], raw[offset:offset+parsed.PageSize]), "auxiliary checkpoint does not cover WAL")
		}
	}
	if truncate {
		raw = []byte{}
	} else if len(raw) > 0 {
		frame := int64(0)
		if len(parsed.Commits) > 0 {
			frame = parsed.Commits[len(parsed.Commits)-1].Frame
		}
		if parsed.Epoch != nil {
			raw = raw[:32+frame*(parsed.PageSize+24)]
		} else {
			raw = []byte{}
		}
	}
	updated[baseName+"-wal"] = s.PutObject(raw, "wal")
	atomicWrite(filepath.Join(s.Root, "auxiliary.json"), encode(updated))
	s.Auxiliary = updated
}
func (s *Store) ColdState() Document {
	blockCache.Lock()
	defer blockCache.Unlock()
	return Document{"cloud_manifest": s.CloudManifest, "database": s.Database, "generations": s.Generations, "views": s.ViewNames, "hits": s.Reader.Hits, "misses": s.Reader.Misses, "root": s.Root}
}
func (s *Store) Export() {
	for _, o := range s.Overlays {
		o.Materialize()
	}
}
func (s *Store) String() string { return fmt.Sprintf("Store(%s)", s.Root) }
