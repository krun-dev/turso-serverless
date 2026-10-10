package pageserver

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

const ChunkSize = 64 * 1024
const MaxPack = 64 * 1024 * 1024

type Archive struct {
	Storage      *ObjectStorage
	Group, State string
	lock         *os.File
	mu           sync.Mutex
	MaxCatchup   int64
	Fault        string
}

func NewArchive(storage *ObjectStorage, group, state string) *Archive {
	identity(group)
	check(os.MkdirAll(state, 0700))
	f := must(os.OpenFile(filepath.Join(state, "coordinator.lock"), os.O_CREATE|os.O_RDWR, 0600))
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		panic(err)
	}
	return &Archive{Storage: storage, Group: group, State: state, lock: f, MaxCatchup: 10000}
}
func (a *Archive) Close() {
	if a.lock != nil {
		a.lock.Close()
		a.lock = nil
	}
}
func CommitKey(revision int64) string { return fmt.Sprintf("catalog/commits/r%020d.json", revision) }
func (a *Archive) Load(revision int64) (Document, Document) {
	var head Document
	var rawHead []byte
	if revision > 0 {
		rawHead = a.Storage.Read(CommitKey(revision))
		require(rawHead != nil, "requested archive revision missing")
		head = doc(decode(rawHead))
		require(num(head["format"]) == 1 && str(head["group"]) == a.Group && num(head["revision"]) == revision, "requested commit identity mismatch")
	} else {
		rawHint := a.Storage.Read("catalog/CURRENT.json")
		if rawHint != nil {
			hint := doc(decode(rawHint))
			r := num(hint["revision"])
			require(str(hint["group"]) == a.Group && r > 0 && str(hint["key"]) == CommitKey(r), "invalid CURRENT identity/key")
			rawHead = a.Storage.Read(CommitKey(r))
			require(rawHead != nil && digest(rawHead) == str(hint["sha256"]), "CURRENT commit missing or corrupt")
			head = doc(decode(rawHead))
			require(num(head["format"]) == 1 && str(head["group"]) == a.Group && num(head["revision"]) == r, "commit identity mismatch")
		}
		caught := int64(0)
		for {
			r := num(head["revision"])
			rawNext := a.Storage.Read(CommitKey(r + 1))
			if rawNext == nil {
				break
			}
			caught++
			require(caught <= a.MaxCatchup, "catalog catch-up budget exceeded")
			next := doc(decode(rawNext))
			var parent any
			if rawHead != nil {
				parent = digest(rawHead)
			}
			require(num(next["format"]) == 1 && str(next["group"]) == a.Group && num(next["revision"]) == r+1 && equal(next["parent_sha256"], parent), "non-contiguous commit chain")
			head, rawHead = next, rawNext
		}
	}
	if head == nil {
		return nil, Document{"format": 1, "group": a.Group, "revision": 0, "databases": Document{}, "chunks": Document{}}
	}
	raw := a.Storage.Read(str(head["manifest_key"]))
	require(raw != nil && digest(raw) == str(head["manifest_sha256"]), "published manifest missing or corrupt")
	manifest := doc(decode(raw))
	require(num(manifest["format"]) == 1 && str(manifest["group"]) == a.Group && num(manifest["revision"]) == num(head["revision"]), "manifest identity mismatch")
	head = doc(clone(head))
	head["sha256"] = digest(rawHead)
	return head, manifest
}
func makePack(chunks map[string][]byte) ([]byte, Document) {
	body := bytes.NewBuffer([]byte("TURSOPK1"))
	index := Document{}
	keys := Document{}
	for k := range chunks {
		keys[k] = nil
	}
	for _, sha := range sortedKeys(keys) {
		raw := chunks[sha]
		var compressed bytes.Buffer
		encoder := zlib.NewWriter(&compressed)
		_, err := encoder.Write(raw)
		check(err)
		check(encoder.Close())
		index[sha] = Document{"offset": body.Len(), "length": compressed.Len(), "raw_length": len(raw), "compression": "zlib", "sha256": sha}
		body.Write(compressed.Bytes())
	}
	footer := encode(index)
	body.Write(footer)
	check(binary.Write(body, binary.BigEndian, uint64(len(footer))))
	require(body.Len() <= MaxPack, "prototype pack exceeds 64 MiB budget; split batch")
	return body.Bytes(), index
}
func (a *Archive) fault(step string) {
	if a.Fault == step {
		a.Fault = ""
		fail("OSError", "injected archive failure: "+step)
	}
}
func (a *Archive) prepare(head, manifest Document, chunks map[string][]byte) {
	candidate := doc(clone(manifest))
	candidate["revision"] = num(head["revision"]) + 1
	job := randomID()
	var packKey any
	if len(chunks) > 0 {
		data, index := makePack(chunks)
		key := "packs/" + digest(data) + "__u" + job + ".pack"
		packKey = key
		atomicWrite(filepath.Join(a.State, "pending.pack"), data)
		for sha, v := range index {
			e := doc(v)
			e["key"] = key
			e["pack_sha256"] = digest(data)
			doc(candidate["chunks"])[sha] = e
		}
	}
	raw := encode(candidate)
	manifestKey := fmt.Sprintf("catalog/manifests/r%020d__%s__u%s.json", num(candidate["revision"]), digest(raw), job)
	var parent any
	if head != nil {
		parent = head["sha256"]
	}
	commit := Document{"format": 1, "group": a.Group, "revision": candidate["revision"], "parent_sha256": parent, "manifest_key": manifestKey, "manifest_sha256": digest(raw)}
	atomicWrite(filepath.Join(a.State, "pending.json"), encode(Document{"commit": commit, "manifest": candidate, "pack_key": packKey}))
}
func (a *Archive) finish() Document {
	plan := doc(decode(readFile(filepath.Join(a.State, "pending.json"))))
	commit := doc(plan["commit"])
	manifest := doc(plan["manifest"])
	require(digest(encode(manifest)) == str(commit["manifest_sha256"]) && str(manifest["group"]) == a.Group && num(manifest["revision"]) == num(commit["revision"]), "pending manifest checksum/identity mismatch")
	raw := encode(commit)
	key := CommitKey(num(commit["revision"]))
	existing := a.Storage.Read(key)
	if existing == nil {
		head, _ := a.Load(0)
		var parent any
		if head != nil {
			parent = head["sha256"]
		}
		if !equal(parent, commit["parent_sha256"]) {
			fail("Conflict", "pending plan parent changed; reload/revalidate candidate")
		}
		if plan["pack_key"] != nil {
			data := readFile(filepath.Join(a.State, "pending.pack"))
			require(strings.Contains(str(plan["pack_key"]), digest(data)), "pending pack checksum mismatch")
			a.Storage.Put(str(plan["pack_key"]), data)
		}
		a.fault("after_pack")
		a.Storage.Put(str(commit["manifest_key"]), encode(manifest))
		a.fault("before_commit")
		if !a.Storage.Create(key, raw) {
			existing = a.Storage.Read(key)
			if !bytes.Equal(existing, raw) {
				fail("Conflict", "commit slot occupied; reload/revalidate candidate")
			}
		}
	} else if !bytes.Equal(existing, raw) {
		fail("Conflict", "pending commit slot occupied; reload/revalidate candidate")
	}
	a.fault("after_commit")
	a.Storage.Hint("catalog/CURRENT.json", encode(Document{"group": a.Group, "revision": commit["revision"], "key": key, "sha256": digest(raw)}))
	a.fault("after_hint")
	check(os.Remove(filepath.Join(a.State, "pending.json")))
	os.Remove(filepath.Join(a.State, "pending.pack"))
	atomicWrite(filepath.Join(a.State, "completed.json"), raw)
	return commit
}

type Snapshot struct {
	Manifest Document
	Objects  map[string][]byte
}

func TakeSnapshot(root string, reader *Store) Snapshot {
	// A standalone worker must resolve old immutable objects from the pinned
	// cold directory without opening or changing the live sparse files.
	if reader == nil && exists(filepath.Join(root, "cold-bootstrap.json")) {
		pinned := doc(decode(readFile(filepath.Join(root, "cold-bootstrap.json"))))
		binding := doc(pinned["storage"])
		require(len(binding) > 0, "cold snapshot requires a persistent object storage binding")
		mode := str(binding["create_mode"])
		if mode == "" {
			mode = "s3"
		}
		var storage *ObjectStorage
		if str(binding["service"]) == "oss" {
			storage = Connect("oss", str(binding["endpoint"]), str(binding["bucket"]), str(binding["prefix"]), str(binding["region"]))
		} else {
			storage = ConnectWithMode(str(binding["service"]), str(binding["endpoint"]), str(binding["bucket"]), str(binding["prefix"]), str(binding["region"]), mode)
		}
		manifest := doc(pinned["manifest"])
		source := &Archive{Storage: storage, Group: str(manifest["group"]), MaxCatchup: 10000}
		_, published := source.Load(num(manifest["revision"]))
		require(equal(published, manifest), "cold snapshot pinned directory differs from published archive")
		cache := must(os.MkdirTemp("", "turso-snapshot-cache-"))
		defer os.RemoveAll(cache)
		reader = &Store{Root: root, Database: doc(pinned["database"]), Reader: NewChunkReader(source, manifest, cache), localVerified: map[string]string{}}
	}
	saved := doc(decode(readFile(filepath.Join(root, "manifest.json"))))
	require(num(saved["format"]) == 1 && len(list(saved["generations"])) > 0, "missing local publication")
	validator := reader
	if reader != nil {
		validator = &Store{Root: root, Database: reader.Database, Reader: reader.Reader, localVerified: map[string]string{}}
	}
	if validator == nil {
		validator = &Store{Root: root, localVerified: map[string]string{}}
	}
	validator.buildViews(list(saved["generations"]))
	saved = doc(clone(saved))
	objects := map[string][]byte{}
	for _, v := range list(saved["generations"]) {
		g := doc(v)
		for _, kind := range []string{"base", "wal"} {
			raw := validator.ObjectBytes(str(g[kind]))
			if kind == "wal" && len(raw) > 0 {
				parsed := ParseWAL(raw)
				if parsed.Epoch != nil {
					raw = raw[:32+num(g["commit_frame"])*(parsed.PageSize+24)]
				} else {
					raw = []byte{}
				}
			}
			suffix := "db"
			if kind == "wal" {
				suffix = "wal"
			}
			name := "objects/" + digest(raw) + "." + suffix
			g[kind] = name
			objects[name] = raw
		}
	}
	if exists(filepath.Join(root, "auxiliary.json")) {
		aux := doc(decode(readFile(filepath.Join(root, "auxiliary.json"))))
		saved["auxiliary"] = aux
		for _, v := range aux {
			objects[str(v)] = validator.ObjectBytes(str(v))
		}
	}
	return Snapshot{saved, objects}
}
func checkPrefix(previous, saved Document, objects map[string][]byte) {
	oldGens, newGens := list(doc(previous["store_manifest"])["generations"]), list(saved["generations"])
	require(len(newGens) >= len(oldGens), "cannot archive older local history")
	for index, v := range oldGens {
		old, newG := doc(v), doc(newGens[index])
		require(equal(old["base"], newG["base"]) && equal(old["covers_position"], newG["covers_position"]), "archived baseline/coverage changed")
		require(num(newG["commit_frame"]) >= num(old["commit_frame"]), "archived commit position regressed")
		if old["epoch"] != nil {
			require(equal(old["epoch"], newG["epoch"]), "archived WAL generation changed")
		}
		obj := doc(doc(previous["objects"])[str(old["wal"])])
		raw := objects[str(newG["wal"])]
		require(int64(len(raw)) >= num(obj["size"]), "archived WAL prefix shrank")
		for j, sha := range list(obj["chunks"]) {
			start := int64(j * ChunkSize)
			length := min(int64(ChunkSize), num(obj["size"])-start)
			require(digest(raw[start:start+length]) == str(sha), "archived WAL prefix changed")
		}
	}
}
func (a *Archive) Flush(roots map[string]string, readers map[string]*Store) Document {
	snapshots := map[string]Snapshot{}
	for name, root := range roots {
		snapshots[identity(name)] = TakeSnapshot(root, readers[name])
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if exists(filepath.Join(a.State, "pending.json")) {
		a.finish()
	}
	head, manifest := a.Load(0)
	candidate := doc(clone(manifest))
	newChunks := map[string][]byte{}
	for name, snapshot := range snapshots {
		saved, objects := snapshot.Manifest, snapshot.Objects
		previous := doc(candidate["databases"])[name]
		if previous != nil {
			checkPrefix(doc(previous), saved, objects)
		}
		objectIndex := Document{}
		for key, raw := range objects {
			chunks := []any{}
			for start := 0; start < len(raw); start += ChunkSize {
				data := raw[start:min(len(raw), start+ChunkSize)]
				sha := digest(data)
				chunks = append(chunks, sha)
				if doc(candidate["chunks"])[sha] == nil {
					newChunks[sha] = data
				}
			}
			objectIndex[key] = Document{"size": len(raw), "sha256": digest(raw), "chunks": chunks}
		}
		indexes := []any{}
		for _, v := range list(saved["generations"]) {
			g := doc(v)
			indexes = append(indexes, pageIndex(g, objects[str(g["base"])], objects[str(g["wal"])]))
		}
		db := Document{"history_id": name, "store_manifest": saved, "objects": objectIndex, "page_indexes": indexes}
		if aux := doc(saved["auxiliary"]); len(aux) > 0 {
			db["auxiliary"] = aux
		}
		doc(candidate["databases"])[name] = db
	}
	if equal(candidate, manifest) {
		return Document{"unchanged": true, "revision": manifest["revision"]}
	}
	a.prepare(head, candidate, newChunks)
	return a.finish()
}
func (a *Archive) ReadChunk(sha string, extent Document) []byte {
	require(str(extent["sha256"]) == sha && str(extent["compression"]) == "zlib" && num(extent["raw_length"]) > 0 && num(extent["raw_length"]) <= ChunkSize && num(extent["length"]) > 0 && num(extent["length"]) <= ChunkSize+1024, "invalid chunk metadata")
	compressed := a.Storage.ReadRange(str(extent["key"]), num(extent["offset"]), num(extent["length"]))
	input := bytes.NewReader(compressed)
	decoder := must(zlib.NewReader(input))
	defer decoder.Close()
	raw := must(io.ReadAll(io.LimitReader(decoder, ChunkSize+1)))
	require(input.Len() == 0 && int64(len(raw)) == num(extent["raw_length"]) && digest(raw) == sha, "pack chunk checksum/length mismatch")
	return raw
}

type Compaction struct {
	Head, Manifest Document
	Chunks         map[string][]byte
}

func (a *Archive) PrepareCompaction() Compaction {
	head, manifest := a.Load(0)
	require(head != nil, "no archive to compact")
	needed := map[string]bool{}
	for _, v := range doc(manifest["databases"]) {
		for _, o := range doc(doc(v)["objects"]) {
			for _, sha := range list(doc(o)["chunks"]) {
				needed[str(sha)] = true
			}
		}
	}
	chunks := map[string][]byte{}
	for sha := range needed {
		chunks[sha] = a.ReadChunk(sha, doc(doc(manifest["chunks"])[sha]))
	}
	candidate := doc(clone(manifest))
	candidate["chunks"] = Document{}
	return Compaction{head, candidate, chunks}
}
func (a *Archive) FinishCompaction(plan Compaction) Document {
	a.mu.Lock()
	defer a.mu.Unlock()
	if exists(filepath.Join(a.State, "pending.json")) {
		fail("Conflict", "finish pending archive before compaction")
	}
	head, _ := a.Load(0)
	if !equal(head, plan.Head) {
		fail("Conflict", "compaction input revision changed; retry with new directory")
	}
	a.prepare(plan.Head, plan.Manifest, plan.Chunks)
	return a.finish()
}
func (a *Archive) Compact() Document { return a.FinishCompaction(a.PrepareCompaction()) }
func (a *Archive) Restore(destination string, revision int64) Document {
	require(!exists(destination), "restore destination must not exist")
	head, manifest := a.Load(revision)
	require(head != nil, "no published archive")
	check(os.MkdirAll(filepath.Dir(destination), 0700))
	staging := must(os.MkdirTemp(filepath.Dir(destination), ".restore-"))
	defer os.RemoveAll(staging)
	chunks := map[string][]byte{}
	for name, v := range doc(manifest["databases"]) {
		identity(name)
		db := doc(v)
		root := filepath.Join(staging, name)
		check(os.MkdirAll(root, 0700))
		for key, v := range doc(db["objects"]) {
			obj := doc(v)
			require(filepath.ToSlash(filepath.Dir(key)) == "objects" && strings.TrimSuffix(filepath.Base(key), filepath.Ext(key)) == str(obj["sha256"]), "invalid restored object path")
			var raw []byte
			for _, v := range list(obj["chunks"]) {
				sha := str(v)
				if chunks[sha] == nil {
					chunks[sha] = a.ReadChunk(sha, doc(doc(manifest["chunks"])[sha]))
				}
				raw = append(raw, chunks[sha]...)
			}
			require(int64(len(raw)) == num(obj["size"]) && digest(raw) == str(obj["sha256"]), "restored object checksum mismatch")
			atomicWrite(filepath.Join(root, key), raw)
		}
		atomicWrite(filepath.Join(root, "manifest.json"), encode(db["store_manifest"]))
		NewStore(root, false, false)
		for key, v := range doc(db["auxiliary"]) {
			atomicWrite(filepath.Join(root, safePath(key)), readFile(filepath.Join(root, str(v))))
		}
		if len(doc(db["auxiliary"])) > 0 {
			atomicWrite(filepath.Join(root, "auxiliary.json"), encode(db["auxiliary"]))
		}
	}
	atomicWrite(filepath.Join(staging, "archive-root.json"), encode(head))
	check(os.Rename(staging, destination))
	syncDir(filepath.Dir(destination))
	return Document{"revision": head["revision"], "databases": sortedKeys(doc(manifest["databases"]))}
}
