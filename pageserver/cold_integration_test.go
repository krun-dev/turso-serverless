package pageserver

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func cacheBytes(root string) int64 {
	total := int64(0)
	for _, e := range must(os.ReadDir(root)) {
		if !e.IsDir() {
			total += must(e.Info()).Size()
		}
	}
	return total
}
func rangeFault(s *ObjectStorage, fault string) { s.mu.Lock(); s.RangeFault = fault; s.mu.Unlock() }
func TestCold(t *testing.T) {
	integration(t, "cold", func(s *suite) {
		storage := s.storage("cold")
		archive := NewArchive(storage, "cold", filepath.Join(s.run, "coordinator"))
		defer archive.Close()
		original := newLab(s, "")
		p := original.open("live.db", false)
		original.full(p)
		original.sql(p, "CREATE TABLE t(id INTEGER PRIMARY KEY,payload TEXT)")
		for start := 0; start < 512; start += 32 {
			v := []string{}
			for i := start; i < start+32; i++ {
				v = append(v, fmt.Sprintf("(%d,'%04d%s')", i, i, strings.Repeat("x", 2044)))
			}
			original.sql(p, "INSERT INTO t VALUES "+strings.Join(v, ","))
		}
		original.sql(p, "PRAGMA wal_checkpoint(TRUNCATE)")
		original.admin("baseline")
		original.sql(p, "UPDATE t SET payload='archived' WHERE id=256")
		archive.Flush(map[string]string{"db": original.root}, nil)
		_, manifest := archive.Load(0)
		db := doc(doc(manifest["databases"])["db"])
		before := num(storage.Metrics()["RANGE_GET"])
		cold := coldLab(s, filepath.Join(s.run, "cold-root"), archive, manifest, db)
		require(num(storage.Metrics()["RANGE_GET"]) == before, "startup downloaded payload")
		s.passed("startup_uses_published_page_index_without_payload_download")
		latest := cold.latest()
		r := cold.open(latest, true)
		require(strings.Contains(cold.scalar(r, "SELECT payload FROM t WHERE id=256"), `value: "archived"`), "cold WAL point wrong")
		cold.assertScalar(r, "SELECT length(payload) FROM t WHERE id=400", "Integer(2048)")
		cached := cacheBytes(cold.store.Reader.Cache)
		gens := list(doc(db["store_manifest"])["generations"])
		baseSize := num(doc(doc(db["objects"])[str(doc(gens[len(gens)-1])["base"])])["size"])
		require(cached < baseSize/2, "point query fetched whole base")
		s.passed("cold_point_queries_fetch_only_needed_base_and_wal_blocks", Document{"cached_bytes": cached, "baseline_bytes": baseSize})
		before = num(storage.Metrics()["RANGE_GET"])
		r2 := cold.open(latest, true)
		cold.sql(r2, "SELECT payload FROM t WHERE id=256")
		cold.sql(r2, "SELECT length(payload) FROM t WHERE id=400")
		require(num(storage.Metrics()["RANGE_GET"]) == before, "connection cache not reused")
		s.passed("new_compute_connection_reuses_verified_block_cache")
		live := cold.open("live.db", false)
		cold.full(live)
		cold.sql(live, "INSERT INTO t VALUES(600,'cold-write')")
		cold.count(live, 513)
		cold.count(r, 512)
		s.passed("cold_writable_open_commit_and_pinned_old_reader")
		cold.sql(r, "INSERT INTO t VALUES(999,'readonly')", false)
		for _, path := range []string{"cold-bootstrap.json", "auxiliary.json"} {
			reply := cold.request(Request{Op: "write", Path: path, Extra: Document{}}, nil)
			require(strings.Contains(str(reply["error"]), "immutable"), "internal metadata write allowed")
		}
		s.passed("readonly_snapshots_and_internal_metadata_reject_writes")
		binding := storage.Identity
		storage.Identity = Document{"test": "different-object-prefix"}
		expectFailure(func() { NewColdStore(cold.root, archive, manifest, db, "") }, "binding changed")
		storage.Identity = binding
		cold.count(live, 513)
		s.passed("cold_root_cannot_silently_switch_object_storage_namespace")
		reply, alias, err := cold.store.Handle(Request{Op: "read", Path: "./live.db", Len: 16, Extra: Document{}}, nil)
		check(err)
		require(reply["error"] == nil, "alias error")
		assertBytes(alias, []byte("SQLite format 3\x00"))
		ar := cold.open("./live.db", true)
		cold.count(ar, 513)
		cold.integrity(ar)
		s.passed("relative_alias_uses_the_same_remote_backing_not_sparse_holes")
		cold.sql(live, "PRAGMA wal_checkpoint(TRUNCATE)")
		cold.sql(live, "DELETE FROM t WHERE id<480")
		cold.sql(live, "VACUUM")
		cold.sql(live, "INSERT INTO t VALUES(601,'after-vacuum')")
		cold.count(live, 34)
		cold.integrity(live)
		cold.count(r, 512)
		s.passed("cold_checkpoint_vacuum_shrink_and_old_views")
		archive.Flush(map[string]string{"db": cold.root}, map[string]*Store{"db": cold.store})
		archive.Compact()
		_, newest := archive.Load(0)
		newDB := doc(doc(newest["databases"])["db"])
		empty := coldLab(s, filepath.Join(s.run, "fresh-cold"), archive, newest, newDB)
		w := empty.open("live.db", false)
		empty.full(w)
		empty.count(w, 34)
		empty.sql(w, "INSERT INTO t VALUES(602,'fresh-write')")
		empty.integrity(w)
		s.passed("rearchive_compact_and_start_writable_from_empty_cache")
		pinned := coldLab(s, filepath.Join(s.run, "old-directory"), archive, manifest, db)
		old := pinned.open("live.db", false)
		_ = old
		require(doc(newDB["objects"])[str(doc(gens[0])["wal"])] == nil, "pinned old WAL fixture still in new catalog")
		pinned.manifest, pinned.database = newest, newDB
		old = pinned.restart()
		pinned.count(old, 512)
		require(strings.Contains(pinned.scalar(old, "SELECT payload FROM t WHERE id=256"), `value: "archived"`), "pinned directory changed")
		s.passed("restart_retains_pinned_directory_after_remote_compaction_replaces_wal_object")
		files := must(os.ReadDir(empty.store.Reader.Cache))
		require(len(files) > 0, "cache corruption fixture empty")
		file := filepath.Join(empty.store.Reader.Cache, files[0].Name())
		check(os.WriteFile(file, []byte("corrupt"), 0600))
		require(!bytes.Equal(empty.store.Reader.Chunk(files[0].Name()), []byte("corrupt")), "corrupted cache used")
		s.passed("corrupted_cache_is_refetched_and_verified")
		empty.sql(w, "BEGIN")
		empty.sql(w, "UPDATE t SET payload='unconfirmed' WHERE id=600")
		w = empty.restart()
		empty.count(w, 35)
		require(strings.Contains(empty.scalar(w, "SELECT payload FROM t WHERE id=600"), `value: "cold-write"`), "open transaction recovered")
		empty.integrity(w)
		s.passed("local_restart_keeps_confirmed_commit_and_discards_open_transaction")
		empty.full(w)
		empty.sql(w, "BEGIN")
		empty.sql(w, "INSERT INTO t VALUES(603,'before-failed-sync')")
		empty.admin("fault", Document{"mode": "before"})
		empty.sql(w, "COMMIT", false)
		w = empty.restart()
		empty.count(w, 35)
		s.passed("failed_wal_sync_never_becomes_confirmed_after_restart")
		empty.full(w)
		empty.sql(w, "BEGIN")
		empty.sql(w, "INSERT INTO t VALUES(604,'lost-reply')")
		empty.admin("fault", Document{"mode": "after"})
		empty.sql(w, "COMMIT", false)
		w = empty.restart()
		empty.count(w, 36)
		empty.integrity(w)
		s.passed("lost_wal_sync_reply_recovers_whole_confirmed_transaction")
		for _, policy := range []string{"NORMAL", "OFF"} {
			weak := coldLab(s, filepath.Join(s.run, strings.ToLower(policy)), archive, newest, newDB)
			wp := weak.open("live.db", false)
			weak.sql(wp, "PRAGMA synchronous="+policy)
			weak.sql(wp, "INSERT INTO t VALUES(700,'weak-sync')")
			weak.sql(wp, "PRAGMA wal_checkpoint(TRUNCATE)")
			wp = weak.restart()
			n := 34
			if policy == "NORMAL" {
				n = 35
			}
			weak.count(wp, n)
			weak.integrity(wp)
		}
		s.passed("normal_and_off_checkpoint_preserve_complete_prefix_on_restart", Document{"normal_rows": 35, "off_rows": 34})
		sha := sortedKeys(doc(newest["chunks"]))[0]
		isolated := coldLab(s, filepath.Join(s.run, "read-fault"), archive, newest, newDB)
		rangeFault(storage, "outage")
		expectFailure(func() { isolated.store.Reader.Chunk(sha) }, "outage")
		require(!exists(filepath.Join(isolated.store.Reader.Cache, sha)), "outage wrote invalid cache")
		rangeFault(storage, "")
		require(len(isolated.store.Reader.Chunk(sha)) > 0, "read retry failed")
		s.passed("object_outage_fails_closed_and_read_can_retry")
		corrupt := coldLab(s, filepath.Join(s.run, "remote-corruption"), archive, newest, newDB)
		rangeFault(storage, "corrupt")
		expectFailure(func() { corrupt.store.Reader.Chunk(sha) }, "")
		require(!exists(filepath.Join(corrupt.store.Reader.Cache, sha)), "corrupted remote cache installed")
		rangeFault(storage, "")
		before = num(storage.Metrics()["RANGE_GET"])
		chunks := make([][]byte, 8)
		errs := make([]error, 8)
		var wg sync.WaitGroup
		for i := range chunks {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				chunks[i], errs[i] = Guard(func() []byte { return corrupt.store.Reader.Chunk(sha) })
			}(i)
		}
		wg.Wait()
		for i := range chunks {
			check(errs[i])
			assertBytes(chunks[i], chunks[0])
		}
		require(num(storage.Metrics()["RANGE_GET"]) == before+1, "concurrent misses not coalesced")
		s.passed("remote_corruption_rejected_and_concurrent_retry_coalesces")
		corrupt.store.Reader.Budget = 64 * 1024
		keys := sortedKeys(doc(newest["chunks"]))
		require(len(keys) >= 4, "cache eviction fixture too small")
		for _, key := range keys[:4] {
			corrupt.store.Reader.Chunk(key)
		}
		require(cacheBytes(corrupt.store.Reader.Cache) <= 64*1024, "disk cache exceeds budget")
		cold.integrity(r)
		s.passed("cache_eviction_does_not_change_pinned_version")
		empty.full(w)
		for _, sql := range []string{"ATTACH 'aux.db' AS aux", "CREATE TABLE aux.a(id INTEGER PRIMARY KEY,v TEXT)", "INSERT INTO aux.a VALUES(1,'attached')", "PRAGMA aux.wal_checkpoint(TRUNCATE)"} {
			empty.sql(w, sql)
		}
		w = empty.restart()
		empty.sql(w, "ATTACH 'aux.db' AS aux")
		require(strings.Contains(empty.scalar(w, "SELECT v FROM aux.a"), `value: "attached"`), "attached checkpoint lost")
		for _, sql := range []string{"INSERT INTO aux.a VALUES(2,'archived-aux')", "ATTACH 'suffix-wal' AS suffix", "CREATE TABLE suffix.s(a INTEGER)", "INSERT INTO suffix.s VALUES(9)", "PRAGMA suffix.wal_checkpoint(TRUNCATE)"} {
			empty.sql(w, sql)
		}
		archive.Flush(map[string]string{"db": empty.root}, map[string]*Store{"db": empty.store})
		archive.Compact()
		_, withAux := archive.Load(0)
		aux := coldLab(s, filepath.Join(s.run, "auxiliary-cold"), archive, withAux, doc(doc(withAux["databases"])["db"]))
		ap := aux.open("live.db", false)
		aux.sql(ap, "ATTACH 'aux.db' AS aux")
		aux.assertScalar(ap, "SELECT count(*) FROM aux.a", "Integer(2)")
		require(strings.Contains(aux.scalar(ap, "PRAGMA aux.integrity_check"), `value: "ok"`), "attached integrity failed")
		aux.sql(ap, "ATTACH 'suffix-wal' AS suffix")
		aux.assertScalar(ap, "SELECT a FROM suffix.s", "Integer(9)")
		s.passed("attached_database_basename_ending_with_wal_recovers_correctly")
		s.passed("attached_db_checkpoint_restart_rearchive_and_cold_recovery")
		dest := filepath.Join(s.run, "fully-restored")
		archive.Restore(dest, 0)
		restored := newLab(s, filepath.Join(dest, "db"))
		rp := restored.open("live.db", false)
		restored.sql(rp, "ATTACH 'aux.db' AS aux")
		restored.assertScalar(rp, "SELECT count(*) FROM aux.a", "Integer(2)")
		restored.count(rp, 36)
		require(exists(filepath.Join(dest, "db/auxiliary.json")), "auxiliary map not restored")
		s.passed("full_restore_preserves_auxiliary_objects_and_publication_map")
		g := doc(list(doc(newDB["store_manifest"])["generations"])[0])
		base := str(g["base"])
		size := isolated.store.ObjectSize(base)
		sparse := NewOverlay(isolated.store, "shrink-regrow.db", base, size)
		sparse.Truncated(size - 4096)
		sparse.Truncated(size)
		assertBytes(sparse.Read(size-4096, 4096), make([]byte, 4096))
		assertBytes(sparse.Read(0, 16), []byte("SQLite format 3\x00"))
		s.passed("shrink_then_regrow_never_resurrects_deleted_backing_pages")
		uninitialized := newLab(s, "")
		initial := uninitialized.open("live.db", false)
		uninitialized.assertScalar(initial, "PRAGMA page_count", "Integer(0)")
		uninitialized.admin("baseline")
		archive.Flush(map[string]string{"empty": uninitialized.root}, nil)
		_, initialManifest := archive.Load(0)
		pristine := coldLab(s, filepath.Join(s.run, "uninitialized"), archive, initialManifest, doc(doc(initialManifest["databases"])["empty"]))
		pp := pristine.open("live.db", false)
		pristine.assertScalar(pp, "PRAGMA page_count", "Integer(0)")
		pristine.sql(pp, "PRAGMA page_size=1024")
		pristine.sql(pp, "CREATE TABLE fresh(a INTEGER)")
		pristine.sql(pp, "INSERT INTO fresh VALUES(7)")
		pristine.assertScalar(pp, "PRAGMA page_size", "Integer(1024)")
		archive.Flush(map[string]string{"empty": pristine.root}, map[string]*Store{"empty": pristine.store})
		archive.Compact()
		_, populatedManifest := archive.Load(0)
		populated := coldLab(s, filepath.Join(s.run, "populated-small-pages"), archive, populatedManifest, doc(doc(populatedManifest["databases"])["empty"]))
		pp = populated.open("live.db", false)
		populated.assertScalar(pp, "PRAGMA page_size", "Integer(1024)")
		populated.assertScalar(pp, "SELECT a FROM fresh", "Integer(7)")
		populated.integrity(pp)
		require(populated.store.Views["view-0.db"].DBSize == 0, "empty history fabricated pages")
		s.passed("empty_archive_preserves_zero_pages_and_initial_page_size_selection")
		s.report["operations"] = storage.Metrics()
	})
}
