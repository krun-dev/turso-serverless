package pageserver

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestArchive(t *testing.T) {
	integration(t, "archive", func(s *suite) {
		storage := s.storage("archive")
		storage.Probe()
		s.passed("atomic_create_and_range_probe")
		unsafe := NewStorage(unsafeCreateBackend{NewMemoryBackend()})
		expectFailure(unsafe.Probe, "ignored atomic")
		s.passed("backend_ignoring_creation_condition_is_rejected")
		a, wa := initialized(s)
		b, wb := initialized(s)
		roots := map[string]string{"db_a": a.root, "db_b": b.root}
		stale := filepath.Join(s.run, "stale-db-a")
		treeCopy(a.root, stale)
		archive := NewArchive(storage, "group1", filepath.Join(s.run, "coordinator"))
		defer func() { archive.Close() }()
		before := storage.Metrics()
		first := archive.Flush(roots, nil)
		require(num(first["revision"]) == 1, "first revision wrong")
		after := storage.Metrics()
		writes := int64(0)
		for _, k := range []string{"PUT", "CREATE", "HINT_PUT"} {
			writes += num(after[k]) - num(before[k])
		}
		require(writes == 4, "publication did not use four writes")
		_, manifest := archive.Load(0)
		keys := Document{}
		for _, v := range doc(manifest["chunks"]) {
			keys[str(doc(v)["key"])] = true
		}
		require(len(keys) == 1 && len(doc(manifest["databases"])) == 2, "cross-db pack missing")
		hint1 := storage.Read("catalog/CURRENT.json")
		s.passed("two_databases_one_pack_four_publication_writes", Document{"writes": writes})
		pack := storage.Read(sortedKeys(keys)[0])
		footerSize := int(binary.BigEndian.Uint64(pack[len(pack)-8:]))
		footer := doc(decode(pack[len(pack)-8-footerSize : len(pack)-8]))
		require(string(pack[:8]) == "TURSOPK1" && len(footer) == len(doc(manifest["chunks"])), "pack footer wrong")
		for _, v := range doc(manifest["chunks"]) {
			require(doc(v)["pack_sha256"] == digest(pack), "pack checksum missing")
		}
		s.passed("pack_footer_and_independent_chunk_checksums")
		before = storage.Metrics()
		require(flag(archive.Flush(roots, nil)["unchanged"]), "idle batch changed")
		after = storage.Metrics()
		for _, k := range []string{"PUT", "CREATE", "HINT_PUT"} {
			require(equal(before[k], after[k]), "idle write")
		}
		s.passed("idle_batch_writes_nothing")
		a.sql(wa, "INSERT INTO t VALUES(90,'unarchived')")
		dest := filepath.Join(s.run, "restored-tail")
		archive.Restore(dest, 0)
		checkDatabase(s, filepath.Join(dest, "db_a"), 80)
		checkDatabase(s, filepath.Join(dest, "db_b"), 80)
		s.passed("unarchived_complete_tail_can_be_lost_without_corruption")
		archive.Fault = "before_commit"
		expectFailure(func() { archive.Flush(roots, nil) }, "injected")
		head, _ := archive.Load(0)
		require(num(head["revision"]) == 1, "unpublished directory visible")
		s.passed("pack_and_manifest_without_commit_are_not_visible")
		archive.Close()
		archive = NewArchive(storage, "group1", filepath.Join(s.run, "coordinator"))
		require(num(archive.Flush(roots, nil)["revision"]) == 2 && !exists(filepath.Join(archive.State, "pending.json")), "durable retry failed")
		s.passed("durable_upload_plan_retries_same_commit")
		a.sql(wa, "INSERT INTO t VALUES(91,'after-commit')")
		archive.Fault = "after_commit"
		expectFailure(func() { archive.Flush(roots, nil) }, "injected")
		require(num(doc(decode(storage.Read("catalog/CURRENT.json")))["revision"]) == 2, "hint advanced before fault")
		head, _ = archive.Load(0)
		require(num(head["revision"]) == 3, "committed directory missing")
		dest = filepath.Join(s.run, "after-commit")
		archive.Restore(dest, 0)
		checkDatabase(s, filepath.Join(dest, "db_a"), 82)
		s.passed("crash_after_commit_before_hint_recovers_new_directory")
		archive.Flush(roots, nil)
		storage.Hint("catalog/CURRENT.json", hint1)
		head, _ = archive.Load(0)
		require(num(head["revision"]) == 3, "late hint regressed authority")
		s.passed("late_current_cannot_regress_authority")
		storage.Delete("catalog/CURRENT.json")
		head, _ = archive.Load(0)
		require(num(head["revision"]) == 3, "missing hint recovery failed")
		s.passed("missing_hint_recovers_without_list")
		storage.Hint("catalog/CURRENT.json", encode(Document{"group": "group1", "revision": 3, "key": CommitKey(3), "sha256": head["sha256"]}))
		require(!storage.Create(CommitKey(3), []byte("overwrite")), "commit overwritten")
		head, _ = archive.Load(0)
		require(num(head["revision"]) == 3, "commit corrupted")
		s.passed("commit_slot_cannot_be_overwritten")
		expectFailure(func() { archive.Flush(map[string]string{"db_a": stale}, nil) }, "")
		head, _ = archive.Load(0)
		require(num(head["revision"]) == 3, "stale history published")
		s.passed("older_local_prefix_is_rejected")
		partial := filepath.Join(s.run, "partial")
		treeCopy(a.root, partial)
		saved := doc(decode(readFile(filepath.Join(partial, "manifest.json"))))
		gens := list(saved["generations"])
		g := doc(gens[len(gens)-1])
		raw := append(readFile(filepath.Join(partial, str(g["wal"]))), []byte("torn-uncommitted-tail")...)
		name := "objects/" + digest(raw) + ".wal"
		check(os.WriteFile(filepath.Join(partial, name), raw, 0600))
		g["wal"] = name
		check(os.WriteFile(filepath.Join(partial, "manifest.json"), encode(saved), 0600))
		snapshot := TakeSnapshot(partial, nil)
		gs := list(snapshot.Manifest["generations"])
		require(digest(snapshot.Objects[str(doc(gs[len(gs)-1])["wal"])]) != digest(raw), "partial suffix escaped")
		require(flag(archive.Flush(map[string]string{"db_a": partial}, nil)["unchanged"]), "partial tail changed archive")
		s.passed("partial_wal_suffix_is_excluded")
		a.sql(wa, "INSERT INTO t VALUES(92,'async-a')")
		barrier := filepath.Join(s.run, "upload-barrier")
		storage.mu.Lock()
		storage.PutBarrier = barrier
		storage.mu.Unlock()
		defer func() { os.WriteFile(barrier+".release", nil, 0600) }()
		done := make(chan error, 1)
		go func() { _, err := Guard(func() Document { return archive.Flush(roots, nil) }); done <- err }()
		waitPath(barrier + ".entered")
		b.sql(wb, "INSERT INTO t VALUES(2001,'write-during-upload')")
		b.count(wb, 81)
		check(os.WriteFile(barrier+".release", nil, 0600))
		select {
		case err := <-done:
			check(err)
		case <-time.After(30 * time.Second):
			panic(fmt.Errorf("archive barrier timeout"))
		}
		storage.mu.Lock()
		storage.PutBarrier = ""
		storage.mu.Unlock()
		dest = filepath.Join(s.run, "during-upload")
		archive.Restore(dest, 0)
		checkDatabase(s, filepath.Join(dest, "db_b"), 80)
		s.passed("sql_commit_continues_while_archive_upload_is_blocked")
		archive.Flush(roots, nil)
		storage.mu.Lock()
		storage.RangeFault = "corrupt"
		storage.mu.Unlock()
		dest = filepath.Join(s.run, "rejected-corruption")
		expectFailure(func() { archive.Restore(dest, 0) }, "")
		require(!exists(dest), "corrupted restore installed")
		staging := must(filepath.Glob(filepath.Join(s.run, ".restore-*")))
		require(len(staging) == 0, "restore scratch leaked")
		storage.mu.Lock()
		storage.RangeFault = ""
		storage.mu.Unlock()
		s.passed("corrupt_pack_fails_before_database_installation")
		b.sql(wb, "DELETE FROM t WHERE id>=32 AND id<64")
		b.sql(wb, "VACUUM")
		b.sql(wb, "INSERT INTO t VALUES(2002,'after-vacuum')")
		archive.Flush(roots, nil)
		_, prior := archive.Load(0)
		archive.Compact()
		_, compacted := archive.Load(0)
		require(equal(compacted["databases"], prior["databases"]), "compaction changed databases")
		keys = Document{}
		for _, v := range doc(compacted["chunks"]) {
			keys[str(doc(v)["key"])] = true
		}
		require(len(keys) == 1, "compaction not packed")
		dest = filepath.Join(s.run, "compacted")
		archive.Restore(dest, 0)
		checkDatabase(s, filepath.Join(dest, "db_a"), 83)
		checkDatabase(s, filepath.Join(dest, "db_b"), 50)
		s.passed("pack_compaction_keeps_vacuum_generations_and_live_data")
		dest = filepath.Join(s.run, "historical")
		archive.Restore(dest, 1)
		checkDatabase(s, filepath.Join(dest, "db_a"), 80)
		checkDatabase(s, filepath.Join(dest, "db_b"), 80)
		s.passed("old_archive_revision_survives_compaction")
		plan := archive.PrepareCompaction()
		a.sql(wa, "INSERT INTO t VALUES(93,'compaction-race')")
		archive.Flush(roots, nil)
		expectFailure(func() { archive.FinishCompaction(plan) }, "revision changed")
		dest = filepath.Join(s.run, "final")
		archive.Restore(dest, 0)
		checkDatabase(s, filepath.Join(dest, "db_a"), 84)
		s.passed("stale_compaction_cannot_overwrite_new_archive_tail")
		r := a.open("view-0.db", true)
		a.count(r, 64)
		r.stop()
		s.passed("live_historical_page_reads_survive_remote_compaction")
		latestHint := storage.Read("catalog/CURRENT.json")
		storage.Hint("catalog/CURRENT.json", hint1)
		archive.MaxCatchup = 1
		expectFailure(func() { archive.Load(0) }, "budget")
		archive.MaxCatchup = 10000
		storage.Hint("catalog/CURRENT.json", latestHint)
		s.passed("catchup_budget_failure_does_not_return_stale_head")
		vals := []string{}
		for i := 3000; i < 3192; i++ {
			vals = append(vals, fmt.Sprintf("(%d,'%04d%s')", i, i, strings.Repeat("y", 1000)))
		}
		a.sql(wa, "INSERT INTO t VALUES "+strings.Join(vals, ","))
		archive.Flush(roots, nil)
		_, big := archive.Load(0)
		oldDB := doc(doc(big["databases"])["db_a"])
		gens = list(doc(oldDB["store_manifest"])["generations"])
		oldWAL := doc(doc(oldDB["objects"])[str(doc(gens[len(gens)-1])["wal"])])
		oldChunks := list(oldWAL["chunks"])
		require(len(oldChunks) > 2, "WAL reuse fixture too small")
		a.sql(wa, "INSERT INTO t VALUES(4000,'small-append')")
		archive.Flush(roots, nil)
		_, appended := archive.Load(0)
		newDB := doc(doc(appended["databases"])["db_a"])
		gens = list(doc(newDB["store_manifest"])["generations"])
		newWAL := doc(doc(newDB["objects"])[str(doc(gens[len(gens)-1])["wal"])])
		newChunks := Document{}
		for _, v := range list(newWAL["chunks"]) {
			newChunks[str(v)] = true
		}
		for _, v := range oldChunks[:len(oldChunks)-1] {
			require(newChunks[str(v)] != nil, "full WAL chunks not reused")
		}
		added := int64(0)
		for sha, v := range doc(appended["chunks"]) {
			if doc(big["chunks"])[sha] == nil {
				added += num(doc(v)["raw_length"])
			}
		}
		require(added < num(newWAL["size"]), "large prefix uploaded again")
		s.passed("large_wal_append_reuses_previously_archived_full_chunks")
		a.close()
		b.close()
		for _, root := range roots {
			check(os.RemoveAll(root))
		}
		archive.Close()
		check(os.RemoveAll(archive.State))
		archive = NewArchive(storage, "group1", filepath.Join(s.run, "fresh-coordinator"))
		dest = filepath.Join(s.run, "remote-only")
		archive.Restore(dest, 0)
		checkDatabase(s, filepath.Join(dest, "db_a"), 277)
		checkDatabase(s, filepath.Join(dest, "db_b"), 50)
		s.passed("loss_of_all_local_database_and_coordinator_files_recovers_from_archive")
		ops := storage.Metrics()
		require(num(ops["LIST"]) == 0 && num(ops["metadata_db_operations"]) == 0, "LIST/DB unexpectedly used")
		s.passed("archive_recovery_compaction_use_no_list_or_metadata_db")
		s.report["operations"] = ops
		head, _ = archive.Load(0)
		s.report["latest_revision"] = head["revision"]
	})
}
