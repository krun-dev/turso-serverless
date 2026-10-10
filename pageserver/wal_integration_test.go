package pageserver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWAL(t *testing.T) {
	integration(t, "wal", func(s *suite) {
		l := newLab(s, "")
		p := l.open("live.db", false)
		l.full(p)
		l.sql(p, "CREATE TABLE t(id INTEGER PRIMARY KEY,payload TEXT)")
		l.sql(p, "BEGIN")
		for start := 1; start < 513; start += 32 {
			values := []string{}
			for i := start; i < start+32; i++ {
				values = append(values, fmt.Sprintf("(%d,'%08d%s')", i, i, strings.Repeat("x", 2040)))
			}
			l.sql(p, "INSERT INTO t VALUES "+strings.Join(values, ","))
		}
		l.sql(p, "COMMIT")
		l.sql(p, "PRAGMA wal_checkpoint(TRUNCATE)")
		l.admin("baseline")
		baseBytes := must(os.Stat(filepath.Join(l.root, "baseline.db"))).Size()
		l.admin("reset_stats")
		r := l.open("view-0.db", true)
		l.assertScalar(r, "SELECT length(payload) FROM t WHERE id=256", "Integer(2048)")
		cold := readStats(l, "view-0.db")
		l.admin("reset_stats")
		l.assertScalar(r, "SELECT length(payload) FROM t WHERE id=256", "Integer(2048)")
		warm := readStats(l, "view-0.db")
		require(num(warm["requests"]) == 0 && num(cold["bytes"]) < baseBytes/10, "lazy/cache reads wrong")
		l.admin("reset_stats")
		l.assertScalar(r, "SELECT sum(length(payload)) FROM t", "Integer(1048576)")
		s.passed("lazy_reads_and_cache", Document{"base_bytes": baseBytes, "cold": cold, "warm": warm, "scan": readStats(l, "view-0.db")})
		r.stop()
		samples := []any{}
		for _, delay := range []int{0, 1, 5} {
			l.admin("delay", Document{"ms": delay})
			l.admin("reset_stats")
			r = l.open("view-0.db", true)
			reply := l.sql(r, "SELECT length(payload) FROM t WHERE id=400")
			samples = append(samples, Document{"delay_per_read_ms": delay, "query_elapsed_us": reply["elapsed_us"], "reads_including_open": readStats(l, "view-0.db")["requests"]})
			r.stop()
		}
		l.admin("delay", Document{"ms": 0})
		s.passed("latency_probe", Document{"samples": samples, "note": "single samples, not a benchmark"})
		l.sql(p, "@1 BEGIN")
		l.assertScalar(p, "@1 SELECT length(payload) FROM t WHERE id=1", "Integer(2048)")
		l.sql(p, "BEGIN")
		l.sql(p, "UPDATE t SET payload='v1' WHERE id=1")
		l.admin("fault", Document{"mode": "delay"})
		p.send("COMMIT")
		waitPath(filepath.Join(l.root, "barrier-entered"))
		reply := make(chan Document, 1)
		go func() { reply <- p.receive() }()
		select {
		case <-reply:
			panic(fmt.Errorf("commit returned before barrier"))
		case <-time.After(50 * time.Millisecond):
		}
		require(equal(l.admin("stats")["views"], []any{"view-0.db"}), "unconfirmed view published")
		check(os.WriteFile(filepath.Join(l.root, "barrier-release"), nil, 0600))
		require(flag((<-reply)["ok"]), "commit failed")
		view1 := l.latest()
		s.passed("commit_waits_for_remote_durable_publication")
		l.assertScalar(p, "@1 SELECT length(payload) FROM t WHERE id=1", "Integer(2048)")
		require(strings.Contains(l.scalar(p, "@2 SELECT payload FROM t WHERE id=1"), `value: "v1"`), "new reader missing commit")
		l.sql(p, "PRAGMA wal_checkpoint(PASSIVE)")
		l.assertScalar(p, "@1 SELECT length(payload) FROM t WHERE id=1", "Integer(2048)")
		l.sql(p, "@1 COMMIT")
		s.passed("engine_reader_pinned_across_commit_and_checkpoint")
		for _, sql := range []string{"BEGIN", "SAVEPOINT s", "INSERT INTO t VALUES(514,'rolled-back')", "ROLLBACK TO s", "RELEASE s", "INSERT INTO t VALUES(513,'v2')", "COMMIT"} {
			l.sql(p, sql)
		}
		view2 := l.latest()
		for _, v := range []struct {
			name          string
			count, length int
		}{{"view-0.db", 512, 2048}, {view1, 512, 2}, {view2, 513, 2}} {
			r = l.open(v.name, true)
			l.count(r, v.count)
			l.assertScalar(r, "SELECT length(payload) FROM t WHERE id=1", fmt.Sprintf("Integer(%d)", v.length))
			l.assertScalar(r, "SELECT count(*) FROM t WHERE id=514", "Integer(0)")
			l.integrity(r)
			r.stop()
		}
		s.passed("versioned_pages_and_savepoint_rollback")
		raw := readFile(filepath.Join(l.root, "acked.wal"))
		w := ParseWAL(raw)
		corrupt := append([]byte{}, raw...)
		corrupt[len(corrupt)-1] ^= 1
		for _, tail := range [][]byte{raw[:len(raw)-100], corrupt, raw[:int64(len(raw))-w.PageSize-24]} {
			require(len(ParseWAL(tail).Commits) < len(w.Commits), "invalid WAL suffix admitted")
		}
		s.passed("wal_tail_checksum_and_transaction_boundary")
		check(os.WriteFile(filepath.Join(s.run, "committed.wal"), raw, 0600))
		p = l.restart()
		l.count(p, 513)
		r = l.open(view1, true)
		l.count(r, 512)
		r.stop()
		s.passed("compute_and_storage_sigkill_recovery")
		l.full(p)
		l.sql(p, "BEGIN")
		l.sql(p, "INSERT INTO t VALUES(600,'must-not-publish')")
		l.admin("fault", Document{"mode": "before"})
		l.sql(p, "COMMIT", false)
		require(len(ParseWAL(readFile(filepath.Join(l.root, "live.db-wal"))).Commits) > len(ParseWAL(readFile(filepath.Join(l.root, "acked.wal"))).Commits), "negative recovery fixture missing")
		p = l.restart()
		l.assertScalar(p, "SELECT count(*) FROM t WHERE id=600", "Integer(0)")
		s.passed("failure_before_remote_publication")
		l.full(p)
		l.sql(p, "BEGIN")
		l.sql(p, "INSERT INTO t VALUES(601,'reply-lost')")
		l.admin("fault", Document{"mode": "after"})
		l.sql(p, "COMMIT", false)
		p = l.restart()
		l.assertScalar(p, "SELECT count(*) FROM t WHERE id=601", "Integer(1)")
		s.passed("failure_after_remote_publication", Document{"meaning": "an error does not prove rollback"})
		normal := l.open("normal.db", false)
		l.full(normal)
		l.sql(normal, "CREATE TABLE n(id INTEGER)")
		l.sql(normal, "PRAGMA synchronous=NORMAL")
		l.admin("reset_stats")
		l.sql(normal, "INSERT INTO n VALUES(1)")
		normalSyncs := syncs(l, "normal.db-wal")
		require(normalSyncs == 0, "NORMAL synced unexpectedly")
		l.sql(normal, "PRAGMA synchronous=FULL")
		l.admin("reset_stats")
		l.sql(normal, "INSERT INTO n VALUES(2)")
		require(syncs(l, "normal.db-wal") >= 1, "FULL did not sync")
		l.sql(normal, "PRAGMA wal_checkpoint(TRUNCATE)")
		require(ParseWAL(readFile(filepath.Join(l.root, "normal.db-wal"))).Epoch == nil, "truncate did not reset epoch")
		l.sql(normal, "INSERT INTO n VALUES(3)")
		require(ParseWAL(readFile(filepath.Join(l.root, "normal.db-wal"))).Epoch != nil, "epoch missing after reuse")
		l.assertScalar(normal, "SELECT count(*) FROM n", "Integer(3)")
		s.passed("normal_vs_full_and_wal_reuse")
	})
}

func TestNegative(t *testing.T) {
	integration(t, "negative", func(s *suite) {
		for _, mode := range []string{"latest", "raw-recovery"} {
			l := newLab(s, "")
			l.shutdown()
			l.negative = mode
			l.start()
			p := l.open("live.db", false)
			l.full(p)
			l.sql(p, "CREATE TABLE t(id INTEGER PRIMARY KEY,payload TEXT)")
			l.sql(p, "INSERT INTO t VALUES(1,'baseline')")
			l.sql(p, "PRAGMA wal_checkpoint(TRUNCATE)")
			l.admin("baseline")
			l.sql(p, "INSERT INTO t VALUES(2,'confirmed')")
			if mode == "latest" {
				r := l.open("view-0.db", true)
				got := l.scalar(r, "SELECT count(*) FROM t")
				require(got == "Integer(2)", "unsafe latest control did not produce mixed version")
				s.passed(mode, Document{"rejected_with": "fixed_version_mixed_with_latest"})
			} else {
				l.sql(p, "BEGIN")
				l.sql(p, "INSERT INTO t VALUES(3,'unconfirmed')")
				l.admin("fault", Document{"mode": "before"})
				l.sql(p, "COMMIT", false)
				p = l.restart()
				l.assertScalar(p, "SELECT count(*) FROM t WHERE id=3", "Integer(1)")
				s.passed(mode, Document{"rejected_with": "unacknowledged_wal_recovered"})
			}
			l.close()
		}
	})
}

func TestGenerations(t *testing.T) {
	integration(t, "generation", func(s *suite) {
		l := newLab(s, "")
		p := l.open("live.db", false)
		l.full(p)
		l.sql(p, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT)")
		v := []string{}
		for i := 0; i < 80; i++ {
			v = append(v, "('"+strings.Repeat("x", 2000)+"')")
		}
		l.sql(p, "INSERT INTO t(payload) VALUES "+strings.Join(v, ","))
		for _, sql := range []string{"PRAGMA user_version=42", "PRAGMA application_id=99", "PRAGMA wal_checkpoint(TRUNCATE)"} {
			l.sql(p, sql)
		}
		l.admin("baseline")
		snapshots := map[string]any{"view-0.db": l.rows(p, "SELECT id,length(payload) FROM t ORDER BY id")}
		l.sql(p, "DELETE FROM t WHERE id BETWEEN 2 AND 79")
		snapshots[l.latest()] = l.rows(p, "SELECT id,length(payload) FROM t ORDER BY id")
		pinned := l.open("view-0.db", true)
		l.sql(pinned, "BEGIN")
		l.count(pinned, 80)
		before := must(os.Stat(filepath.Join(l.root, "live.db"))).Size()
		checkViews := func() {
			for name, rows := range snapshots {
				r := l.open(name, true)
				require(equal(l.rows(r, "SELECT id,length(payload) FROM t ORDER BY id"), rows), "historical rows changed")
				l.integrity(r)
				r.stop()
			}
		}
		for i := 0; i < 2; i++ {
			l.sql(p, "VACUUM")
			require(strings.HasPrefix(l.latest(), fmt.Sprintf("view-g%d-", i+1)), "VACUUM did not rotate generation")
			snapshots[l.latest()] = l.rows(p, "SELECT id,length(payload) FROM t ORDER BY id")
			l.assertScalar(p, "PRAGMA user_version", "Integer(42)")
			l.assertScalar(p, "PRAGMA application_id", "Integer(99)")
			l.assertScalar(p, "SELECT seq FROM sqlite_sequence WHERE name='t'", fmt.Sprintf("Integer(%d)", 80+i))
			l.sql(p, "INSERT INTO t(payload) VALUES('after-vacuum')")
			l.assertScalar(p, "SELECT max(id) FROM t", fmt.Sprintf("Integer(%d)", 81+i))
			snapshots[l.latest()] = l.rows(p, "SELECT id,length(payload) FROM t ORDER BY id")
			l.count(pinned, 80)
			checkViews()
		}
		after := must(os.Stat(filepath.Join(l.root, "live.db"))).Size()
		require(after < before, "VACUUM did not shrink")
		pinned.stop()
		s.passed("repeated_vacuum_and_post_vacuum_writes", Document{"before_bytes": before, "after_bytes": after})
		s.passed("old_snapshots_across_page_rewrite_and_generations", Document{"snapshots": len(snapshots)})
		for i, mode := range []string{"TRUNCATE", "RESTART"} {
			l.sql(p, "PRAGMA wal_checkpoint("+mode+")")
			l.sql(p, "INSERT INTO t(payload) VALUES('after-reset')")
			require(strings.HasPrefix(l.latest(), fmt.Sprintf("view-g%d-", 3+i)), "reset generation wrong")
			snapshots[l.latest()] = l.rows(p, "SELECT id,length(payload) FROM t ORDER BY id")
		}
		latestRows := l.rows(p, "SELECT id,length(payload) FROM t ORDER BY id")
		s.passed("truncate_and_restart_reuse")
		gens := l.admin("stats")["generations"]
		check(os.WriteFile(filepath.Join(l.root, "baseline.db"), []byte("stale baseline mirror"), 0600))
		check(os.WriteFile(filepath.Join(l.root, "acked.wal"), []byte("stale WAL mirror"), 0600))
		p = l.restart()
		require(equal(l.rows(p, "SELECT id,length(payload) FROM t ORDER BY id"), latestRows) && equal(l.admin("stats")["generations"], gens), "manifest authority regressed")
		checkViews()
		s.passed("retained_generations_after_process_restart")
		manifest := readFile(filepath.Join(l.root, "manifest.json"))
		g := doc(list(gens)[0])
		for _, name := range []string{"manifest.json", str(g["base"]), str(g["wal"])} {
			for _, op := range []string{"truncate", "remove", "write"} {
				reply := l.request(Request{Op: op, Path: name, Extra: Document{}}, nil)
				require(strings.Contains(str(reply["error"]), "immutable"), "metadata mutation accepted")
			}
		}
		assertBytes(readFile(filepath.Join(l.root, "manifest.json")), manifest)
		reply := l.request(Request{Op: "truncate", Path: "live.db-wal", Extra: Document{}}, nil)
		require(strings.Contains(str(reply["error"]), "synced DB image"), "unsafe reset accepted")
		require(must(os.Stat(filepath.Join(l.root, "live.db-wal"))).Size() > 0, "unsafe WAL destroyed")
		image := readFile(filepath.Join(l.root, "live.db"))
		image[len(image)-1] ^= 1
		check(os.WriteFile(filepath.Join(l.root, "live.db"), image, 0600))
		require(l.request(Request{Op: "sync", Path: "live.db", Extra: Document{}}, nil)["error"] == nil, "sync failed")
		reply = l.request(Request{Op: "truncate", Path: "live.db-wal", Extra: Document{}}, nil)
		require(strings.Contains(str(reply["error"]), "checkpoint does not cover published"), "uncovered reset accepted")
		assertBytes(readFile(filepath.Join(l.root, "manifest.json")), manifest)
		p = l.restart()
		require(equal(l.rows(p, "SELECT id,length(payload) FROM t ORDER BY id"), latestRows), "recovery lost publication")
		s.passed("unsafe_reset_rejected_without_covering_durable_base")
	})
}
