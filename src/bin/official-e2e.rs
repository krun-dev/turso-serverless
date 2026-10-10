#[path = "support/file_backend.rs"]
mod file_backend;
use file_backend::FileBackend;
use serde_json::json;
use sqltest::backends::{BackendError, DatabaseInstance, SqlBackend};
use sqltest::parser::ast::{Backend, Capability, DatabaseConfig};
use sqltest::{
    backends::rust::RustBackend, generate_integrity_fixture, load_test_files, DatabaseLocation,
    DefaultDatabaseResolver, DefaultDatabases, RunnerConfig, SnapshotUpdateMode, TestRunner,
    INTEGRITY_FIXTURE_RELATIVE_PATHS,
};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
};

struct StrictBaseline(RustBackend);
#[async_trait::async_trait]
impl SqlBackend for StrictBaseline {
    fn name(&self) -> &str {
        "rust-strict"
    }
    fn backend_type(&self) -> Backend {
        Backend::Rust
    }
    fn supports_snapshots(&self) -> bool {
        true
    }
    fn capabilities(&self) -> std::collections::HashSet<Capability> {
        let mut caps = self.0.capabilities();
        caps.insert(Capability::Strict);
        caps
    }
    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        self.0.create_database(config).await
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mode = &args[1];
    let root = PathBuf::from(&args[2]);
    let addr = &args[3];
    let paths: Vec<PathBuf> = args[4..].iter().map(PathBuf::from).collect();
    std::fs::create_dir_all(&root)?;
    let mut loaded = load_test_files(&paths).await?;
    // WAL-only task scope: upstream contains tests that explicitly switch modes.
    let mut excluded = 0usize;
    for (path, file) in &mut loaded.files {
        let excluded_file = path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains("mvcc"));
        file.tests.retain(|case| {
            let keep = !excluded_file
                && !case.sql.to_ascii_lowercase().contains("mvcc")
                && !case.modifiers.setups.iter().any(|r| {
                    file.setups
                        .get(&r.name)
                        .is_some_and(|sql| sql.to_ascii_lowercase().contains("mvcc"))
                });
            if !keep {
                excluded += 1;
            }
            keep
        });
        file.matrices.retain(|case| {
            !excluded_file && !case.sql_template.to_ascii_lowercase().contains("mvcc")
        });
    }
    eprintln!("WAL scope: excluded {excluded} cases explicitly referencing the other journal mode");
    if std::env::var("E2E_STRICT").is_ok() {
        for (_, file) in &mut loaded.files {
            let global = file
                .global_requires
                .iter()
                .any(|r| r.capability == Capability::Strict);
            file.tests.retain(|c| {
                global
                    || c.modifiers
                        .requires
                        .iter()
                        .any(|r| r.capability == Capability::Strict)
            });
            file.snapshots.retain(|c| {
                global
                    || c.modifiers
                        .requires
                        .iter()
                        .any(|r| r.capability == Capability::Strict)
            });
            file.matrices.retain(|c| {
                global
                    || c.modifiers
                        .requires
                        .iter()
                        .any(|r| r.capability == Capability::Strict)
            });
        }
    }
    if std::env::var("E2E_SKIPPED_DIAGNOSTICS").is_ok() {
        for (_, file) in &mut loaded.files {
            file.tests.retain(|c| {
                c.modifiers.skip.iter().any(|s| s.condition.is_none())
                    || file.global_skip.iter().any(|s| s.condition.is_none())
            });
            for case in &mut file.tests {
                case.modifiers.skip.clear();
            }
            file.global_skip.clear();
            file.snapshots.clear();
            file.matrices.clear();
            if std::env::var("E2E_SKIPPED_PERSISTENT").is_ok() {
                for db in &mut file.databases {
                    if matches!(db.location, DatabaseLocation::Memory) {
                        db.location = DatabaseLocation::TempFile;
                    }
                }
            }
        }
    }
    println!(
        "{}",
        json!({"kind":"scope", "mode":mode, "explicit_mvcc_cases_excluded":excluded,
        "snapshots_enabled":std::env::var("E2E_FULL").is_ok(), "cross_check_integrity":std::env::var("E2E_FULL").is_ok()})
    );
    if mode == "remote-pages" || mode == "local-select" {
        for (_, file) in &mut loaded.files {
            file.matrices.clear();
            file.snapshots.clear();
            file.tests.retain(|case| {
                let mut parser = turso_parser::parser::Parser::new(case.sql.as_bytes());
                let mut any = false;
                loop {
                    match parser.next() {
                        Some(Ok(turso_parser::ast::Cmd::Stmt(
                            turso_parser::ast::Stmt::Select(_),
                        ))) => any = true,
                        None => return any,
                        _ => return false,
                    }
                }
            });
        }
        loaded.files.retain(|(_, f)| !f.tests.is_empty());
    }
    if let Ok(filter) = std::env::var("E2E_FILTER") {
        for (_, file) in &mut loaded.files {
            file.tests.retain(|case| case.name == filter);
            file.matrices.clear();
        }
    }
    let mut fixtures = std::collections::BTreeSet::new();
    for file in loaded.test_files() {
        for rel in INTEGRITY_FIXTURE_RELATIVE_PATHS {
            if file.tests.iter().any(|case| case.sql.contains(rel))
                || file.setups.values().any(|sql| sql.contains(rel))
            {
                fixtures.insert(*rel);
            }
        }
        for db in &file.databases {
            if let DatabaseLocation::Path(path) = &db.location {
                for rel in INTEGRITY_FIXTURE_RELATIVE_PATHS {
                    if path == Path::new(rel) {
                        fixtures.insert(*rel);
                    }
                }
            }
        }
    }
    for rel in fixtures {
        generate_integrity_fixture(Path::new(rel), rel).await?;
    }
    // ATTACH uses relative fixture paths inside SQL, in addition to @database.
    if mode.starts_with("remote-") && Path::new("database").is_dir() {
        std::fs::create_dir_all(root.join("database"))?;
        for entry in std::fs::read_dir("database")? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "db") {
                std::fs::copy(entry.path(), root.join("database").join(entry.file_name()))?;
            }
        }
    }
    let needs = DefaultDatabases::scan_needs(loaded.test_files());
    let defaults = DefaultDatabases::generate(needs, 42, 10000, false)
        .await?
        .map(Arc::new);
    let resolver = defaults
        .as_ref()
        .map(|v| v.clone() as Arc<dyn DefaultDatabaseResolver>);
    let jobs = if mode == "remote-pages" || mode == "local-select" || mode == "remote-published" {
        1
    } else {
        std::env::var("E2E_JOBS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8)
    };
    let mut config = RunnerConfig::default()
        .with_max_jobs(jobs)
        .with_snapshot_update_mode(SnapshotUpdateMode::No);
    if std::env::var("E2E_FULL").is_ok() {
        config = config.with_cross_check_binary(PathBuf::from("/usr/bin/sqlite3"));
    } else {
        config = config.with_snapshot_filter("__never__");
    }
    let mut count = 0usize;
    let callback = |result: &sqltest::TestResult| {
        count += 1;
        println!(
            "{}",
            json!({"kind":"test", "mode":mode, "file":result.file, "name":result.name,
            "database":result.database.location.to_string(), "outcome":format!("{:?}", result.outcome),
            "passed":result.outcome.is_passed(), "skipped":result.outcome.is_skipped(),
            "failed":result.outcome.is_failed(), "error":result.outcome.is_error(),
            "duration_ms":result.duration.as_millis()})
        );
        std::io::stdout().flush().unwrap();
        if count % 100 == 0 {
            eprintln!("{mode}: {count} cases completed");
        }
    };
    let start = std::time::Instant::now();
    let results = if mode == "official" {
        let mut backend = RustBackend::new();
        if let Some(r) = resolver {
            backend = backend.with_default_db_resolver(r);
        }
        if std::env::var("E2E_STRICT").is_ok() {
            TestRunner::new(StrictBaseline(backend))
                .with_config(config)
                .run_loaded_tests(loaded, callback)
                .await?
        } else {
            TestRunner::new(backend)
                .with_config(config)
                .run_loaded_tests(loaded, callback)
                .await?
        }
    } else {
        let backend = FileBackend {
            addr: mode.starts_with("remote-").then(|| addr.clone()),
            root,
            resolver,
            next: AtomicUsize::new(0),
            pages: mode == "remote-pages",
            published: mode == "remote-published",
            cloud: mode == "remote-cloud",
        };
        TestRunner::new(backend)
            .with_config(config)
            .run_loaded_tests(loaded, callback)
            .await?
    };
    let summary = sqltest::summarize(start, &results);
    println!(
        "{}",
        json!({"kind":"summary", "mode":mode, "total":summary.total,
        "passed":summary.passed, "failed":summary.failed, "skipped":summary.skipped,
        "errors":summary.errors, "snapshots_new":summary.snapshots_new, "snapshots_updated":summary.snapshots_updated,
        "duration_ms":summary.duration.as_millis()})
    );
    if summary.failed > 0
        || summary.errors > 0
        || summary.snapshots_new > 0
        || summary.snapshots_updated > 0
    {
        std::process::exit(1);
    }
    Ok(())
}
