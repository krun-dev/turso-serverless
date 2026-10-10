// Adapted from Turso testing/sqltest/src/backends/rust.rs at 8cc059bd.
// Copyright 2025 the Turso authors. All rights reserved. MIT license.
// Query execution and output formatting are copied unchanged from the official backend.
use async_trait::async_trait;
use sqltest::backends::{
    BackendError, DatabaseFileHandle, DatabaseInstance, DefaultDatabaseResolver, QueryResult,
    SqlBackend,
};
use sqltest::parser::ast::{Backend, Capability, DatabaseConfig, DatabaseLocation};
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tempfile::NamedTempFile;
use turso::{Builder, Connection, Database, Value};
use turso_pageserver_probe::{RemoteIo, RoutedIo};

pub struct FileBackend {
    pub addr: Option<String>,
    pub root: PathBuf,
    pub resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
    pub next: AtomicUsize,
    pub pages: bool,
    pub published: bool,
    pub cloud: bool,
}
#[async_trait]
impl SqlBackend for FileBackend {
    fn name(&self) -> &str {
        if self.addr.is_some() {
            "remote-file"
        } else {
            "local-file"
        }
    }
    fn backend_type(&self) -> Backend {
        Backend::Rust
    }
    fn supports_snapshots(&self) -> bool {
        true
    }
    fn capabilities(&self) -> HashSet<Capability> {
        let mut caps = HashSet::from([
            Capability::Trigger,
            Capability::MaterializedViews,
            Capability::CustomTypes,
        ]);
        if std::env::var("E2E_STRICT").is_ok() {
            caps.insert(Capability::Strict);
        }
        caps
    }
    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        if self.cloud {
            return self.create_cloud(config).await;
        }
        let name = if self.pages || self.published {
            remote_admin(self.addr.as_ref().unwrap(), "reset_publication")?;
            for name in ["live.db", "live.db-wal", "acked.wal", "baseline.db"] {
                let _ = std::fs::remove_file(self.root.join(name));
            }
            "live.db".to_owned()
        } else {
            format!("case-{}.db", self.next.fetch_add(1, Ordering::SeqCst))
        };
        let path = self.root.join(&name);
        let source = match &config.location {
            DatabaseLocation::Memory | DatabaseLocation::TempFile => None,
            DatabaseLocation::Path(p) => Some(p.clone()),
            loc => self.resolver.as_ref().and_then(|r| r.resolve(loc)),
        };
        if let Some(src) = source {
            std::fs::copy(&src, &path).map_err(|e| {
                BackendError::CreateDatabase(format!("fixture {}: {e}", src.display()))
            })?;
        } else if matches!(
            config.location,
            DatabaseLocation::Default | DatabaseLocation::DefaultNoRowidAlias
        ) {
            return Err(BackendError::CreateDatabase(
                "missing default fixture".into(),
            ));
        }
        let db_path = if self.addr.is_some() {
            name
        } else {
            path.to_string_lossy().to_string()
        };
        let mut builder = Builder::new_local(&db_path)
            .experimental_attach(true)
            .experimental_index_method(true)
            .experimental_materialized_views(true)
            .experimental_custom_types(true)
            .experimental_generated_columns(true)
            .experimental_vacuum(true)
            .experimental_without_rowid(true);
        // Match official RustBackend: it does not apply DatabaseConfig.readonly.
        if let Some(addr) = &self.addr {
            builder = builder.with_io_impl(Arc::new(RemoteIo { addr: addr.clone() }));
        }
        let db = builder
            .build()
            .await
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        let conn = db
            .connect()
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        let instance = RustDatabaseInstance {
            _db: db,
            conn,
            _temp_file: None,
        };
        if self.pages || self.published {
            instance.execute_query("PRAGMA user_version=1; PRAGMA user_version=0; PRAGMA wal_checkpoint(TRUNCATE); PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;")
                .await.map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
            let addr = self.addr.clone().unwrap();
            remote_admin(&addr, "baseline")?;
            if !self.pages {
                return Ok(Box::new(instance));
            }
            return Ok(Box::new(PageInstance {
                writer: instance,
                addr,
                reader: None,
            }));
        }
        Ok(Box::new(instance))
    }
}

fn configured(path: &str) -> Builder {
    Builder::new_local(path)
        .experimental_attach(true)
        .experimental_index_method(true)
        .experimental_materialized_views(true)
        .experimental_custom_types(true)
        .experimental_generated_columns(true)
        .experimental_vacuum(true)
        .experimental_without_rowid(true)
}

impl FileBackend {
    async fn create_cloud(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        let namespace = format!("case-{}", self.next.fetch_add(1, Ordering::SeqCst));
        let root = self.root.join(&namespace);
        std::fs::create_dir_all(&root).map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        let source = match &config.location {
            DatabaseLocation::Memory | DatabaseLocation::TempFile => None,
            DatabaseLocation::Path(p) => Some(p.clone()),
            loc => self.resolver.as_ref().and_then(|r| r.resolve(loc)),
        };
        if let Some(src) = source {
            std::fs::copy(&src, root.join("live.db")).map_err(|e| {
                BackendError::CreateDatabase(format!("fixture {}: {e}", src.display()))
            })?;
        }
        let addr = self.addr.clone().unwrap();
        let initial_name = format!("{namespace}.db");
        let io = |main: String| {
            Arc::new(RoutedIo {
                inner: RemoteIo { addr: addr.clone() },
                namespace: namespace.clone(),
                main,
            })
        };
        let db = configured(&initial_name)
            .with_io_impl(io(initial_name.clone()))
            .build()
            .await
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        let conn = db
            .connect()
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        let initial = RustDatabaseInstance {
            _db: db,
            conn,
            _temp_file: None,
        };
        drop(initial);
        routed_admin(&addr, &namespace, "baseline")?;
        routed_admin(&addr, &namespace, "cloud_start")?;
        let logical = if matches!(config.location, DatabaseLocation::Memory) {
            format!(":memory:{namespace}")
        } else {
            initial_name
        };
        let db = configured(&logical)
            .with_io_impl(io(logical.clone()))
            .build()
            .await
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        let conn = db
            .connect()
            .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
        Ok(Box::new(CloudInstance {
            writer: RustDatabaseInstance {
                _db: db,
                conn,
                _temp_file: None,
            },
            addr,
            namespace,
            root,
        }))
    }
}

struct CloudInstance {
    writer: RustDatabaseInstance,
    addr: String,
    namespace: String,
    root: PathBuf,
}

#[async_trait]
impl DatabaseInstance for CloudInstance {
    async fn execute_setup(&mut self, sql: &str) -> Result<(), BackendError> {
        self.writer.execute_setup(sql).await
    }
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        self.writer.execute(sql).await
    }
    async fn close(self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        let Self {
            writer,
            addr,
            namespace,
            root,
        } = *self;
        drop(writer);
        routed_admin(&addr, &namespace, "cloud_export")?;
        let path = root.join("live.db");
        let file = std::fs::File::open(&path).map_err(|e| BackendError::Execute(e.to_string()))?;
        #[allow(deprecated)]
        let temp = NamedTempFile::from_parts(file, tempfile::TempPath::from_path(path));
        routed_admin(&addr, &namespace, "cloud_release")?;
        // Upstream consumes this handle for its annotated SQLite cross-checks.
        // Its drop unlinks live.db; the router then reclaims this case directory.
        Ok(DatabaseFileHandle::temp(temp))
    }
}
/// A database instance backed by Turso Rust bindings
pub struct RustDatabaseInstance {
    /// The database handle (kept alive for the connection)
    _db: Database,
    /// The connection to execute queries on
    conn: Connection,
    /// Keep temp file alive - deleted when dropped
    _temp_file: Option<NamedTempFile>,
}

impl RustDatabaseInstance {
    /// Execute SQL (which may contain multiple statements) and collect results
    /// Results are returned from all SELECT/PRAGMA statements, concatenated together
    async fn execute_query(&self, sql: &str) -> Result<QueryResult, turso::Error> {
        let mut all_rows = Vec::new();
        let mut remaining = sql;

        // FIXME: Overhead of having to parse the query twice, but at least we are correct
        // Use turso_parser to properly split SQL into statements
        while !remaining.trim().is_empty() {
            let mut parser = turso_parser::parser::Parser::new(remaining.as_bytes());

            match parser.next() {
                Some(Ok(_cmd)) => {
                    // Get the offset where the parser stopped (after this statement)
                    let offset = parser.offset();

                    // Extract the statement SQL (everything up to the offset)
                    let stmt_sql = &remaining[..offset].trim();

                    if !stmt_sql.is_empty() {
                        // Prepare and execute the statement
                        let mut stmt = self.conn.prepare(stmt_sql).await?;

                        // Use query() which works for both SELECT and non-SELECT statements
                        let mut rows_result = stmt.query(()).await?;
                        while let Some(row) = rows_result.next().await? {
                            let mut row_values = Vec::new();
                            let col_count = row.column_count();
                            for i in 0..col_count {
                                let value = row.get_value(i)?;
                                row_values.push(value_to_string(&value));
                            }
                            all_rows.push(row_values);
                        }
                    }

                    // Move to the remaining SQL
                    remaining = &remaining[offset..];
                }
                Some(Err(e)) => {
                    return Err(turso::Error::Error(format!("Parse error: {e}")));
                }
                None => {
                    // No more statements
                    break;
                }
            }
        }

        Ok(QueryResult::success(all_rows))
    }
}

#[async_trait]
impl DatabaseInstance for RustDatabaseInstance {
    async fn execute_setup(&mut self, sql: &str) -> Result<(), BackendError> {
        // Use execute_batch for setup SQL (may contain multiple statements)
        self.conn
            .execute_batch(sql)
            .await
            .map_err(|e| BackendError::Execute(e.to_string()))?;
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        // Try to execute as a query to capture results
        match self.execute_query(sql).await {
            Ok(result) => Ok(result),
            Err(e) => {
                // Return error as QueryResult (not BackendError)
                // This matches how the test framework expects errors
                Ok(QueryResult::error(e.to_string()))
            }
        }
    }

    async fn close(self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        // Connection and database are dropped automatically
        match self._temp_file {
            Some(tf) => Ok(DatabaseFileHandle::temp(tf)),
            None => Ok(DatabaseFileHandle::none()),
        }
    }
}

/// Convert a Turso Value to its string representation
///
/// This matches the output format expected by the test framework:
/// - NULL becomes empty string (matching CLI list mode)
/// - Integer and Real use standard formatting
/// - Text is used as-is
/// - Blob is converted to UTF-8 string (matching CLI list mode behavior)
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format_real(*f),
        Value::Text(s) => s.clone(),
        Value::Blob(bytes) => String::from_utf8_lossy(bytes).to_string(),
    }
}

/// Format a floating-point number to match SQLite's output
/// SQLite uses %!.15g format (15 significant digits, no trailing zeros)
/// Also used by the matrix oracle so both engines' reals are rendered
/// through the identical formatter, making text comparison value-exact.
pub(crate) fn format_real(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_positive() {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }

    // Use exponential notation for very large or very small numbers (like SQLite's %g)
    let abs_f = f.abs();
    if abs_f != 0.0 && !(1e-4..1e15).contains(&abs_f) {
        // Use exponential notation with up to 15 significant digits
        let formatted = format!("{f:.14e}");
        // Clean up: remove trailing zeros in mantissa and unnecessary + in exponent
        let formatted = clean_exponential(&formatted);
        return formatted;
    }

    // Check if it's a whole number that can be represented exactly
    if f.fract() == 0.0 {
        format!("{}.0", f as i64)
    } else {
        // Format with 15 significant digits to match SQLite's %!.15g format
        // Then remove trailing zeros after the decimal point
        format_with_significant_digits(f, 15)
    }
}

/// Format a float with a specific number of significant digits, removing trailing zeros
fn format_with_significant_digits(f: f64, sig_digits: usize) -> String {
    if f == 0.0 {
        return "0.0".to_string();
    }

    let abs_f = f.abs();
    // Count digits before decimal point
    let digits_before_decimal = if abs_f >= 1.0 {
        (abs_f.log10().floor() as usize) + 1
    } else {
        0
    };

    // Calculate decimal places needed for the desired significant digits
    let decimal_places = sig_digits.saturating_sub(digits_before_decimal);

    // Format with calculated decimal places
    let formatted = format!("{f:.decimal_places$}");

    // Remove trailing zeros after decimal point, but keep at least one digit after decimal
    if formatted.contains('.') {
        let trimmed = formatted.trim_end_matches('0');
        if trimmed.ends_with('.') {
            format!("{trimmed}0")
        } else {
            trimmed.to_string()
        }
    } else {
        formatted
    }
}

/// Clean up exponential notation to match SQLite's format
fn clean_exponential(s: &str) -> String {
    // Input format: "-1.23456789012340e+18" or "1.23456789012340e-05"
    if let Some(e_pos) = s.find('e') {
        let mantissa = &s[..e_pos];
        let exponent = &s[e_pos + 1..];

        // Remove trailing zeros from mantissa (but keep at least one digit after decimal)
        let mantissa = mantissa.trim_end_matches('0');
        let mantissa = if mantissa.ends_with('.') {
            format!("{mantissa}0")
        } else {
            mantissa.to_string()
        };

        // Parse and format exponent (remove leading zeros and +)
        let exp_num: i32 = exponent.parse().unwrap_or(0);
        format!("{mantissa}e{exp_num:+}")
    } else {
        s.to_string()
    }
}

// Read-only official SELECT cases: setup writes through the same engine; queries
// use a new connection to a server-composed immutable WAL commit version.
struct PageInstance {
    writer: RustDatabaseInstance,
    addr: String,
    reader: Option<RustDatabaseInstance>,
}
fn remote_admin(addr: &str, method: &str) -> Result<serde_json::Value, BackendError> {
    routed_admin(addr, "", method)
}
fn routed_admin(
    addr: &str,
    namespace: &str,
    method: &str,
) -> Result<serde_json::Value, BackendError> {
    use std::io::{BufRead, BufReader, Write};
    let result = (|| -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut sock = std::net::TcpStream::connect(addr)?;
        sock.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
        writeln!(
            sock,
            "{}",
            serde_json::json!({"op":"admin", "path":if namespace.is_empty() { "control".to_owned() } else {format!("{namespace}/control")}, "pos":0, "len":0, "extra":{"method":method}})
        )?;
        let mut line = String::new();
        BufReader::new(sock).read_line(&mut line)?;
        let value: serde_json::Value = serde_json::from_str(&line)?;
        if value.get("error").is_some() {
            return Err(value.to_string().into());
        }
        Ok(value)
    })();
    result.map_err(|e| BackendError::Execute(e.to_string()))
}
#[async_trait]
impl DatabaseInstance for PageInstance {
    async fn execute_setup(&mut self, sql: &str) -> Result<(), BackendError> {
        self.writer.execute_setup(sql).await
    }
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        if self.reader.is_none() {
            let stats = remote_admin(&self.addr, "stats")?;
            let name = stats["views"]
                .as_array()
                .and_then(|v| v.last())
                .and_then(|v| v.as_str())
                .ok_or_else(|| BackendError::Execute("missing committed page version".into()))?;
            eprintln!("page_snapshot={name}");
            let db = Builder::new_local(name)
                .with_io_impl(Arc::new(RemoteIo {
                    addr: self.addr.clone(),
                }))
                .read_only(true)
                .experimental_attach(true)
                .experimental_index_method(true)
                .experimental_materialized_views(true)
                .experimental_custom_types(true)
                .experimental_generated_columns(true)
                .experimental_vacuum(true)
                .experimental_without_rowid(true)
                .build()
                .await
                .map_err(|e| BackendError::Execute(e.to_string()))?;
            let conn = db
                .connect()
                .map_err(|e| BackendError::Execute(e.to_string()))?;
            self.reader = Some(RustDatabaseInstance {
                _db: db,
                conn,
                _temp_file: None,
            });
        }
        self.reader.as_mut().unwrap().execute(sql).await
    }
    async fn close(self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        Ok(DatabaseFileHandle::none())
    }
}
