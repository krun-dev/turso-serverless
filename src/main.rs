use serde_json::json;
use std::io::{BufRead, Write};
use std::sync::Arc;
use turso_core::{Database, OpenFlags, OpenOptions, Result, SqliteDialect, StepResult, IO};
use turso_pageserver_probe::RemoteIo;

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let io = Arc::new(RemoteIo {
        addr: args[1].clone(),
    });
    let flags = if args.get(3).is_some_and(|s| s == "readonly") {
        OpenFlags::ReadOnly
    } else {
        OpenFlags::Create
    };
    let storage = Arc::new(turso_core::storage::database::DatabaseFile::new(
        io.open_file(&args[2], flags, false)?,
    ));
    let db = Database::open(
        io.clone(),
        &args[2],
        OpenOptions::new(Arc::new(SqliteDialect))
            .flags(flags)
            .db_opts(
                turso_core::DatabaseOpts::default()
                    .with_vacuum(true)
                    .with_attach(true),
            )
            .storage(storage),
    )?;
    let connections = [db.connect()?, db.connect()?, db.connect()?];
    println!("{}", json!({"ready": true}));
    std::io::stdout().flush()?;
    for line in std::io::stdin().lock().lines() {
        let line = line?;
        let (session, sql) = if let Some(rest) = line.strip_prefix('@') {
            let (session, sql) = rest.split_once(' ').expect("session SQL");
            (session.parse::<usize>()?, sql.to_owned())
        } else {
            (0, line)
        };
        let conn = &connections[session];
        let started = std::time::Instant::now();
        let result = run_sql(conn, &io, &sql);
        let reply = match result {
            Ok(rows) => {
                json!({"ok": true, "rows": rows, "elapsed_us": started.elapsed().as_micros()})
            }
            Err(error) => {
                json!({"ok": false, "error": error.to_string(), "elapsed_us": started.elapsed().as_micros()})
            }
        };
        println!("{reply}");
        std::io::stdout().flush()?;
    }
    std::process::exit(0);
}

fn run_sql(
    conn: &Arc<turso_core::Connection>,
    io: &Arc<RemoteIo>,
    sql: &str,
) -> Result<Vec<Vec<String>>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::Row => rows.push(
                stmt.row()
                    .unwrap()
                    .get_values()
                    .map(|v| format!("{v:?}"))
                    .collect(),
            ),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => io.step()?,
            StepResult::Done => break,
            other => {
                return Err(turso_core::LimboError::InternalError(format!(
                    "unexpected step {other:?}"
                )))
            }
        }
    }
    Ok(rows)
}
