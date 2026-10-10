use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use turso_core::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use turso_core::io::{FileId, FileSyncType};
use turso_core::{Buffer, Clock, Completion, CompletionError, File, OpenFlags, Result, IO};

pub struct RemoteIo {
    pub addr: String,
}

/// Keep the engine's logical database name while isolating persistent RPC files.
pub struct RoutedIo {
    pub inner: RemoteIo,
    pub namespace: String,
    pub main: String,
}
impl RoutedIo {
    fn mapped(&self, path: &str) -> String {
        let path_name = canonical(path);
        let main = canonical(&self.main);
        let name = if path_name == main {
            "live.db".to_owned()
        } else if path_name == format!("{main}-wal") {
            "live.db-wal".to_owned()
        } else {
            path_name
        };
        if is_local_temp(path) {
            name
        } else {
            format!("{}/{}", self.namespace, name)
        }
    }
}

fn canonical(path: &str) -> String {
    std::path::Path::new(path)
        .components()
        .collect::<std::path::PathBuf>()
        .to_string_lossy()
        .into_owned()
}
impl IO for RoutedIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.inner.open_file(&self.mapped(path), flags, direct)
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(&self.mapped(path))
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(&self.mapped(path))
    }
    fn step(&self) -> Result<()> {
        self.inner.step()
    }
}
impl Clock for RoutedIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

fn is_local_temp(path: &str) -> bool {
    let path = std::path::Path::new(path);
    path.is_absolute()
        && path.file_name().is_some_and(|name| {
            name == "tursodb_temp_file" || name.to_string_lossy().starts_with("etilqs_")
        })
}

impl IO for RemoteIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        // Engine spill files are ephemeral compute-local data, not durable database objects.
        if is_local_temp(path) {
            return turso_core::io::PlatformIO::new()?.open_file(path, flags, direct);
        }
        let file = RemoteFile {
            addr: self.addr.clone(),
            path: canonical(path),
            stream: Arc::new(Mutex::new(None)),
        };
        file.call("open", 0, 0, &[], json!({"create": flags.contains(OpenFlags::Create), "readonly": flags.contains(OpenFlags::ReadOnly)}))?;
        Ok(Arc::new(file))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        if is_local_temp(path) {
            return turso_core::io::PlatformIO::new()?.remove_file(path);
        }
        RemoteFile {
            addr: self.addr.clone(),
            path: canonical(path),
            stream: Arc::new(Mutex::new(None)),
        }
        .call("remove", 0, 0, &[], json!({}))?;
        Ok(())
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        if is_local_temp(path) {
            return turso_core::io::PlatformIO::new()?.file_id(path);
        }
        Ok(FileId::from_path_hash(&canonical(path)))
    }
    fn step(&self) -> Result<()> {
        std::thread::sleep(Duration::from_micros(100));
        Ok(())
    }
}

impl Clock for RemoteIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

#[derive(Clone)]
struct RemoteFile {
    addr: String,
    path: String,
    stream: Arc<Mutex<Option<BufReader<TcpStream>>>>,
}

impl File for RemoteFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let buffer = c.as_read().buf();
        let (_, data) = self.call("read", pos, buffer.len(), &[], json!({}))?;
        buffer.as_mut_slice()[..data.len()].copy_from_slice(&data);
        c.complete(data.len() as i32);
        Ok(c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.call("write", pos, buffer.len(), buffer.as_slice(), json!({}))?;
        c.complete(buffer.len() as i32);
        Ok(c)
    }
    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        let file = self.clone();
        let pending = c.clone();
        std::thread::spawn(move || match file.call("sync", 0, 0, &[], json!({})) {
            Ok(_) => pending.complete(0),
            Err(_) => pending.error(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "remote_sync",
            )),
        });
        Ok(c)
    }
    fn size(&self) -> Result<u64> {
        Ok(self.call("size", 0, 0, &[], json!({}))?.0["size"]
            .as_u64()
            .unwrap())
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.call("truncate", len, 0, &[], json!({}))?;
        c.complete(0);
        Ok(c)
    }
}

impl RemoteFile {
    fn call(
        &self,
        op: &str,
        pos: u64,
        len: usize,
        data: &[u8],
        extra: Value,
    ) -> Result<(Value, Vec<u8>)> {
        let result = (|| -> std::io::Result<(Value, Vec<u8>)> {
            let mut guard = self
                .stream
                .lock()
                .map_err(|_| std::io::Error::other("RPC mutex poisoned"))?;
            if guard.is_none() {
                let stream = TcpStream::connect(&self.addr)?;
                stream.set_nodelay(true)?;
                stream.set_read_timeout(Some(Duration::from_secs(15)))?;
                stream.set_write_timeout(Some(Duration::from_secs(15)))?;
                *guard = Some(BufReader::new(stream));
            }
            let reader = guard.as_mut().unwrap();
            let request =
                json!({"op": op, "path": self.path, "pos": pos, "len": len, "extra": extra});
            writeln!(reader.get_mut(), "{request}")?;
            reader.get_mut().write_all(data)?;
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let reply: Value = serde_json::from_str(&line)?;
            if let Some(error) = reply.get("error") {
                return Err(std::io::Error::other(error.to_string()));
            }
            let mut body = vec![0; reply["len"].as_u64().unwrap_or(0) as usize];
            reader.read_exact(&mut body)?;
            Ok((reply, body))
        })();
        if result.is_err() {
            if let Ok(mut guard) = self.stream.lock() {
                *guard = None;
            }
        }
        result.map_err(|e| turso_core::io_error(e, "remote_rpc"))
    }
}
