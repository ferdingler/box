//! Bind backend callbacks and HTTP propagation to one immutable Call context.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use strands_shell::os::{
    DirEntry, Fd, FileStat, Follow, HostSpawn, HostSpawnOutcome, HttpRequest, HttpResponse, Kernel,
    OpenFlags, Process, Resolved, ScriptOutcome, WriteFailure,
};

pub(super) struct CallKernel {
    backend: Arc<dyn Kernel>,
    correlation: telemetry::Correlation,
}

impl CallKernel {
    fn traced_path(path: &Resolved) -> String {
        if crate::run::execution_trace::enabled() {
            path.path().to_string()
        } else {
            String::new()
        }
    }
    /// Share the backend while retaining this Call's correlation.
    pub(super) fn new(backend: Arc<dyn Kernel>, correlation: telemetry::Correlation) -> Self {
        Self {
            backend,
            correlation,
        }
    }

    async fn filesystem<T>(
        &self,
        operation: &str,
        path: &str,
        future: impl std::future::Future<Output = io::Result<T>>,
    ) -> io::Result<T> {
        if !crate::run::execution_trace::enabled() {
            return future.await;
        }
        self.correlation
            .scope(async {
                let activity =
                    crate::run::execution_trace::Activity::filesystem("shell", operation, path);
                let result = future.await;
                activity.finish(result.is_ok());
                result
            })
            .await
    }
}

#[async_trait::async_trait]
impl Kernel for CallKernel {
    async fn resolve(&self, proc: &Process, path: &str, follow: Follow) -> Resolved {
        self.backend.resolve(proc, path, follow).await
    }

    fn new_process(&self) -> Process {
        self.backend.new_process()
    }

    async fn open(&self, proc: &mut Process, path: Resolved, flags: OpenFlags) -> io::Result<Fd> {
        let named = Self::traced_path(&path);
        let operation = if flags.write || flags.create || flags.truncate {
            "Open for write"
        } else {
            "Open for read"
        };
        self.filesystem(operation, &named, self.backend.open(proc, path, flags))
            .await
    }

    async fn list_dir(&self, proc: &Process, path: Resolved) -> io::Result<Vec<DirEntry>> {
        let named = Self::traced_path(&path);
        self.filesystem("List directory", &named, self.backend.list_dir(proc, path))
            .await
    }

    async fn change_dir(&self, proc: &mut Process, path: Resolved) -> io::Result<()> {
        let named = Self::traced_path(&path);
        self.filesystem(
            "Change directory",
            &named,
            self.backend.change_dir(proc, path),
        )
        .await
    }

    async fn stat(&self, proc: &Process, path: Resolved) -> FileStat {
        self.backend.stat(proc, path).await
    }

    async fn lstat(&self, proc: &Process, path: Resolved) -> FileStat {
        self.backend.lstat(proc, path).await
    }

    async fn access(&self, proc: &Process, path: Resolved, mode: i32) -> bool {
        self.backend.access(proc, path, mode).await
    }

    async fn canonicalize(&self, proc: &Process, path: Resolved) -> io::Result<PathBuf> {
        self.backend.canonicalize(proc, path).await
    }

    async fn is_executable(&self, proc: &Process, path: Resolved) -> bool {
        self.backend.is_executable(proc, path).await
    }

    async fn glob(&self, proc: &Process, pattern: &str) -> Vec<String> {
        self.backend.glob(proc, pattern).await
    }

    fn isatty(&self, fd: i32) -> bool {
        self.backend.isatty(fd)
    }

    async fn remove_file(&self, proc: &Process, path: Resolved) -> io::Result<()> {
        let named = Self::traced_path(&path);
        self.filesystem("Remove file", &named, self.backend.remove_file(proc, path))
            .await
    }

    async fn remove_dir(&self, proc: &Process, path: Resolved) -> io::Result<()> {
        let named = Self::traced_path(&path);
        self.filesystem(
            "Remove directory",
            &named,
            self.backend.remove_dir(proc, path),
        )
        .await
    }

    async fn create_dir(&self, proc: &Process, path: Resolved) -> io::Result<()> {
        let named = Self::traced_path(&path);
        self.filesystem(
            "Create directory",
            &named,
            self.backend.create_dir(proc, path),
        )
        .await
    }

    async fn rename(&self, proc: &Process, from: Resolved, to: Resolved) -> io::Result<()> {
        let named = if crate::run::execution_trace::enabled() {
            format!("{} → {}", from.path(), to.path())
        } else {
            String::new()
        };
        self.filesystem("Rename", &named, self.backend.rename(proc, from, to))
            .await
    }

    async fn symlink(&self, proc: &Process, target: &str, link: Resolved) -> io::Result<()> {
        let named = if crate::run::execution_trace::enabled() {
            format!("{} → {target}", link.path())
        } else {
            String::new()
        };
        self.filesystem(
            "Create symlink",
            &named,
            self.backend.symlink(proc, target, link),
        )
        .await
    }

    async fn read_link(&self, proc: &Process, path: Resolved) -> io::Result<String> {
        let named = Self::traced_path(&path);
        self.filesystem("Read symlink", &named, self.backend.read_link(proc, path))
            .await
    }

    async fn set_permissions(&self, proc: &Process, path: Resolved, mode: u32) -> io::Result<()> {
        let named = Self::traced_path(&path);
        self.filesystem(
            "Set permissions",
            &named,
            self.backend.set_permissions(proc, path, mode),
        )
        .await
    }

    fn now(&self) -> SystemTime {
        self.backend.now()
    }

    async fn settle_writes(&self) -> Vec<WriteFailure> {
        self.backend.settle_writes().await
    }

    fn check_url(&self, url: &str) -> io::Result<()> {
        self.backend.check_url(url)
    }

    async fn http_request(&self, mut req: HttpRequest) -> io::Result<HttpResponse> {
        self.correlation.inject_http(&mut req.headers);
        self.correlation
            .scope(async {
                let _execution_http = crate::run::execution_trace::Activity::start(
                    "gateway",
                    "shell",
                    "Shell HTTP attempt",
                );
                self.backend.http_request(req).await
            })
            .await
    }

    async fn run_script(&self, source: String) -> io::Result<ScriptOutcome> {
        self.correlation
            .scope(self.backend.run_script(source))
            .await
    }

    async fn spawn_host(&self, spawn: HostSpawn) -> io::Result<HostSpawnOutcome> {
        self.correlation.scope(self.backend.spawn_host(spawn)).await
    }
}
