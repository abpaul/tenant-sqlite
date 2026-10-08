//! Bounded local SQLite tenant pools with explicit creation and scoped access.
//!
//! Keep each [`TenantHandle`] alive until its queries and transactions complete.
//! Use one registry per trusted local root and process; external filesystem mutation
//! is outside the supported model.

use sea_orm::{
    sqlx::sqlite::{SqliteJournalMode, SqliteSynchronous},
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbErr,
};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    ops::Deref,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify};

/// A filename-safe application tenant identity; authentication remains application-owned.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TenantId(String);
impl TenantId {
    /// Reject empty IDs, IDs over 128 bytes, and characters outside lowercase ASCII letters,
    /// digits, `_` and `-`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidId`] for an unsupported identifier.
    ///
    /// # Examples
    ///
    /// ```
    /// use tenant_sqlite::TenantId;
    /// let id: TenantId = "customer-123".parse()?;
    /// assert_eq!(id.as_ref(), "customer-123");
    /// assert_eq!(id.to_string(), "customer-123");
    /// # Ok::<(), tenant_sqlite::Error>(())
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| (b.is_ascii_lowercase() || b.is_ascii_digit()) || b == b'-' || b == b'_')
        {
            return Err(Error::InvalidId);
        }
        Ok(Self(value))
    }
    /// The validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for TenantId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}
impl TryFrom<String> for TenantId {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl TryFrom<&str> for TenantId {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}
impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// SQLite durability policy. NORMAL preserves atomicity but a power loss can lose
/// recently committed WAL transactions. FULL syncs the WAL at each commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Synchronous {
    /// Preserve atomicity with reduced synchronization; power loss can lose recent commits.
    #[default]
    Normal,
    /// Synchronize the WAL at commit for stronger power-loss durability.
    Full,
}

/// Limits apply to all retained and initializing pools, including leased pools.
#[derive(Clone, Debug)]
#[non_exhaustive]
#[must_use]
pub struct Config {
    /// Trusted root, created and canonicalized by [`Registry::new`].
    pub root: PathBuf,
    /// Hard limit on all retained pools, including active tenants; must be nonzero.
    pub max_pools: usize,
    /// Maximum physical connections per pool; must be nonzero.
    pub max_connections: u32,
    /// Time before idle physical connections close; logical pools remain retained.
    pub idle_timeout: Duration,
    /// Timeout for establishing a physical connection.
    pub connect_timeout: Duration,
    /// Timeout for acquiring a physical connection from a tenant pool.
    pub acquire_timeout: Duration,
    /// SQLite timeout while waiting for another connection to release a database lock.
    pub busy_timeout: Duration,
    /// Synchronization policy applied to every physical connection.
    pub synchronous: Synchronous,
    /// Whether SQLx statement logging is enabled.
    pub sqlx_logging: bool,
    /// Optional application SQL executed once before migration on each pool opening.
    pub run_on_start: Option<String>,
}
impl Config {
    /// Defaults: 100 pools, two connections each, zero minimum connections,
    /// 60-second idle timeout, WAL, NORMAL synchronous, five-second busy timeout.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_pools: 100,
            max_connections: 2,
            idle_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(30),
            busy_timeout: Duration::from_secs(5),
            synchronous: Synchronous::Normal,
            sqlx_logging: false,
            run_on_start: None,
        }
    }
}

/// An application migration callback. Use transactional or retry-safe migrations.
pub type MigrationFuture<'a> = Pin<Box<dyn Future<Output = Result<(), DbErr>> + Send + 'a>>;
type Migration = dyn for<'a> Fn(&'a DatabaseConnection) -> MigrationFuture<'a> + Send + Sync;

/// Lifecycle errors never trigger silent creation or migration fallback.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An identifier contains unsupported characters or has an invalid length.
    #[error("invalid tenant identifier")]
    InvalidId,
    /// The configuration has an invalid resource limit.
    #[error("invalid configuration: {0}")]
    Configuration(&'static str),
    /// A root, main file or SQLite sidecar has an unexpected filesystem identity.
    #[error("unsafe tenant path: {0}")]
    UnsafePath(PathBuf),
    /// Existing-file access found no tenant database.
    #[error("tenant database is missing: {0}")]
    Missing(PathBuf),
    /// Exclusive provisioning found an existing pool, file or sidecar.
    #[error("tenant database already exists: {0}")]
    AlreadyExists(PathBuf),
    /// All retained pools are active and no idle eviction victim is available.
    #[error("all retained tenant pools are active")]
    Capacity,
    /// The registry has a process-local deletion tombstone for this tenant.
    #[error("tenant database is closed for deletion")]
    Deleted,
    /// The registry has stopped admitting tenants.
    #[error("tenant registry is shut down")]
    Shutdown,
    /// Application code closed a managed pool outside the registry.
    #[error("tenant pool was closed outside its registry")]
    Closed,
    /// The application migration callback returned an error.
    #[error("tenant migration failed: {0}")]
    Migration(#[source] DbErr),
    /// An owned lifecycle worker failed, including a migration panic.
    #[error("tenant lifecycle task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    /// Connection, startup SQL or pool closure failed.
    #[error(transparent)]
    Database(#[from] DbErr),
    /// Filesystem inspection, directory creation or exclusive file creation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

struct Entry {
    scopes: Arc<Scopes>,
    used: u64,
}
#[derive(Debug)]
struct Scopes {
    db: DatabaseConnection,
    active: AtomicUsize,
    drained: Notify,
}
impl Scopes {
    // Sharing the drain flag and count makes the last-drop wakeup atomic with drainage.
    const DRAINING: usize = 1 << (usize::BITS - 1);

    fn is_idle(&self) -> bool {
        self.active.load(Ordering::Acquire) & !Self::DRAINING == 0
    }

    // The state mutex excludes admission; clones always start from an active lease.
    async fn drain(&self) {
        self.active.fetch_or(Self::DRAINING, Ordering::AcqRel);
        while !self.is_idle() {
            self.drained.notified().await;
        }
    }
}
struct State {
    entries: HashMap<TenantId, Entry>,
    deleted: HashSet<TenantId>,
    tick: u64,
    shutdown: bool,
}
struct Inner {
    config: Config,
    migrate: Arc<Migration>,
    state: Mutex<State>,
}

/// One registry per root per process. Clones share pool ownership and lifecycle.
#[derive(Clone)]
#[must_use]
pub struct Registry {
    inner: Arc<Inner>,
}

/// A scoped pool lease. Clones retain scoped access through eviction and deletion.
///
/// Borrow the dereferenced SeaORM connection only while this handle is alive.
/// Do not clone its raw DatabaseConnection, extract a raw pool, or close it manually.
/// Keep the handle alive through transaction commit, rollback or drop.
///
/// # Examples
///
/// Accidentally discarding an acquired scope is a compile error when unused
/// results are denied:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// use tenant_sqlite::{Error, Registry, TenantId};
/// async fn discard(registry: &Registry, id: &TenantId) -> Result<(), Error> {
///     registry.open_existing(id).await?;
///     Ok(())
/// }
/// ```
///
/// # Panics
///
/// Cloning panics if the active scope count would exceed `isize::MAX`.
/// Normal acquisition is bounded by the same count limit.
#[derive(Debug)]
#[must_use = "retain the handle through its operations and transaction completion"]
pub struct TenantHandle {
    scopes: Arc<Scopes>,
}
impl Clone for TenantHandle {
    fn clone(&self) -> Self {
        Self::new(&self.scopes)
    }
}
impl TenantHandle {
    fn new(scopes: &Arc<Scopes>) -> Self {
        scopes
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
                if active & !Scopes::DRAINING == Scopes::DRAINING - 1 {
                    None
                } else {
                    Some(active + 1)
                }
            })
            .expect("too many tenant handle clones");
        Self {
            scopes: scopes.clone(),
        }
    }
}
impl Drop for TenantHandle {
    fn drop(&mut self) {
        if self.scopes.active.fetch_sub(1, Ordering::Release) == Scopes::DRAINING + 1 {
            self.scopes.drained.notify_one();
        }
    }
}
impl Deref for TenantHandle {
    type Target = DatabaseConnection;
    fn deref(&self) -> &Self::Target {
        &self.scopes.db
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for TenantHandle {
    fn get_database_backend(&self) -> sea_orm::DbBackend {
        self.scopes.db.get_database_backend()
    }
    async fn execute_raw(&self, stmt: sea_orm::Statement) -> Result<sea_orm::ExecResult, DbErr> {
        self.scopes.db.execute_raw(stmt).await
    }
    async fn execute_unprepared(&self, sql: &str) -> Result<sea_orm::ExecResult, DbErr> {
        self.scopes.db.execute_unprepared(sql).await
    }
    async fn query_one_raw(
        &self,
        stmt: sea_orm::Statement,
    ) -> Result<Option<sea_orm::QueryResult>, DbErr> {
        self.scopes.db.query_one_raw(stmt).await
    }
    async fn query_all_raw(
        &self,
        stmt: sea_orm::Statement,
    ) -> Result<Vec<sea_orm::QueryResult>, DbErr> {
        self.scopes.db.query_all_raw(stmt).await
    }
}

#[async_trait::async_trait]
impl sea_orm::TransactionTrait for TenantHandle {
    type Transaction = sea_orm::DatabaseTransaction;
    async fn begin(&self) -> Result<Self::Transaction, DbErr> {
        self.scopes.db.begin().await
    }
    async fn begin_with_config(
        &self,
        isolation: Option<sea_orm::IsolationLevel>,
        access: Option<sea_orm::AccessMode>,
    ) -> Result<Self::Transaction, DbErr> {
        self.scopes.db.begin_with_config(isolation, access).await
    }
    async fn begin_with_options(
        &self,
        options: sea_orm::TransactionOptions,
    ) -> Result<Self::Transaction, DbErr> {
        self.scopes.db.begin_with_options(options).await
    }
    async fn transaction<F, T, E>(&self, callback: F) -> Result<T, sea_orm::TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c Self::Transaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        self.scopes.db.transaction(callback).await
    }
    async fn transaction_with_config<F, T, E>(
        &self,
        callback: F,
        isolation: Option<sea_orm::IsolationLevel>,
        access: Option<sea_orm::AccessMode>,
    ) -> Result<T, sea_orm::TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c Self::Transaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        self.scopes
            .db
            .transaction_with_config(callback, isolation, access)
            .await
    }
}

/// A snapshot of retained ownership for observation and tests.
/// Concurrent operations can change these counts immediately after collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[must_use]
pub struct Stats {
    /// Total retained pools, including active tenant pools.
    pub retained_pools: usize,
    /// Retained pools with at least one live scoped handle.
    pub leased_pools: usize,
    /// Whether shutdown has stopped new tenant admission.
    pub shutdown: bool,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry")
            .field("max_pools", &self.inner.config.max_pools)
            .field("max_connections", &self.inner.config.max_connections)
            .finish_non_exhaustive()
    }
}

impl Registry {
    /// Create and canonicalize the trusted root. Limits must be nonzero.
    ///
    /// Performs synchronous filesystem setup; construct during application startup.
    /// The migration callback must not reenter this registry.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] for zero limits, [`Error::UnsafePath`]
    /// for a symlink root, or [`Error::Io`] for filesystem setup failure.
    pub fn new<F>(mut config: Config, migrate: F) -> Result<Self, Error>
    where
        F: for<'a> Fn(&'a DatabaseConnection) -> MigrationFuture<'a> + Send + Sync + 'static,
    {
        if config.max_pools == 0 || config.max_connections == 0 {
            return Err(Error::Configuration("limits must be nonzero"));
        }
        std::fs::create_dir_all(&config.root)?;
        if std::fs::symlink_metadata(&config.root)?
            .file_type()
            .is_symlink()
        {
            return Err(Error::UnsafePath(config.root));
        }
        config.root = std::fs::canonicalize(config.root)?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                migrate: Arc::new(migrate),
                state: Mutex::new(State {
                    entries: HashMap::new(),
                    deleted: HashSet::new(),
                    tick: 0,
                    shutdown: false,
                }),
            }),
        })
    }

    /// The canonical path. This method does not authorize or access a tenant.
    #[must_use]
    pub fn path(&self, id: &TenantId) -> PathBuf {
        self.inner.config.root.join(format!("{}.db", id.as_str()))
    }

    /// Open a new database exclusively, migrate it, and retain a scoped handle.
    /// Failed provisioning leaves its file for explicit diagnosis and recovery.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyExists`] for an existing pool/file/sidecar,
    /// [`Error::Capacity`] when no idle victim is available, and path, filesystem,
    /// database, migration or worker errors when initialization fails.
    /// Deleted identities and stopped registries return [`Error::Deleted`] and
    /// [`Error::Shutdown`], respectively.
    pub async fn provision(&self, id: &TenantId) -> Result<TenantHandle, Error> {
        self.acquire(id, true).await
    }

    /// Open and migrate an existing database. Never creates a missing file.
    /// Cancellation stops waiting; the owned initializer completes and can be retried.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Missing`] for a missing file, [`Error::Capacity`] when
    /// all retained pools are active, or path, filesystem, database, migration
    /// and worker errors. Deleted/stopped registries reject admission, and a
    /// raw application pool closure returns [`Error::Closed`].
    pub async fn open_existing(&self, id: &TenantId) -> Result<TenantHandle, Error> {
        self.acquire(id, false).await
    }

    async fn acquire(&self, id: &TenantId, create: bool) -> Result<TenantHandle, Error> {
        {
            let mut state = self.inner.state.lock().await;
            if let Some(handle) = self.ready(&mut state, id, create)? {
                return Ok(handle);
            }
        }
        let registry = self.clone();
        let id = id.clone();
        tokio::spawn(async move { registry.acquire_owned(id, create).await }).await?
    }

    fn ready(
        &self,
        state: &mut State,
        id: &TenantId,
        create: bool,
    ) -> Result<Option<TenantHandle>, Error> {
        if state.shutdown {
            return Err(Error::Shutdown);
        }
        if state.deleted.contains(id) {
            return Err(Error::Deleted);
        }
        let Some(entry) = state.entries.get_mut(id) else {
            return Ok(None);
        };
        if create {
            return Err(Error::AlreadyExists(self.path(id)));
        }
        if entry.scopes.db.get_sqlite_connection_pool().is_closed() {
            return Err(Error::Closed);
        }
        state.tick = state.tick.wrapping_add(1);
        entry.used = state.tick;
        Ok(Some(Self::lease(entry)))
    }

    async fn acquire_owned(&self, id: TenantId, create: bool) -> Result<TenantHandle, Error> {
        let mut state = self.inner.state.lock().await;
        if let Some(handle) = self.ready(&mut state, &id, create)? {
            return Ok(handle);
        }
        state.tick = state.tick.wrapping_add(1);
        let tick = state.tick;
        self.validate_path(&id, create).await?;
        if state.entries.len() == self.inner.config.max_pools {
            let candidate = state
                .entries
                .iter()
                .filter(|(_, entry)| entry.scopes.is_idle())
                .min_by_key(|(_, entry)| entry.used)
                .map(|(id, _)| id.clone())
                .ok_or(Error::Capacity)?;
            let entry = &state.entries[&candidate];
            entry.scopes.db.close_by_ref().await?;
            state.entries.remove(&candidate);
        }
        let path = self.path(&id);
        if create {
            match tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
            {
                Ok(file) => drop(file),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(Error::AlreadyExists(path))
                }
                Err(e) => return Err(e.into()),
            }
        }
        let config = &self.inner.config;
        let mut options = ConnectOptions::new("sqlite::memory:");
        options
            .min_connections(0)
            .max_connections(config.max_connections)
            .idle_timeout(config.idle_timeout)
            .connect_timeout(config.connect_timeout)
            .acquire_timeout(config.acquire_timeout)
            .sqlx_logging(config.sqlx_logging);
        let busy = config.busy_timeout;
        let sync = match config.synchronous {
            Synchronous::Normal => SqliteSynchronous::Normal,
            Synchronous::Full => SqliteSynchronous::Full,
        };
        options.map_sqlx_sqlite_opts(move |opts| {
            opts.filename(&path)
                .in_memory(false)
                .create_if_missing(false)
                .foreign_keys(true)
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(sync)
                .busy_timeout(busy)
                .pragma("mmap_size", "134217728")
                .pragma("journal_size_limit", "67108864")
                .pragma("cache_size", "2000")
        });
        let db = Database::connect(options).await?;
        let worker_db = db.clone();
        let migration = self.inner.migrate.clone();
        let startup = config.run_on_start.clone();
        let result = tokio::spawn(async move {
            if let Some(sql) = startup {
                worker_db
                    .execute_unprepared(&sql)
                    .await
                    .map_err(Error::Database)?;
            }
            migration(&worker_db).await.map_err(Error::Migration)
        })
        .await
        .map_err(Error::Task)
        .and_then(|result| result);
        if let Err(error) = result {
            db.close().await?;
            return Err(error);
        }
        let entry = Entry {
            scopes: Arc::new(Scopes {
                db,
                active: AtomicUsize::new(0),
                drained: Notify::new(),
            }),
            used: tick,
        };
        let handle = Self::lease(&entry);
        state.entries.insert(id, entry);
        Ok(handle)
    }

    fn lease(entry: &Entry) -> TenantHandle {
        TenantHandle::new(&entry.scopes)
    }

    async fn validate_path(&self, id: &TenantId, create: bool) -> Result<(), Error> {
        let path = self.path(id);
        if tokio::fs::canonicalize(&self.inner.config.root).await? != self.inner.config.root {
            return Err(Error::UnsafePath(path));
        }
        for suffix in ["", "-wal", "-shm"] {
            let file = path.with_file_name(format!("{}.db{suffix}", id.as_str()));
            match tokio::fs::symlink_metadata(&file).await {
                Ok(metadata) => {
                    if !metadata.is_file() || metadata.file_type().is_symlink() {
                        return Err(Error::UnsafePath(file));
                    }
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        if metadata.nlink() != 1 {
                            return Err(Error::UnsafePath(file));
                        }
                    }
                    if create {
                        return Err(Error::AlreadyExists(file));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    if suffix.is_empty() && !create {
                        return Err(Error::Missing(file));
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Evict a tenant only when no scoped handle is active. Await pool closure
    /// before returning. An active pool returns Capacity and remains usable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Capacity`] for active scoped handles, [`Error::Database`]
    /// for pool closure failure, or [`Error::Task`] for worker failure.
    pub async fn evict(&self, id: &TenantId) -> Result<(), Error> {
        let registry = self.clone();
        let id = id.clone();
        tokio::spawn(async move {
            let mut state = registry.inner.state.lock().await;
            if let Some(entry) = state.entries.get(&id) {
                if !entry.scopes.is_idle() {
                    return Err(Error::Capacity);
                }
                entry.scopes.db.close_by_ref().await?;
            }
            state.entries.remove(&id);
            Ok(())
        })
        .await?
    }

    /// Drain leases and transactions, close the pool, and prevent future opens.
    /// The application can unlink database/WAL/SHM only after this returns success.
    /// Canceling the caller leaves the close running; retry is idempotent.
    /// Release your own handles before awaiting deletion to avoid a wait cycle.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Shutdown`] if shutdown has begun, [`Error::Database`]
    /// if closure fails, or [`Error::Task`] if the owned worker fails.
    pub async fn close_for_deletion(&self, id: &TenantId) -> Result<PathBuf, Error> {
        let registry = self.clone();
        let id = id.clone();
        tokio::spawn(async move {
            let mut state = registry.inner.state.lock().await;
            if state.shutdown {
                return Err(Error::Shutdown);
            }
            state.deleted.insert(id.clone());
            if let Some(entry) = state.entries.get(&id) {
                entry.scopes.drain().await;
                entry.scopes.db.close_by_ref().await?;
            }
            state.entries.remove(&id);
            Ok(registry.path(&id))
        })
        .await?
    }

    /// Stop admission, drain scoped handles and transactions, and close every pool.
    /// Stop application jobs and drop application handles before awaiting this call.
    /// Cancellation leaves the shutdown running; repeated shutdown is safe.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Database`] if pool closure fails or [`Error::Task`]
    /// if the owned worker fails.
    pub async fn shutdown(&self) -> Result<(), Error> {
        let registry = self.clone();
        tokio::spawn(async move {
            let mut state = registry.inner.state.lock().await;
            state.shutdown = true;
            for entry in state.entries.values() {
                entry.scopes.drain().await;
                entry.scopes.db.close_by_ref().await?;
            }
            state.entries.clear();
            Ok(())
        })
        .await?
    }

    /// Observe retained pools; lifecycle work may delay this snapshot.
    pub async fn stats(&self) -> Stats {
        let state = self.inner.state.lock().await;
        Stats {
            retained_pools: state.entries.len(),
            leased_pools: state
                .entries
                .values()
                .filter(|entry| !entry.scopes.is_idle())
                .count(),
            shutdown: state.shutdown,
        }
    }
}

/// Check an application control-plane path against the canonical tenant path.
/// The application should call this before opening a tenant recorded in its control DB.
///
/// # Errors
///
/// Returns [`Error::UnsafePath`] if no parent/filename can be identified, or
/// [`Error::Io`] if the recorded path's parent cannot be canonicalized.
pub async fn matches_path(registry: &Registry, id: &TenantId, path: &Path) -> Result<bool, Error> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::UnsafePath(path.to_owned()))?;
    Ok(tokio::fs::canonicalize(parent).await?.join(
        path.file_name()
            .ok_or_else(|| Error::UnsafePath(path.to_owned()))?,
    ) == registry.path(id))
}
