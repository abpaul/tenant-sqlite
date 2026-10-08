//! Verify tenancy and lifecycle behavior against real temporary SQLite files.

use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tenant_sqlite::{Config, Error, Registry, TenantId};
use tokio::sync::Semaphore;

fn id(name: &str) -> TenantId {
    TenantId::new(name).unwrap()
}
fn fixture(limit: usize) -> (tempfile::TempDir, Registry) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path());
    config.max_pools = limit;
    let registry = Registry::new(config, |db| {
        Box::pin(async move {
            db.execute_unprepared("CREATE TABLE IF NOT EXISTS items (value TEXT NOT NULL)")
                .await?;
            Ok(())
        })
    })
    .unwrap();
    (dir, registry)
}
async fn values(db: &sea_orm::DatabaseConnection) -> Vec<String> {
    db.query_all_raw(Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT value FROM items ORDER BY value",
    ))
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.try_get("", "value").unwrap())
    .collect()
}

#[test]
fn validates_ids_and_limits() {
    for name in [
        "",
        "..",
        "a/b",
        "../tenant",
        "a\\b",
        "a?mode=rwc",
        "tenant.db",
        "é",
        "Uppercase",
        "a\0",
    ] {
        assert!(matches!(TenantId::new(name), Err(Error::InvalidId)));
    }
    assert!(TenantId::new("a".repeat(129)).is_err());
    assert_eq!(id("customer_123-ab").as_str(), "customer_123-ab");
    let dir = tempfile::tempdir().unwrap();
    let mut c = Config::new(dir.path());
    c.max_pools = 0;
    assert!(matches!(
        Registry::new(c, |_| Box::pin(async { Ok(()) })),
        Err(Error::Configuration(_))
    ));
}

#[tokio::test]
async fn ordinary_open_never_creates_and_provision_is_exclusive() {
    let (_dir, r) = fixture(2);
    assert!(matches!(
        r.open_existing(&id("missing")).await,
        Err(Error::Missing(_))
    ));
    assert!(!r.path(&id("missing")).exists());
    let db = r.provision(&id("one")).await.unwrap();
    assert!(matches!(
        r.provision(&id("one")).await,
        Err(Error::AlreadyExists(_))
    ));
    assert_eq!(values(&db).await, Vec::<String>::new());
    drop(db);
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn isolation_transactions_and_restart_persistence() {
    let (dir, r) = fixture(2);
    let a = r.provision(&id("a")).await.unwrap();
    let b = r.provision(&id("b")).await.unwrap();
    let tx = a.begin().await.unwrap();
    tx.execute_unprepared("INSERT INTO items VALUES ('committed')")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let tx = a.begin().await.unwrap();
    tx.execute_unprepared("INSERT INTO items VALUES ('rolled-back')")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(values(&a).await, ["committed"]);
    assert_eq!(values(&b).await, Vec::<String>::new());
    drop(a);
    drop(b);
    r.shutdown().await.unwrap();
    let r = Registry::new(Config::new(dir.path()), |_| Box::pin(async { Ok(()) })).unwrap();
    let a = r.open_existing(&id("a")).await.unwrap();
    assert_eq!(values(&a).await, ["committed"]);
    drop(a);
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn active_pools_count_toward_limits_and_idle_eviction_closes() {
    let (_dir, r) = fixture(1);
    let a = r.provision(&id("a")).await.unwrap();
    let clone = a.clone();
    let pool = a.get_sqlite_connection_pool().clone();
    assert!(matches!(r.provision(&id("b")).await, Err(Error::Capacity)));
    assert!(!r.path(&id("b")).exists());
    assert_eq!(r.stats().await.retained_pools, 1);
    drop(a);
    assert!(matches!(r.provision(&id("b")).await, Err(Error::Capacity)));
    assert_eq!(values(&clone).await, Vec::<String>::new());
    drop(clone);
    let b = r.provision(&id("b")).await.unwrap();
    assert!(pool.is_closed());
    assert_eq!(r.stats().await.retained_pools, 1);
    drop(b);
    let a = r.open_existing(&id("a")).await.unwrap();
    assert_eq!(values(&a).await, Vec::<String>::new());
    drop(a);
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_initialization_and_waiter_cancellation_share_one_pool() {
    let dir = tempfile::tempdir().unwrap();
    let reached = Arc::new(Semaphore::new(0));
    let resume = Arc::new(Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let r = Registry::new(Config::new(dir.path()), {
        let reached = reached.clone();
        let resume = resume.clone();
        let calls = calls.clone();
        move |db| {
            let reached = reached.clone();
            let resume = resume.clone();
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                reached.add_permits(1);
                resume.acquire().await.unwrap().forget();
                db.execute_unprepared("CREATE TABLE items (value TEXT)")
                    .await?;
                Ok(())
            })
        }
    })
    .unwrap();
    let first = {
        let r = r.clone();
        tokio::spawn(async move { r.provision(&id("a")).await })
    };
    reached.acquire().await.unwrap().forget();
    let second = {
        let r = r.clone();
        tokio::spawn(async move { r.open_existing(&id("a")).await })
    };
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    resume.add_permits(1);
    let a = tokio::time::timeout(std::time::Duration::from_secs(5), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(r.stats().await.retained_pools, 1);
    assert_eq!(values(&a).await, Vec::<String>::new());
    drop(a);
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn migration_failure_and_panic_close_and_allow_retry() {
    for panic in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let r = Registry::new(Config::new(dir.path()), {
            let calls = calls.clone();
            move |db| {
                let calls = calls.clone();
                Box::pin(async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        assert!(!panic, "injected migration panic");
                        return Err(DbErr::Custom("injected migration failure".into()));
                    }
                    db.execute_unprepared("CREATE TABLE items (value TEXT)")
                        .await?;
                    Ok(())
                })
            }
        })
        .unwrap();
        let error = r.provision(&id("a")).await.err().unwrap();
        assert!(if panic {
            matches!(error, Error::Task(_))
        } else {
            matches!(error, Error::Migration(_))
        });
        assert_eq!(r.stats().await.retained_pools, 0);
        assert!(r.path(&id("a")).exists());
        let a = r.open_existing(&id("a")).await.unwrap();
        assert_eq!(values(&a).await, Vec::<String>::new());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(a);
        r.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn startup_failure_is_explicit_and_leaves_no_retained_pool() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path());
    config.run_on_start = Some("SELECT missing FROM no_table".into());
    let r = Registry::new(config, |_| Box::pin(async { Ok(()) })).unwrap();
    assert!(matches!(
        r.provision(&id("a")).await,
        Err(Error::Database(_))
    ));
    assert_eq!(r.stats().await.retained_pools, 0);
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn deletion_waits_for_scopes_and_transactions_before_unlink() {
    let (_dir, r) = fixture(1);
    let a = r.provision(&id("a")).await.unwrap();
    let clone = a.clone();
    let pool = a.get_sqlite_connection_pool().clone();
    let tx = a.begin().await.unwrap();
    let deleting = {
        let r = r.clone();
        tokio::spawn(async move { r.close_for_deletion(&id("a")).await })
    };
    tokio::task::yield_now().await;
    assert!(!deleting.is_finished());
    assert!(!pool.is_closed());
    assert_eq!(values(&a).await, Vec::<String>::new());
    drop(a);
    let last = clone.clone();
    drop(clone);
    assert_eq!(values(&last).await, Vec::<String>::new());
    assert!(!pool.is_closed());
    drop(last);
    tokio::time::timeout(std::time::Duration::from_secs(5), pool.close_event())
        .await
        .unwrap();
    assert!(!deleting.is_finished());
    assert!(r.path(&id("a")).exists());
    tx.execute_unprepared("INSERT INTO items VALUES ('last')")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let path = deleting.await.unwrap().unwrap();
    assert!(pool.is_closed());
    tokio::fs::remove_file(path).await.unwrap();
    assert!(matches!(
        r.open_existing(&id("a")).await,
        Err(Error::Deleted)
    ));
    r.close_for_deletion(&id("a")).await.unwrap();
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_deletion_completes_and_shutdown_drains() {
    let (_dir, r) = fixture(2);
    let a = r.provision(&id("a")).await.unwrap();
    let tx = a.begin().await.unwrap();
    let pool = a.get_sqlite_connection_pool().clone();
    let closing = {
        let r = r.clone();
        tokio::spawn(async move { r.close_for_deletion(&id("a")).await })
    };
    drop(a);
    tokio::time::timeout(std::time::Duration::from_secs(5), pool.close_event())
        .await
        .unwrap();
    closing.abort();
    assert!(closing.await.unwrap_err().is_cancelled());
    tx.rollback().await.unwrap();
    r.close_for_deletion(&id("a")).await.unwrap();
    let b = r.provision(&id("b")).await.unwrap();
    let pool = b.get_sqlite_connection_pool().clone();
    let shutdown = {
        let r = r.clone();
        tokio::spawn(async move { r.shutdown().await })
    };
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    assert!(!pool.is_closed());
    drop(b);
    shutdown.await.unwrap().unwrap();
    assert!(pool.is_closed());
    assert_eq!(r.stats().await.retained_pools, 0);
    assert!(matches!(
        r.open_existing(&id("b")).await,
        Err(Error::Shutdown)
    ));
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn sqlite_options_apply_to_every_connection_and_snapshot_includes_wal() {
    let (dir, r) = fixture(1);
    let a = r.provision(&id("a")).await.unwrap();
    let tx1 = a.begin().await.unwrap();
    let tx2 = a.begin().await.unwrap();
    for db in [&tx1, &tx2] {
        for (pragma, expected) in [
            ("foreign_keys", "1"),
            ("synchronous", "1"),
            ("busy_timeout", "5000"),
        ] {
            let row = db
                .query_one_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!("PRAGMA {pragma}"),
                ))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                row.try_get_by_index::<i64>(0).unwrap().to_string(),
                expected
            );
        }
        let row = db
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA journal_mode",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get_by_index::<String>(0).unwrap(), "wal");
    }
    tx1.rollback().await.unwrap();
    tx2.rollback().await.unwrap();
    a.execute_unprepared("INSERT INTO items VALUES ('wal-value')")
        .await
        .unwrap();
    let snapshot = dir.path().join("snapshot.db");
    a.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "VACUUM INTO ?",
        [snapshot.to_string_lossy().into_owned().into()],
    ))
    .await
    .unwrap();
    let db = sea_orm::Database::connect(format!("sqlite://{}?mode=rw", snapshot.display()))
        .await
        .unwrap();
    assert_eq!(values(&db).await, ["wal-value"]);
    db.close().await.unwrap();
    drop(a);
    r.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_symlinks_hardlinks_and_nonfiles() {
    let (dir, r) = fixture(1);
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::os::unix::fs::symlink(outside.path(), r.path(&id("link"))).unwrap();
    assert!(matches!(
        r.open_existing(&id("link")).await,
        Err(Error::UnsafePath(_))
    ));
    std::fs::hard_link(outside.path(), r.path(&id("hard"))).unwrap();
    assert!(matches!(
        r.open_existing(&id("hard")).await,
        Err(Error::UnsafePath(_))
    ));
    std::fs::create_dir(r.path(&id("directory"))).unwrap();
    assert!(matches!(
        r.open_existing(&id("directory")).await,
        Err(Error::UnsafePath(_))
    ));
    std::os::unix::fs::symlink(outside.path(), dir.path().join("a.db-wal")).unwrap();
    assert!(matches!(
        r.provision(&id("a")).await,
        Err(Error::UnsafePath(_))
    ));
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_eviction_respects_leases_and_full_durability_is_configurable() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path());
    config.max_connections = 3;
    config.synchronous = tenant_sqlite::Synchronous::Full;
    config.busy_timeout = std::time::Duration::from_millis(1234);
    let r = Registry::new(config, |_| Box::pin(async { Ok(()) })).unwrap();
    let a = r.provision(&id("a")).await.unwrap();
    assert_eq!(
        a.get_sqlite_connection_pool()
            .options()
            .get_max_connections(),
        3
    );
    assert_eq!(
        a.get_sqlite_connection_pool()
            .options()
            .get_min_connections(),
        0
    );
    for (pragma, expected) in [("synchronous", 2), ("busy_timeout", 1234)] {
        let row = a
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("PRAGMA {pragma}"),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get_by_index::<i64>(0).unwrap(), expected);
    }
    assert!(matches!(r.evict(&id("a")).await, Err(Error::Capacity)));
    let pool = a.get_sqlite_connection_pool().clone();
    drop(a);
    r.evict(&id("a")).await.unwrap();
    assert!(pool.is_closed());
    assert_eq!(r.stats().await.retained_pools, 0);
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_externally_closed_pool_is_an_explicit_error() {
    let (_dir, r) = fixture(1);
    let a = r.provision(&id("a")).await.unwrap();
    a.close_by_ref().await.unwrap();
    drop(a);
    assert!(matches!(
        r.open_existing(&id("a")).await,
        Err(Error::Closed)
    ));
    r.shutdown().await.unwrap();
}

#[tokio::test]
async fn cross_thread_handle_clones_keep_shutdown_scoped() {
    let (_dir, r) = fixture(1);
    let a = r.provision(&id("a")).await.unwrap();
    a.execute_unprepared("INSERT INTO items VALUES ('held')")
        .await
        .unwrap();
    let pool = a.get_sqlite_connection_pool().clone();
    let mut workers = Vec::new();
    let mut starts = Vec::new();
    for _ in 0..8 {
        let handle = a.clone();
        let (send, recv) = std::sync::mpsc::channel();
        starts.push(send);
        workers.push(std::thread::spawn(move || {
            recv.recv().unwrap();
            for _ in 0..2_000 {
                let first = handle.clone();
                let second = first.clone();
                drop(first);
                drop(second);
            }
            drop(handle);
        }));
    }
    assert_eq!(r.stats().await.leased_pools, 1);
    assert!(matches!(r.evict(&id("a")).await, Err(Error::Capacity)));
    let shutdown = {
        let r = r.clone();
        tokio::spawn(async move { r.shutdown().await })
    };
    assert_eq!(values(&a).await, vec!["held"]);
    assert!(!pool.is_closed());
    drop(a);
    for start in starts {
        start.send(()).unwrap();
    }
    for worker in workers {
        worker.join().unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(pool.is_closed());
    assert_eq!(r.stats().await.retained_pools, 0);
}

#[test]
fn tenant_id_standard_conversions_preserve_validation_and_identity() {
    let parsed: TenantId = "customer_123-ab".parse().unwrap();
    let borrowed = TenantId::try_from("customer_123-ab").unwrap();
    let owned = TenantId::try_from(String::from("customer_123-ab")).unwrap();
    assert_eq!(parsed.as_ref(), "customer_123-ab");
    assert_eq!(parsed.to_string(), "customer_123-ab");
    assert_eq!(borrowed, parsed);
    assert_eq!(owned, parsed);
    for invalid in ["", "../tenant", "Uppercase", "a\0"] {
        assert!(matches!(invalid.parse::<TenantId>(), Err(Error::InvalidId)));
        assert!(matches!(TenantId::try_from(invalid), Err(Error::InvalidId)));
        assert!(matches!(
            TenantId::try_from(invalid.to_owned()),
            Err(Error::InvalidId)
        ));
    }
    assert_eq!(
        tenant_sqlite::Synchronous::default(),
        tenant_sqlite::Synchronous::Normal
    );
}
