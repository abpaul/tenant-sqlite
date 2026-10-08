# tenant-sqlite

Scoped, bounded SQLite database-per-tenant management for Rust applications using
SeaORM and SQLx. One process owns a registry over trusted local database files.

- Validated tenant IDs and explicit provisioning; ordinary access never creates a missing database.
- Coordinated initialization, application migrations and limits covering every retained pool.
- Scoped handles keep active operations safe during eviction, deletion and shutdown.

## Install

Rust 1.94 or newer. Registry publication is disabled; use a pinned Git dependency:

```toml
[dependencies]
tenant-sqlite = { git = "https://github.com/abpaul/tenant-sqlite.git", rev = "1077718d6574c7f5aa19d8c3b928460f3730882f" }
sea-orm = { version = "2.0.4", default-features = false, features = ["sqlx-sqlite", "runtime-tokio-rustls"] }
tokio = { version = "1.45", features = ["rt", "macros"] }
```

## Use

```rust
use sea_orm::ConnectionTrait;
use tenant_sqlite::{Config, Registry, TenantId};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new("./tenant-data");
    config.max_pools = 32;
    config.max_connections = 2;
    let registry = Registry::new(config, |db| {
        Box::pin(async move {
            db.execute_unprepared("CREATE TABLE IF NOT EXISTS notes (body TEXT NOT NULL)")
                .await?;
            Ok(())
        })
    })?;
    let id = TenantId::new("customer-1")?;
    let tenant = registry.provision(&id).await?;
    tenant.execute_unprepared("INSERT INTO notes VALUES ('hello')").await?;
    drop(tenant);

    let tenant = registry.open_existing(&id).await?;
    drop(tenant);
    registry.shutdown().await?;
    Ok(())
}
```

Provision once through a creation workflow. Later requests use `open_existing`.
Authenticate and resolve tenants in the application before accessing the registry.
Provide transactional, idempotent migrations; failures remain explicit and retryable.

Keep handles alive through queries and transaction completion. Clone a handle for
background work; never retain raw connection/pool clones beyond its scope.
Before deleting files, release handles and await `close_for_deletion`. Stop jobs,
release handles and await `shutdown` before runtime teardown.

SQLite uses WAL, foreign keys, a five-second busy timeout and synchronous NORMAL
by default. NORMAL can lose recent commits on power loss; configure
`Synchronous::Full` for stronger commit durability. Use supported SQLite snapshot
operations such as `VACUUM INTO`, rather than copying a live database file.

Run [the example](examples/basic.rs) with a new directory:
`cargo run --locked --example basic -- ./example-data`.

## License

[Apache-2.0](LICENSE). See [NOTICE](NOTICE).
