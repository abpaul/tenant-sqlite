//! Provision, migrate, query and shut down a local tenant registry.

use sea_orm::{ConnectionTrait, DbBackend, Statement};
use tenant_sqlite::{Config, Registry, TenantId};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .ok_or("usage: basic <new tenant directory>")?;
    let registry = Registry::new(Config::new(root), |db| {
        Box::pin(async move {
            db.execute_unprepared("CREATE TABLE IF NOT EXISTS notes (body TEXT NOT NULL)")
                .await?;
            Ok(())
        })
    })?;
    let id = TenantId::new("example")?;
    let tenant = registry.provision(&id).await?;
    tenant
        .execute_unprepared("INSERT INTO notes VALUES ('persistent')")
        .await?;
    drop(tenant);
    let tenant = registry.open_existing(&id).await?;
    let row = tenant
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT body FROM notes",
        ))
        .await?
        .ok_or("persisted note is missing")?;
    let body: String = row.try_get("", "body")?;
    println!("{body}");
    drop(tenant);
    registry.shutdown().await?;
    Ok(())
}
