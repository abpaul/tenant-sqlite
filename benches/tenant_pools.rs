//! Measure scoped access and bounded churn using temporary SQLite files.

use sea_orm::{ConnectionTrait, DbBackend, Statement};
use std::{hint::black_box, time::Instant};
use tenant_sqlite::{Config, Registry, TenantId};

const ROUNDS: usize = 9;
const HANDLE_OPS: usize = 100_000;
const SELECT_OPS: usize = 5_000;
const CHURN_OPS: usize = 128;

async fn workload(registry: &Registry, ids: &[TenantId], kind: &str, operations: usize) {
    for i in 0..operations {
        let id = if kind == "bounded_churn" {
            &ids[i % ids.len()]
        } else {
            &ids[0]
        };
        let handle = registry
            .open_existing(black_box(id))
            .await
            .expect("existing tenant");
        if kind == "warm_select" {
            let row = handle
                .query_one_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    "SELECT 42 AS answer",
                ))
                .await
                .expect("SELECT")
                .expect("row");
            assert_eq!(
                row.try_get::<i64>("", "answer").expect("literal result"),
                42
            );
            black_box(row);
        }
        black_box(&handle);
        drop(handle);
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("runtime=current_thread profile=release opt_level=3 rounds={ROUNDS}");
    println!("workload,round,operations,elapsed_ns,ns_per_operation");
    for (kind, operations, tenants, pools) in [
        ("warm_handle", HANDLE_OPS, 1, 8),
        ("warm_select", SELECT_OPS, 1, 8),
        ("bounded_churn", CHURN_OPS, 16, 8),
    ] {
        let root = tempfile::tempdir().expect("temporary root");
        let mut config = Config::new(root.path());
        config.max_pools = pools;
        config.max_connections = 1;
        config.sqlx_logging = false;
        let registry = Registry::new(config, |db| {
            Box::pin(async move {
                db.execute_unprepared(
                    "CREATE TABLE IF NOT EXISTS benchmark_state (value INTEGER NOT NULL)",
                )
                .await?;
                Ok(())
            })
        })
        .expect("registry");
        let ids: Vec<_> = (0..tenants)
            .map(|i| TenantId::new(format!("tenant-{i}")).expect("id"))
            .collect();
        for id in &ids {
            drop(registry.provision(id).await.expect("provision"));
        }
        workload(&registry, &ids, kind, operations).await;
        let mut samples = Vec::with_capacity(ROUNDS);
        for round in 1..=ROUNDS {
            let started = Instant::now();
            workload(&registry, &ids, kind, operations).await;
            let elapsed = started.elapsed().as_nanos();
            let ns = elapsed as f64 / operations as f64;
            samples.push(ns);
            println!("{kind},{round},{operations},{elapsed},{ns:.3}");
            let stats = registry.stats().await;
            assert!(stats.retained_pools <= pools);
            assert_eq!(stats.leased_pools, 0);
        }
        samples.sort_by(f64::total_cmp);
        println!("median,{kind},{:.3}", samples[ROUNDS / 2]);
        registry.shutdown().await.expect("shutdown");
        assert_eq!(registry.stats().await.retained_pools, 0);
    }
}
