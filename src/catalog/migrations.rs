use anyhow::Result;
use futures::future::BoxFuture;

pub trait CatalogMigrations {
    type Pool;

    fn ensure_migrations_table(pool: &Self::Pool) -> BoxFuture<'_, Result<()>>;
    fn current_version(pool: &Self::Pool) -> BoxFuture<'_, Result<i64>>;
    fn record_version(pool: &Self::Pool, version: i64) -> BoxFuture<'_, Result<()>>;

    fn migrate_v1(pool: &Self::Pool) -> BoxFuture<'_, Result<()>>;
    fn migrate_v2(pool: &Self::Pool) -> BoxFuture<'_, Result<()>>;
}

pub async fn run_migrations<M: CatalogMigrations>(pool: &M::Pool) -> Result<()> {
    M::ensure_migrations_table(pool).await?;

    let mut current_version = M::current_version(pool).await?;
    let steps: &[(i64, for<'a> fn(&'a M::Pool) -> BoxFuture<'a, Result<()>>)] =
        &[(1, M::migrate_v1), (2, M::migrate_v2)];

    for (version, apply) in steps {
        if current_version < *version {
            apply(pool).await?;
            M::record_version(pool, *version).await?;
            current_version = *version;
        }
    }

    Ok(())
}
