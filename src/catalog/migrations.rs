//! Schema migration support for catalog backends.
//!
//! This module provides a trait-based migration system that allows different
//! catalog backends (e.g., SQLite, PostgreSQL) to implement their own migration
//! logic while sharing a common execution framework.

use anyhow::Result;

/// Trait for implementing catalog schema migrations.
///
/// Each catalog backend implements this trait to provide database-specific
/// migration logic. The trait defines methods for tracking migration state
/// and applying schema changes.
///
/// # Associated Types
///
/// * `Pool` - The database connection pool type (e.g., `SqlitePool`)
///
/// # Implementation Notes
///
/// Implementations must ensure that migrations are idempotent and safe to
/// re-run. The migration table tracks which versions have been applied.
pub trait CatalogMigrations {
    /// The database connection pool type for this backend.
    type Pool;

    /// Creates the migrations tracking table if it doesn't exist.
    async fn ensure_migrations_table(pool: &Self::Pool) -> Result<()>;

    /// Returns the current schema version, or 0 if no migrations have been applied.
    async fn current_version(pool: &Self::Pool) -> Result<i64>;

    /// Records that a migration version has been successfully applied.
    async fn record_version(pool: &Self::Pool, version: i64) -> Result<()>;

    /// Applies the v1 schema migration (initial schema setup).
    async fn migrate_v1(pool: &Self::Pool) -> Result<()>;
}

/// Runs all pending migrations for a catalog backend.
///
/// This function checks the current schema version and applies any migrations
/// that haven't been run yet, in order. Each successful migration is recorded
/// to prevent re-application.
///
/// # Arguments
///
/// * `pool` - Database connection pool for the catalog backend
///
/// # Errors
///
/// Returns an error if any migration fails. Migrations are applied
/// sequentially, so a failure will leave the database at the last
/// successfully applied version.
pub async fn run_migrations<M: CatalogMigrations>(pool: &M::Pool) -> Result<()> {
    M::ensure_migrations_table(pool).await?;

    let current_version = M::current_version(pool).await?;

    if current_version < 1 {
        M::migrate_v1(pool).await?;
        M::record_version(pool, 1).await?;
    }

    Ok(())
}
