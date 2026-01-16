//! Schema migration support for catalog backends.
//!
//! This module provides a trait-based migration system that allows different
//! catalog backends (e.g., SQLite, PostgreSQL) to implement their own migration
//! logic while sharing a common execution framework.
//!
//! ## Hash Verification
//!
//! Migration SQL files are hashed at compile time using build.rs. When applying
//! migrations, the hash is stored alongside the version. On subsequent startups,
//! if a migration version exists but the hash doesn't match, startup fails with
//! an error instructing developers to wipe/recreate the database.

use anyhow::{bail, Result};

// Include the generated migration data
include!(concat!(env!("OUT_DIR"), "/migrations.rs"));

/// Error returned when a migration hash mismatch is detected.
#[derive(Debug)]
pub struct MigrationHashMismatch {
    pub version: i64,
    pub compiled_hash: String,
    pub stored_hash: String,
}

impl std::fmt::Display for MigrationHashMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Migration v{} was modified after being applied!\n\
             Compiled hash (current code): {}\n\
             Stored hash (in database):    {}\n\n\
             This database was created with a different version of migration v{}.\n\
             During development, you should wipe and recreate the database.\n\
             Run: rm -rf <your-db-path> and restart the application.",
            self.version, self.compiled_hash, self.stored_hash, self.version
        )
    }
}

impl std::error::Error for MigrationHashMismatch {}

/// Error returned when the database has missing intermediate migrations.
#[derive(Debug)]
pub struct MissingMigration {
    pub version: i64,
    pub max_applied: i64,
}

impl std::fmt::Display for MissingMigration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Migration v{} is missing but v{} was applied!\n\
             The database state is inconsistent - migrations must be applied sequentially.\n\
             During development, you should wipe and recreate the database.",
            self.version, self.max_applied
        )
    }
}

impl std::error::Error for MissingMigration {}

/// Error returned when the database has migrations not present in the compiled list.
#[derive(Debug)]
pub struct UnknownMigration {
    pub version: i64,
    pub max_compiled: i64,
}

impl std::fmt::Display for UnknownMigration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Database has migration v{} but compiled code only knows up to v{}!\n\
             The database was created with a newer version of the application.\n\
             During development, you should wipe and recreate the database.",
            self.version, self.max_compiled
        )
    }
}

impl std::error::Error for UnknownMigration {}

/// Trait for implementing catalog schema migrations.
///
/// Each catalog backend implements this trait to provide database-specific
/// migration logic. The trait defines methods for tracking migration state
/// and applying schema changes.
pub trait CatalogMigrations: Send + Sync {
    /// The database connection pool type for this backend.
    type Pool;

    /// Returns the compile-time migrations for this backend.
    fn migrations() -> &'static [Migration];

    /// Creates the migrations tracking table if it doesn't exist.
    async fn ensure_migrations_table(pool: &Self::Pool) -> Result<()>;

    /// Returns all applied migration versions with their hashes.
    async fn get_applied_migrations(pool: &Self::Pool) -> Result<Vec<(i64, String)>>;

    /// Applies a single migration within a transaction.
    async fn apply_migration(pool: &Self::Pool, version: i64, hash: &str, sql: &str) -> Result<()>;
}

/// Wraps migration SQL in a transaction with version recording.
///
/// The returned SQL includes BEGIN/COMMIT to run atomically. If an error occurs
/// mid-transaction, the connection may be left in an aborted state until it is
/// returned to the pool and recycled. This is acceptable for migrations since
/// they run at startup and a failure is fatal anyway.
///
/// Note: build.rs validates that migrations don't contain `{}` or `"##` sequences.
pub fn wrap_migration_sql(sql: &str, version: i64, hash: &str) -> String {
    format!(
        "BEGIN;\n{}\nINSERT INTO schema_migrations (version, hash) VALUES ({}, '{}');\nCOMMIT;",
        sql, version, hash
    )
}

/// Runs all pending migrations for a catalog backend.
pub async fn run_migrations<M: CatalogMigrations>(pool: &M::Pool) -> Result<()> {
    M::ensure_migrations_table(pool).await?;

    let migrations = M::migrations();
    let applied = M::get_applied_migrations(pool).await?;

    // Build a map of applied version -> hash for quick lookup
    let applied_map: std::collections::HashMap<i64, String> = applied.into_iter().collect();
    let max_applied = applied_map.keys().max().copied().unwrap_or(0);
    let compiled_versions: std::collections::HashSet<i64> =
        migrations.iter().map(|m| m.version).collect();
    let max_compiled = compiled_versions.iter().max().copied().unwrap_or(0);

    // Check for migrations in DB that don't exist in compiled code
    for &applied_version in applied_map.keys() {
        if !compiled_versions.contains(&applied_version) {
            bail!(UnknownMigration {
                version: applied_version,
                max_compiled,
            });
        }
    }

    // Verify hashes and check for missing intermediate migrations
    for migration in migrations {
        if let Some(stored_hash) = applied_map.get(&migration.version) {
            if stored_hash != migration.hash {
                bail!(MigrationHashMismatch {
                    version: migration.version,
                    compiled_hash: migration.hash.to_string(),
                    stored_hash: stored_hash.clone(),
                });
            }
        } else if migration.version < max_applied {
            bail!(MissingMigration {
                version: migration.version,
                max_applied,
            });
        }
    }

    // Apply any pending migrations
    for migration in migrations {
        if migration.version > max_applied {
            M::apply_migration(pool, migration.version, migration.hash, migration.sql).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_migration_sql_wraps_correctly() {
        let sql = "CREATE TABLE foo (id INT)";
        let result = wrap_migration_sql(sql, 1, "abc123");

        assert!(result.contains("BEGIN;"));
        assert!(result.contains("CREATE TABLE foo (id INT)"));
        assert!(result.contains("VALUES (1, 'abc123')"));
        assert!(result.contains("COMMIT;"));
    }

    // Note: Curly brace validation is done at compile time in build.rs,
    // which rejects migrations containing `{}` or `"##` sequences.
}
