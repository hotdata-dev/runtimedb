//! Build script to compute SHA-256 hashes for migration SQL files.
//!
//! This scans the migrations/ directory for backend subdirectories (e.g., postgres/, sqlite/)
//! and generates migration_hashes.rs containing compile-time hashes and SQL content
//! for each migration version per backend.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let migrations_dir = Path::new(&manifest_dir).join("migrations");

    // Rerun if migrations directory changes
    println!("cargo:rerun-if-changed={}", migrations_dir.display());

    // Collect migrations per backend: backend_name -> [(version, hash, sql)]
    let mut backends: BTreeMap<String, Vec<(i64, String, String)>> = BTreeMap::new();

    if migrations_dir.exists() {
        for entry in fs::read_dir(&migrations_dir).expect("Failed to read migrations dir") {
            let entry = entry.expect("Failed to read entry");
            let path = entry.path();

            if path.is_dir() {
                let backend_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .expect("Invalid backend directory name")
                    .to_string();

                println!("cargo:rerun-if-changed={}", path.display());

                let mut migrations = Vec::new();

                for sql_entry in fs::read_dir(&path).expect("Failed to read backend dir") {
                    let sql_entry = sql_entry.expect("Failed to read sql entry");
                    let sql_path = sql_entry.path();

                    if sql_path.extension().is_some_and(|ext| ext == "sql") {
                        if let Some(version) = parse_version(&sql_path) {
                            let content = fs::read_to_string(&sql_path)
                                .unwrap_or_else(|_| panic!("Failed to read {:?}", sql_path));

                            // Check for raw string delimiter that would break generated code
                            if content.contains("\"##") {
                                panic!(
                                    "Migration {:?} contains '\"##' which breaks raw string literals. \
                                     Please rewrite the SQL to avoid this sequence.",
                                    sql_path
                                );
                            }

                            let hash = sha256_hex(&content);

                            println!("cargo:rerun-if-changed={}", sql_path.display());
                            migrations.push((version, hash, content));
                        }
                    }
                }

                // Sort by version
                migrations.sort_by_key(|(v, _, _)| *v);

                if migrations.is_empty() {
                    println!("cargo:warning=No migrations found in {}", path.display());
                } else {
                    // Validate sequential versions starting from 1
                    for (i, (version, _, _)) in migrations.iter().enumerate() {
                        let expected = (i + 1) as i64;
                        if *version != expected {
                            panic!(
                                "Migration version gap in {}: expected v{}.sql but found v{}.sql. \
                                 Migrations must be sequential starting from v1.",
                                backend_name, expected, version
                            );
                        }
                    }
                }

                backends.insert(backend_name, migrations);
            }
        }
    }

    // Validate all backends have the same migration versions
    if backends.len() > 1 {
        let backend_versions: Vec<(&String, Vec<i64>)> = backends
            .iter()
            .map(|(name, migrations)| (name, migrations.iter().map(|(v, _, _)| *v).collect()))
            .collect();

        let (first_name, first_versions) = &backend_versions[0];
        for (other_name, other_versions) in &backend_versions[1..] {
            if first_versions != other_versions {
                let first_set: std::collections::HashSet<i64> =
                    first_versions.iter().copied().collect();
                let other_set: std::collections::HashSet<i64> =
                    other_versions.iter().copied().collect();

                let missing_in_other: Vec<i64> =
                    first_set.difference(&other_set).copied().collect();
                let missing_in_first: Vec<i64> =
                    other_set.difference(&first_set).copied().collect();

                let mut msg = format!(
                    "Migration version mismatch between backends {} and {}.",
                    first_name, other_name
                );
                if !missing_in_other.is_empty() {
                    msg.push_str(&format!(
                        " {} is missing versions: {:?}.",
                        other_name, missing_in_other
                    ));
                }
                if !missing_in_first.is_empty() {
                    msg.push_str(&format!(
                        " {} is missing versions: {:?}.",
                        first_name, missing_in_first
                    ));
                }
                panic!("{}", msg);
            }
        }
    }

    // Generate the Rust source file
    let mut code = String::new();
    code.push_str("// Auto-generated migration data. Do not edit.\n\n");

    // Generate a struct to hold migration data
    code.push_str("/// A single migration with version, hash, and SQL content.\n");
    code.push_str("#[derive(Debug, Clone, Copy)]\n");
    code.push_str("pub struct Migration {\n");
    code.push_str("    pub version: i64,\n");
    code.push_str("    pub hash: &'static str,\n");
    code.push_str("    pub sql: &'static str,\n");
    code.push_str("}\n\n");

    // Generate constants for each backend
    for (backend, migrations) in &backends {
        let const_name = format!("{}_MIGRATIONS", backend.to_uppercase());
        code.push_str(&format!(
            "/// Compile-time migrations for {} backend.\n",
            backend
        ));
        code.push_str(&format!("pub const {}: &[Migration] = &[\n", const_name));

        for (version, hash, sql) in migrations {
            // Use r##"..."## to handle SQL that might contain "#
            code.push_str(&format!(
                "    Migration {{ version: {}, hash: \"{}\", sql: r##\"{}\"## }},\n",
                version, hash, sql
            ));
        }

        code.push_str("];\n\n");
    }

    let dest_path = Path::new(&out_dir).join("migrations.rs");
    fs::write(&dest_path, code).expect("Failed to write migrations.rs");
}

/// Parse version number from filename like "v1.sql" -> 1
fn parse_version(path: &Path) -> Option<i64> {
    let stem = path.file_stem()?.to_str()?;
    stem.strip_prefix('v').and_then(|s| s.parse().ok())
}

/// Compute SHA-256 hash and return as hex string
fn sha256_hex(data: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    format!("{:x}", hasher.finalize())
}
