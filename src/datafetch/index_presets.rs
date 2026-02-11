//! Preset index configuration for creating sorted copies of tables at cache time.
//!
//! Index presets are full copies of tables sorted by different columns,
//! enabling efficient row-group pruning for range queries on those columns.

use std::collections::HashMap;

/// Configuration for a single index preset.
#[derive(Debug, Clone)]
pub struct IndexPreset {
    /// Name of the preset (used in directory structure)
    pub name: String,
    /// Columns to sort by (in order)
    pub sort_columns: Vec<String>,
}

impl IndexPreset {
    pub fn new(name: impl Into<String>, sort_columns: Vec<impl Into<String>>) -> Self {
        Self {
            name: name.into(),
            sort_columns: sort_columns.into_iter().map(|s| s.into()).collect(),
        }
    }
}

/// Registry of index preset configurations per table.
/// Key format: "schema.table" (lowercase)
#[derive(Debug, Clone, Default)]
pub struct IndexPresetRegistry {
    presets: HashMap<String, Vec<IndexPreset>>,
}

impl IndexPresetRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register index presets for a table.
    pub fn register(&mut self, schema: &str, table: &str, presets: Vec<IndexPreset>) {
        let key = format!("{}.{}", schema.to_lowercase(), table.to_lowercase());
        self.presets.insert(key, presets);
    }

    /// Get index presets for a table.
    pub fn get(&self, schema: &str, table: &str) -> Option<&[IndexPreset]> {
        let key = format!("{}.{}", schema.to_lowercase(), table.to_lowercase());
        self.presets.get(&key).map(|v| v.as_slice())
    }

    /// Create a registry with TPC-H optimized index presets.
    pub fn tpch_optimized() -> Self {
        let mut registry = Self::new();

        // lineitem: sorted by l_shipdate (8 queries filter on this)
        registry.register(
            "main",
            "lineitem",
            vec![
                IndexPreset::new("by_shipdate", vec!["l_shipdate"]),
                IndexPreset::new("by_partkey", vec!["l_partkey"]),
            ],
        );

        // orders: sorted by o_orderdate (5 queries filter on this)
        registry.register(
            "main",
            "orders",
            vec![
                IndexPreset::new("by_orderdate", vec!["o_orderdate"]),
                IndexPreset::new("by_custkey", vec!["o_custkey"]),
            ],
        );

        // customer: sorted by c_nationkey (joins)
        registry.register(
            "main",
            "customer",
            vec![IndexPreset::new("by_nationkey", vec!["c_nationkey"])],
        );

        // partsupp: sorted by ps_suppkey (joins)
        registry.register(
            "main",
            "partsupp",
            vec![IndexPreset::new("by_suppkey", vec!["ps_suppkey"])],
        );

        registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_preset_registry() {
        let mut registry = IndexPresetRegistry::new();
        registry.register(
            "main",
            "orders",
            vec![IndexPreset::new("by_date", vec!["order_date"])],
        );

        let presets = registry.get("main", "orders").unwrap();
        assert_eq!(presets.len(), 1);
        assert_eq!(presets[0].name, "by_date");
        assert_eq!(presets[0].sort_columns, vec!["order_date"]);
    }

    #[test]
    fn test_case_insensitive_lookup() {
        let mut registry = IndexPresetRegistry::new();
        registry.register("Main", "Orders", vec![]);

        assert!(registry.get("main", "orders").is_some());
        assert!(registry.get("MAIN", "ORDERS").is_some());
    }

    #[test]
    fn test_tpch_optimized() {
        let registry = IndexPresetRegistry::tpch_optimized();

        let lineitem = registry.get("main", "lineitem").unwrap();
        assert_eq!(lineitem.len(), 2);
        assert_eq!(lineitem[0].name, "by_shipdate");
        assert_eq!(lineitem[1].name, "by_partkey");

        let orders = registry.get("main", "orders").unwrap();
        assert_eq!(orders.len(), 2);
    }
}
