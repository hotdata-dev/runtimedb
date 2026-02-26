use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::LogicalPlan;

/// High-level category describing the shape of a SQL query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryCategory {
    FullScan,
    Projection,
    FilteredScan,
    PointLookup,
    Aggregation,
    Join,
}

impl QueryCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FullScan => "full_scan",
            Self::Projection => "projection",
            Self::FilteredScan => "filtered_scan",
            Self::PointLookup => "point_lookup",
            Self::Aggregation => "aggregation",
            Self::Join => "join",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "full_scan" => Some(Self::FullScan),
            "projection" => Some(Self::Projection),
            "filtered_scan" => Some(Self::FilteredScan),
            "point_lookup" => Some(Self::PointLookup),
            "aggregation" => Some(Self::Aggregation),
            "join" => Some(Self::Join),
            _ => None,
        }
    }
}

impl std::fmt::Display for QueryCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Rich classification result containing the category and boolean flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryClassification {
    pub category: QueryCategory,
    pub num_tables: i32,
    pub has_predicate: bool,
    pub has_join: bool,
    pub has_aggregation: bool,
    pub has_group_by: bool,
    pub has_order_by: bool,
    pub has_limit: bool,
}

/// Classify a logical plan into a [`QueryClassification`].
///
/// Walks the plan tree and determines the highest-priority category based on
/// which operators are present. Priority (highest first):
/// join > aggregation > point_lookup > filtered_scan > projection > full_scan
pub fn classify_plan(plan: &LogicalPlan) -> QueryClassification {
    let mut has_join = false;
    let mut has_aggregation = false;
    let mut has_predicate = false;
    let mut has_limit = false;
    let mut limit_fetch_one = false;
    let mut has_projection = false;
    let mut num_tables: i32 = 0;
    let mut has_group_by = false;
    let mut has_order_by = false;

    plan.apply(|node| {
        match node {
            LogicalPlan::Join(_) => {
                has_join = true;
            }
            LogicalPlan::Aggregate(agg) => {
                has_aggregation = true;
                if !agg.group_expr.is_empty() {
                    has_group_by = true;
                }
            }
            LogicalPlan::Filter(_) => {
                has_predicate = true;
            }
            LogicalPlan::Limit(limit) => {
                has_limit = true;
                if let Some(fetch) = &limit.fetch {
                    // Check if it's a literal 1
                    if let datafusion::logical_expr::Expr::Literal(
                        datafusion::common::ScalarValue::Int64(Some(1)),
                        _,
                    ) = fetch.as_ref()
                    {
                        limit_fetch_one = true;
                    }
                }
            }
            LogicalPlan::Sort(_) => {
                has_order_by = true;
            }
            LogicalPlan::Projection(_) => {
                has_projection = true;
            }
            LogicalPlan::TableScan(_) => {
                num_tables += 1;
            }
            _ => {}
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .ok();

    // Priority: join > aggregation > point_lookup > filtered_scan > projection > full_scan
    let category = if has_join {
        QueryCategory::Join
    } else if has_aggregation {
        QueryCategory::Aggregation
    } else if has_predicate && has_limit && limit_fetch_one {
        QueryCategory::PointLookup
    } else if has_predicate {
        QueryCategory::FilteredScan
    } else if has_projection && num_tables > 0 {
        QueryCategory::Projection
    } else {
        QueryCategory::FullScan
    };

    QueryClassification {
        category,
        num_tables,
        has_predicate,
        has_join,
        has_aggregation,
        has_group_by,
        has_order_by,
        has_limit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
    use datafusion::logical_expr::{col, lit, LogicalPlanBuilder};
    use std::sync::Arc;

    fn table_source(schema: Schema) -> Arc<LogicalTableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(schema)))
    }

    fn test_table_scan() -> LogicalPlanBuilder {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]);
        LogicalPlanBuilder::scan("test_table", table_source(schema), None).unwrap()
    }

    #[test]
    fn test_full_scan() {
        let plan = test_table_scan().build().unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::FullScan);
        assert_eq!(c.num_tables, 1);
        assert!(!c.has_predicate);
        assert!(!c.has_join);
        assert!(!c.has_aggregation);
        assert!(!c.has_group_by);
        assert!(!c.has_order_by);
        assert!(!c.has_limit);
    }

    #[test]
    fn test_projection() {
        let plan = test_table_scan()
            .project(vec![col("id"), col("name")])
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::Projection);
        assert_eq!(c.num_tables, 1);
        assert!(!c.has_predicate);
    }

    #[test]
    fn test_filtered_scan() {
        let plan = test_table_scan()
            .filter(col("id").gt(lit(10)))
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::FilteredScan);
        assert!(c.has_predicate);
        assert!(!c.has_limit);
    }

    #[test]
    fn test_point_lookup() {
        let plan = test_table_scan()
            .filter(col("id").eq(lit(42)))
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::PointLookup);
        assert!(c.has_predicate);
        assert!(c.has_limit);
    }

    #[test]
    fn test_aggregation() {
        let plan = test_table_scan()
            .aggregate(
                vec![col("name")],
                vec![datafusion::functions_aggregate::count::count(col("id"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::Aggregation);
        assert!(c.has_aggregation);
        assert!(c.has_group_by);
    }

    #[test]
    fn test_aggregation_without_group_by() {
        let plan = test_table_scan()
            .aggregate(
                Vec::<datafusion::logical_expr::Expr>::new(),
                vec![datafusion::functions_aggregate::count::count(col("id"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::Aggregation);
        assert!(c.has_aggregation);
        assert!(!c.has_group_by);
    }

    #[test]
    fn test_join() {
        let left = test_table_scan();
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("dept", DataType::Utf8, true),
        ]);
        let right = LogicalPlanBuilder::scan("other_table", table_source(schema2), None).unwrap();

        let plan = left
            .join(
                right.build().unwrap(),
                datafusion::logical_expr::JoinType::Inner,
                (vec!["id"], vec!["id"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::Join);
        assert!(c.has_join);
        assert_eq!(c.num_tables, 2);
    }

    #[test]
    fn test_join_beats_aggregation() {
        let left = test_table_scan();
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("dept", DataType::Utf8, true),
        ]);
        let right = LogicalPlanBuilder::scan("other_table", table_source(schema2), None).unwrap();

        let plan = left
            .join(
                right.build().unwrap(),
                datafusion::logical_expr::JoinType::Inner,
                (vec!["id"], vec!["id"]),
                None,
            )
            .unwrap()
            .aggregate(
                vec![col("test_table.id")],
                vec![datafusion::functions_aggregate::count::count(col(
                    "other_table.dept",
                ))],
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(classify_plan(&plan).category, QueryCategory::Join);
    }

    #[test]
    fn test_aggregation_beats_filter() {
        let plan = test_table_scan()
            .filter(col("id").gt(lit(5)))
            .unwrap()
            .aggregate(
                vec![col("name")],
                vec![datafusion::functions_aggregate::count::count(col("id"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::Aggregation);
        assert!(c.has_predicate);
        assert!(c.has_aggregation);
    }

    #[test]
    fn test_filter_with_limit_not_one_is_filtered_scan() {
        let plan = test_table_scan()
            .filter(col("id").gt(lit(10)))
            .unwrap()
            .limit(0, Some(10))
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert_eq!(c.category, QueryCategory::FilteredScan);
        assert!(c.has_limit);
        assert!(c.has_predicate);
    }

    #[test]
    fn test_order_by() {
        let plan = test_table_scan()
            .sort(vec![col("id").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        let c = classify_plan(&plan);
        assert!(c.has_order_by);
        assert!(!c.has_predicate);
    }

    #[test]
    fn test_num_tables_single() {
        let plan = test_table_scan().build().unwrap();
        assert_eq!(classify_plan(&plan).num_tables, 1);
    }

    #[test]
    fn test_num_tables_join() {
        let left = test_table_scan();
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("dept", DataType::Utf8, true),
        ]);
        let right = LogicalPlanBuilder::scan("other_table", table_source(schema2), None).unwrap();
        let plan = left
            .join(
                right.build().unwrap(),
                datafusion::logical_expr::JoinType::Inner,
                (vec!["id"], vec!["id"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(classify_plan(&plan).num_tables, 2);
    }

    #[test]
    fn test_parse_roundtrip() {
        for cat in [
            QueryCategory::FullScan,
            QueryCategory::Projection,
            QueryCategory::FilteredScan,
            QueryCategory::PointLookup,
            QueryCategory::Aggregation,
            QueryCategory::Join,
        ] {
            assert_eq!(QueryCategory::parse(cat.as_str()), Some(cat));
        }
        assert_eq!(QueryCategory::parse("unknown"), None);
    }
}
