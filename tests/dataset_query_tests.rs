//! Tests for querying datasets via DataFusion.

use datafusion::arrow::array::Array;
use runtimedb::datasets::DEFAULT_SCHEMA;
use runtimedb::http::models::{DatasetSource, InlineData};
use runtimedb::RuntimeEngine;
use tempfile::TempDir;

/// Helper to get string value from any string-like array type.
fn get_string_value(array: &dyn Array, index: usize) -> String {
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    // Cast to Utf8 (StringArray) to normalize string types
    let casted = cast(array, &DataType::Utf8).expect("Should be castable to Utf8");
    let string_array = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Should be StringArray after cast");
    string_array.value(index).to_string()
}

/// Generate a test secret key (base64-encoded 32 bytes)
fn test_secret_key() -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use rand::RngCore;
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    STANDARD.encode(key)
}

async fn create_test_engine() -> (RuntimeEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path().to_path_buf())
        .secret_key(test_secret_key())
        .build()
        .await
        .unwrap();
    (engine, temp_dir)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_dataset_after_inline_creation() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset with inline CSV data
    engine
        .create_dataset(
            "Numbers",
            Some("numbers"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    columns: None,
                    content: "id,value\n1,one\n2,two\n3,three".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Query it via DataFusion
    let result = engine
        .execute_query(&format!(
            "SELECT * FROM datasets.{}.numbers ORDER BY id",
            DEFAULT_SCHEMA
        ))
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_dataset_with_aggregation() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset with inline CSV data
    engine
        .create_dataset(
            "Sales",
            Some("sales"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    columns: None,
                    content: "product,amount\napple,10\nbanana,20\napple,30\nbanana,40".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Run aggregation query
    let result = engine
        .execute_query(&format!(
            "SELECT product, SUM(amount) as total FROM datasets.{}.sales GROUP BY product ORDER BY product",
            DEFAULT_SCHEMA
        ))
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 2);

    // Verify the aggregation results by checking column names and basic structure
    let batch = &result.results[0];
    let product_col = batch.column_by_name("product").unwrap();
    let total_col = batch.column_by_name("total").unwrap();

    // Use helper for string columns
    assert_eq!(get_string_value(product_col.as_ref(), 0), "apple");
    assert_eq!(get_string_value(product_col.as_ref(), 1), "banana");

    // Verify totals exist (numeric type may vary)
    assert!(!total_col.is_null(0));
    assert!(!total_col.is_null(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_nonexistent_dataset() {
    let (engine, _temp) = create_test_engine().await;

    // Query a dataset that doesn't exist
    let result = engine
        .execute_query(&format!(
            "SELECT * FROM datasets.{}.nonexistent",
            DEFAULT_SCHEMA
        ))
        .await;

    // Should fail with an error about the table not existing
    assert!(result.is_err());
    let err = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Expected error for nonexistent table"),
    };
    assert!(
        err.contains("nonexistent") || err.contains("not found") || err.contains("doesn't exist"),
        "Error should mention the table name: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_multiple_datasets() {
    let (engine, _temp) = create_test_engine().await;

    // Create first dataset
    engine
        .create_dataset(
            "Users",
            Some("users"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    columns: None,
                    content: "id,name\n1,Alice\n2,Bob".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Create second dataset
    engine
        .create_dataset(
            "Orders",
            Some("orders"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    columns: None,
                    content: "id,user_id,amount\n100,1,50\n101,2,75\n102,1,25".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Join the two datasets
    let result = engine
        .execute_query(&format!(
            "SELECT u.name, SUM(o.amount) as total_amount
             FROM datasets.{schema}.users u
             JOIN datasets.{schema}.orders o ON u.id = o.user_id
             GROUP BY u.name
             ORDER BY u.name",
            schema = DEFAULT_SCHEMA
        ))
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 2);

    // Verify the join results
    let batch = &result.results[0];
    let name_col = batch.column_by_name("name").unwrap();
    let total_col = batch.column_by_name("total_amount").unwrap();

    // Use helper for string columns
    assert_eq!(get_string_value(name_col.as_ref(), 0), "Alice");
    assert_eq!(get_string_value(name_col.as_ref(), 1), "Bob");

    // Verify totals exist (numeric type may vary)
    assert!(!total_col.is_null(0));
    assert!(!total_col.is_null(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataset_columns_queryable() {
    let (engine, _temp) = create_test_engine().await;

    // Create a dataset with specific columns
    engine
        .create_dataset(
            "TestData",
            Some("test_data"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    columns: None,
                    content: "col1,col2\na,1\nb,2".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Verify we can query specific columns
    let result = engine
        .execute_query(&format!(
            "SELECT col1, col2 FROM datasets.{}.test_data ORDER BY col1",
            DEFAULT_SCHEMA
        ))
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 2);

    // Verify column values
    let batch = &result.results[0];
    let col1 = batch.column_by_name("col1").unwrap();
    let col2 = batch.column_by_name("col2").unwrap();

    // Use helper for string columns
    assert_eq!(get_string_value(col1.as_ref(), 0), "a");
    assert_eq!(get_string_value(col1.as_ref(), 1), "b");

    // Verify col2 values exist (numeric type may vary)
    assert!(!col2.is_null(0));
    assert!(!col2.is_null(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_dataset_with_json_data() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset with inline JSON data
    engine
        .create_dataset(
            "JsonData",
            Some("json_data"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "json".to_string(),
                    content: r#"{"id": 1, "name": "alpha"}
{"id": 2, "name": "beta"}
{"id": 3, "name": "gamma"}"#
                        .to_string(),
                    columns: None,
                },
            },
        )
        .await
        .unwrap();

    // Query it
    let result = engine
        .execute_query(&format!(
            "SELECT * FROM datasets.{}.json_data ORDER BY id",
            DEFAULT_SCHEMA
        ))
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 3);
}
