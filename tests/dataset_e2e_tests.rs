//! End-to-end tests for the complete dataset upload flow.
//!
//! These tests exercise the full lifecycle:
//! 1. Upload raw data (CSV/JSON) via engine.store_upload()
//! 2. Create dataset from upload via engine.create_dataset()
//! 3. Query dataset via engine.execute_query() with `datasets.default.table_name`
//! 4. Verify catalog state changes (upload consumed, dataset listed)
//! 5. Test update and delete effects on queries

use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use runtimedb::http::models::{DatasetSource, InlineData};
use runtimedb::RuntimeEngine;
use tempfile::TempDir;

/// Helper to get string value from any string-like array type.
fn get_string_value(array: &dyn Array, index: usize) -> String {
    let casted = cast(array, &DataType::Utf8).expect("Should be castable to Utf8");
    let string_array = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Should be StringArray after cast");
    string_array.value(index).to_string()
}

/// Helper to get i64 value from numeric array.
fn get_i64_value(array: &dyn Array, index: usize) -> i64 {
    let casted = cast(array, &DataType::Int64).expect("Should be castable to Int64");
    let int_array = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Should be Int64Array after cast");
    int_array.value(index)
}

/// Helper to get f64 value from numeric array.
fn get_f64_value(array: &dyn Array, index: usize) -> f64 {
    let casted = cast(array, &DataType::Float64).expect("Should be castable to Float64");
    let float_array = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Should be Float64Array after cast");
    float_array.value(index)
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
async fn test_full_upload_to_query_flow() {
    let (engine, _temp) = create_test_engine().await;

    // 1. Upload CSV data using store_upload
    let csv_data = b"product,price,quantity\nWidget,10.50,100\nGadget,25.00,50";
    let upload = engine
        .store_upload(csv_data.to_vec(), Some("text/csv".to_string()))
        .await
        .unwrap();

    // 2. Verify upload is in catalog with pending status
    let retrieved = engine
        .catalog()
        .get_upload(&upload.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.status, "pending");
    assert_eq!(retrieved.size_bytes, csv_data.len() as i64);

    // 3. Create dataset from upload
    let dataset = engine
        .create_dataset(
            "Product Inventory",
            None, // auto-generate table_name
            DatasetSource::Upload {
                upload_id: upload.id.clone(),
                format: Some("csv".to_string()),
            },
        )
        .await
        .unwrap();

    assert_eq!(dataset.label, "Product Inventory");
    assert_eq!(dataset.table_name, "product_inventory");
    assert_eq!(dataset.schema_name, "default");

    // 4. Verify upload is consumed
    let consumed_upload = engine
        .catalog()
        .get_upload(&upload.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(consumed_upload.status, "consumed");
    assert!(consumed_upload.consumed_at.is_some());

    // 5. Query the dataset with calculation
    let result = engine
        .execute_query(
            "SELECT product, price * quantity as total FROM datasets.default.product_inventory ORDER BY total DESC",
        )
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 2);

    // Verify query results
    let batch = &result.results[0];
    let product_col = batch.column_by_name("product").unwrap();
    let total_col = batch.column_by_name("total").unwrap();

    // Gadget has higher total (25.00 * 50 = 1250)
    assert_eq!(get_string_value(product_col.as_ref(), 0), "Gadget");
    assert_eq!(get_string_value(product_col.as_ref(), 1), "Widget");
    assert_eq!(get_f64_value(total_col.as_ref(), 0), 1250.0);
    assert_eq!(get_f64_value(total_col.as_ref(), 1), 1050.0);

    // 6. List datasets shows it
    let (datasets, has_more) = engine.catalog().list_datasets(100, 0).await.unwrap();
    assert_eq!(datasets.len(), 1);
    assert!(!has_more);
    assert_eq!(datasets[0].id, dataset.id);
    assert_eq!(datasets[0].label, "Product Inventory");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_json_upload_and_query() {
    let (engine, _temp) = create_test_engine().await;

    let json_data = br#"{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}"#;

    let upload = engine
        .store_upload(json_data.to_vec(), Some("application/json".to_string()))
        .await
        .unwrap();

    let dataset = engine
        .create_dataset(
            "People",
            Some("people"),
            DatasetSource::Upload {
                upload_id: upload.id,
                format: Some("json".to_string()),
            },
        )
        .await
        .unwrap();

    assert_eq!(dataset.table_name, "people");

    // Query with WHERE clause
    let result = engine
        .execute_query("SELECT name FROM datasets.default.people WHERE age > 26 ORDER BY name")
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 2);

    let batch = &result.results[0];
    let name_col = batch.column_by_name("name").unwrap();
    assert_eq!(get_string_value(name_col.as_ref(), 0), "Alice");
    assert_eq!(get_string_value(name_col.as_ref(), 1), "Charlie");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_inline_creation_and_query() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset with inline CSV data (no file upload)
    let dataset = engine
        .create_dataset(
            "Sales Data",
            Some("sales"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "region,amount\nNorth,1000\nSouth,1500\nEast,800\nWest,1200"
                        .to_string(),
                },
            },
        )
        .await
        .unwrap();

    assert_eq!(dataset.label, "Sales Data");
    assert_eq!(dataset.table_name, "sales");

    // Run aggregation query
    let result = engine
        .execute_query("SELECT SUM(amount) as total FROM datasets.default.sales")
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 1);

    let batch = &result.results[0];
    let total_col = batch.column_by_name("total").unwrap();
    assert_eq!(get_i64_value(total_col.as_ref(), 0), 4500);

    // Also verify listing shows the dataset
    let (datasets, _) = engine.catalog().list_datasets(100, 0).await.unwrap();
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].id, dataset.id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_inline_json_creation() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset with inline JSON data
    let dataset = engine
        .create_dataset(
            "Metrics",
            Some("metrics"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "json".to_string(),
                    content: r#"{"metric": "cpu", "value": 75.5}
{"metric": "memory", "value": 82.3}
{"metric": "disk", "value": 45.0}"#
                        .to_string(),
                },
            },
        )
        .await
        .unwrap();

    assert_eq!(dataset.table_name, "metrics");

    // Query and verify
    let result = engine
        .execute_query("SELECT metric, value FROM datasets.default.metrics WHERE value > 50 ORDER BY value DESC")
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 2);

    let batch = &result.results[0];
    let metric_col = batch.column_by_name("metric").unwrap();
    assert_eq!(get_string_value(metric_col.as_ref(), 0), "memory");
    assert_eq!(get_string_value(metric_col.as_ref(), 1), "cpu");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataset_update_affects_queries() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset
    let dataset = engine
        .create_dataset(
            "Original Name",
            Some("original_table"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "id,value\n1,100\n2,200".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Query works with original table name
    let result = engine
        .execute_query("SELECT * FROM datasets.default.original_table")
        .await;
    assert!(result.is_ok());

    // Update the table_name via catalog
    let updated = engine
        .catalog()
        .update_dataset(&dataset.id, "Updated Name", "updated_table")
        .await
        .unwrap();
    assert!(updated);

    // Verify the dataset was updated
    let retrieved = engine
        .catalog()
        .get_dataset(&dataset.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.label, "Updated Name");
    assert_eq!(retrieved.table_name, "updated_table");

    // Note: The catalog metadata is updated, but DataFusion's table registry
    // may still have the old registration. In a real scenario, you'd need to
    // re-register or the system would need to handle dynamic table name changes.
    // This test verifies the catalog update works correctly.
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dataset_deletion_removes_from_catalog() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset
    let dataset = engine
        .create_dataset(
            "To Be Deleted",
            Some("deletable"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "col1,col2\na,1\nb,2".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Verify it exists
    let exists = engine
        .catalog()
        .get_dataset(&dataset.id)
        .await
        .unwrap()
        .is_some();
    assert!(exists);

    // Query works
    let result = engine
        .execute_query("SELECT * FROM datasets.default.deletable")
        .await;
    assert!(result.is_ok());

    // Delete the dataset
    let deleted = engine.catalog().delete_dataset(&dataset.id).await.unwrap();
    assert!(deleted.is_some());
    assert_eq!(deleted.unwrap().id, dataset.id);

    // Verify it's gone from catalog
    let gone = engine
        .catalog()
        .get_dataset(&dataset.id)
        .await
        .unwrap()
        .is_none();
    assert!(gone);

    // List datasets should be empty
    let (datasets, _) = engine.catalog().list_datasets(100, 0).await.unwrap();
    assert!(datasets.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cannot_consume_upload_twice() {
    let (engine, _temp) = create_test_engine().await;

    // Upload data
    let csv_data = b"x,y\n1,2\n3,4";
    let upload = engine
        .store_upload(csv_data.to_vec(), Some("text/csv".to_string()))
        .await
        .unwrap();

    // Create first dataset (consumes the upload)
    let _dataset1 = engine
        .create_dataset(
            "First Dataset",
            Some("first"),
            DatasetSource::Upload {
                upload_id: upload.id.clone(),
                format: Some("csv".to_string()),
            },
        )
        .await
        .unwrap();

    // Try to create another dataset from same upload - should fail
    let result = engine
        .create_dataset(
            "Second Dataset",
            Some("second"),
            DatasetSource::Upload {
                upload_id: upload.id.clone(),
                format: Some("csv".to_string()),
            },
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("consumed") || err.contains("already"),
        "Error should mention upload was consumed: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_name_uniqueness() {
    let (engine, _temp) = create_test_engine().await;

    // Create first dataset
    let _dataset1 = engine
        .create_dataset(
            "First",
            Some("unique_table"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "a,b\n1,2".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Try to create another dataset with same table_name - should fail
    let result = engine
        .create_dataset(
            "Second",
            Some("unique_table"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "c,d\n3,4".to_string(),
                },
            },
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("unique_table") && (err.contains("already") || err.contains("in use")),
        "Error should mention table name conflict: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_auto_generated_table_name() {
    let (engine, _temp) = create_test_engine().await;

    // Create dataset with spaces and special chars in label, no explicit table_name
    let dataset = engine
        .create_dataset(
            "My Sales Report 2024!",
            None, // auto-generate
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "month,revenue\nJan,1000\nFeb,1200".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Table name should be sanitized/normalized
    assert_eq!(dataset.label, "My Sales Report 2024!");
    // The actual generated name depends on the sanitization logic
    assert!(!dataset.table_name.is_empty());
    assert!(!dataset.table_name.contains(' '));
    assert!(!dataset.table_name.contains('!'));

    // Verify query works with generated name
    let result = engine
        .execute_query(&format!(
            "SELECT * FROM datasets.default.{}",
            dataset.table_name
        ))
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reserved_word_label_rejected() {
    let (engine, _temp) = create_test_engine().await;

    // Labels that would generate SQL reserved words as table names should be rejected
    let reserved_labels = ["select", "SELECT", "from", "where", "table", "order"];

    for label in reserved_labels {
        let result = engine
            .create_dataset(
                label,
                None, // auto-generate table name
                DatasetSource::Inline {
                    inline: InlineData {
                        format: "csv".to_string(),
                        content: "a,b\n1,2".to_string(),
                    },
                },
            )
            .await;

        assert!(
            result.is_err(),
            "Label '{}' should produce invalid table name",
            label
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("reserved") || err.contains("Cannot create valid table name"),
            "Error for label '{}' should mention reserved word issue: {}",
            label,
            err
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_datasets_join() {
    let (engine, _temp) = create_test_engine().await;

    // Create customers dataset
    engine
        .create_dataset(
            "Customers",
            Some("customers"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "customer_id,name\n1,Alice\n2,Bob\n3,Charlie".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Create orders dataset
    engine
        .create_dataset(
            "Orders",
            Some("orders"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "order_id,customer_id,amount\n100,1,50\n101,1,75\n102,2,100"
                        .to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Join the datasets
    let result = engine
        .execute_query(
            "SELECT c.name, SUM(o.amount) as total_spent
             FROM datasets.default.customers c
             JOIN datasets.default.orders o ON c.customer_id = o.customer_id
             GROUP BY c.name
             ORDER BY total_spent DESC",
        )
        .await
        .unwrap();

    assert_eq!(result.results.len(), 1);
    assert_eq!(result.results[0].num_rows(), 2); // Alice and Bob have orders

    let batch = &result.results[0];
    let name_col = batch.column_by_name("name").unwrap();
    let total_col = batch.column_by_name("total_spent").unwrap();

    // Alice has 50+75=125, Bob has 100
    assert_eq!(get_string_value(name_col.as_ref(), 0), "Alice");
    assert_eq!(get_i64_value(total_col.as_ref(), 0), 125);
    assert_eq!(get_string_value(name_col.as_ref(), 1), "Bob");
    assert_eq!(get_i64_value(total_col.as_ref(), 1), 100);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_format_auto_detection_csv() {
    let (engine, _temp) = create_test_engine().await;

    // Upload with CSV content type but no explicit format
    let csv_data = b"name,score\nTest,100";
    let upload = engine
        .store_upload(csv_data.to_vec(), Some("text/csv".to_string()))
        .await
        .unwrap();

    // Create dataset without specifying format - should auto-detect from content_type
    let _dataset = engine
        .create_dataset(
            "Auto CSV",
            Some("auto_csv"),
            DatasetSource::Upload {
                upload_id: upload.id,
                format: None, // auto-detect
            },
        )
        .await
        .unwrap();

    // Query should work
    let result = engine
        .execute_query("SELECT * FROM datasets.default.auto_csv")
        .await
        .unwrap();
    assert_eq!(result.results[0].num_rows(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_format_auto_detection_json() {
    let (engine, _temp) = create_test_engine().await;

    // Upload with JSON content type but no explicit format
    let json_data = br#"{"key": "value"}"#;
    let upload = engine
        .store_upload(json_data.to_vec(), Some("application/json".to_string()))
        .await
        .unwrap();

    // Create dataset without specifying format - should auto-detect from content_type
    let _dataset = engine
        .create_dataset(
            "Auto JSON",
            Some("auto_json"),
            DatasetSource::Upload {
                upload_id: upload.id,
                format: None, // auto-detect
            },
        )
        .await
        .unwrap();

    // Query should work
    let result = engine
        .execute_query("SELECT * FROM datasets.default.auto_json")
        .await
        .unwrap();
    assert_eq!(result.results[0].num_rows(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nonexistent_upload_fails() {
    let (engine, _temp) = create_test_engine().await;

    // Try to create dataset from non-existent upload
    let result = engine
        .create_dataset(
            "Invalid",
            Some("invalid"),
            DatasetSource::Upload {
                upload_id: "upld_nonexistent_12345".to_string(),
                format: Some("csv".to_string()),
            },
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found") || err.contains("nonexistent"),
        "Error should mention upload not found: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_data_fails() {
    let (engine, _temp) = create_test_engine().await;

    // Try to create dataset with empty CSV (just header)
    let result = engine
        .create_dataset(
            "Empty",
            Some("empty"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "col1,col2".to_string(), // header only, no data rows
                },
            },
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No data") || err.contains("empty"),
        "Error should mention no data: {}",
        err
    );
}

// ============================================================================
// Format case-insensitivity tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_format_case_insensitive_csv() {
    let (engine, _temp) = create_test_engine().await;

    // Test uppercase CSV
    let dataset = engine
        .create_dataset(
            "Upper CSV",
            Some("upper_csv"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "CSV".to_string(), // uppercase
                    content: "a,b\n1,2".to_string(),
                },
            },
        )
        .await
        .unwrap();

    assert_eq!(dataset.table_name, "upper_csv");

    let result = engine
        .execute_query("SELECT * FROM datasets.default.upper_csv")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_format_case_insensitive_json() {
    let (engine, _temp) = create_test_engine().await;

    // Test mixed case JSON
    let dataset = engine
        .create_dataset(
            "Mixed JSON",
            Some("mixed_json"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "Json".to_string(), // mixed case
                    content: r#"{"x": 1}"#.to_string(),
                },
            },
        )
        .await
        .unwrap();

    assert_eq!(dataset.table_name, "mixed_json");

    let result = engine
        .execute_query("SELECT * FROM datasets.default.mixed_json")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_format_case_insensitive_parquet_upload() {
    let (engine, _temp) = create_test_engine().await;

    // Create a simple parquet file in memory using arrow
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![42]))]).unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let upload = engine
        .store_upload(buf, Some("application/octet-stream".to_string()))
        .await
        .unwrap();

    // Test uppercase PARQUET format
    let dataset = engine
        .create_dataset(
            "Upper Parquet",
            Some("upper_parquet"),
            DatasetSource::Upload {
                upload_id: upload.id,
                format: Some("PARQUET".to_string()), // uppercase
            },
        )
        .await
        .unwrap();

    assert_eq!(dataset.table_name, "upper_parquet");

    let result = engine
        .execute_query("SELECT * FROM datasets.default.upper_parquet")
        .await;
    assert!(result.is_ok());
}

// ============================================================================
// Label validation tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_label_rejected() {
    let (engine, _temp) = create_test_engine().await;

    let result = engine
        .create_dataset(
            "",
            Some("empty_label"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "a,b\n1,2".to_string(),
                },
            },
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("empty") || err.contains("label"),
        "Error should mention empty label: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_whitespace_only_label_rejected() {
    let (engine, _temp) = create_test_engine().await;

    let result = engine
        .create_dataset(
            "   ",
            Some("whitespace_label"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "a,b\n1,2".to_string(),
                },
            },
        )
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("empty") || err.contains("label"),
        "Error should mention empty label: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_with_empty_label_rejected() {
    let (engine, _temp) = create_test_engine().await;

    // First create a valid dataset
    let dataset = engine
        .create_dataset(
            "Valid Label",
            Some("update_label_test"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "a,b\n1,2".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Try to update with empty label
    let result = engine.update_dataset(&dataset.id, Some(""), None).await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("empty") || err.contains("label"),
        "Error should mention empty label: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_with_whitespace_label_rejected() {
    let (engine, _temp) = create_test_engine().await;

    let dataset = engine
        .create_dataset(
            "Valid Label",
            Some("update_ws_label_test"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "a,b\n1,2".to_string(),
                },
            },
        )
        .await
        .unwrap();

    // Try to update with whitespace-only label
    let result = engine
        .update_dataset(&dataset.id, Some("   \t\n  "), None)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("empty") || err.contains("label"),
        "Error should mention empty label: {}",
        err
    );
}

// ============================================================================
// Concurrency / unique constraint tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_create_same_table_name() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let (engine, _temp) = create_test_engine().await;
    let engine = Arc::new(engine);
    let barrier = Arc::new(Barrier::new(2));

    // Spawn two tasks that try to create datasets with the same table_name concurrently
    let engine1 = engine.clone();
    let barrier1 = barrier.clone();
    let task1 = tokio::spawn(async move {
        barrier1.wait().await;
        engine1
            .create_dataset(
                "Dataset One",
                Some("concurrent_table"),
                DatasetSource::Inline {
                    inline: InlineData {
                        format: "csv".to_string(),
                        content: "x,y\n1,2".to_string(),
                    },
                },
            )
            .await
    });

    let engine2 = engine.clone();
    let barrier2 = barrier.clone();
    let task2 = tokio::spawn(async move {
        barrier2.wait().await;
        engine2
            .create_dataset(
                "Dataset Two",
                Some("concurrent_table"),
                DatasetSource::Inline {
                    inline: InlineData {
                        format: "csv".to_string(),
                        content: "a,b\n3,4".to_string(),
                    },
                },
            )
            .await
    });

    let (result1, result2) = tokio::join!(task1, task2);
    let result1 = result1.unwrap();
    let result2 = result2.unwrap();

    // Exactly one should succeed, one should fail with TableNameInUse
    let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();
    let failure_count = [&result1, &result2].iter().filter(|r| r.is_err()).count();

    assert_eq!(success_count, 1, "Exactly one create should succeed");
    assert_eq!(failure_count, 1, "Exactly one create should fail");

    // The failure should indicate table name conflict
    let failed = if result1.is_err() { result1 } else { result2 };
    let err = failed.unwrap_err().to_string();
    assert!(
        err.contains("in use") || err.contains("already"),
        "Error should indicate table name conflict: {}",
        err
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_update_to_same_table_name() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let (engine, _temp) = create_test_engine().await;

    // Create two datasets with different table names
    let dataset1 = engine
        .create_dataset(
            "Dataset A",
            Some("table_a"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "x\n1".to_string(),
                },
            },
        )
        .await
        .unwrap();

    let dataset2 = engine
        .create_dataset(
            "Dataset B",
            Some("table_b"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: "y\n2".to_string(),
                },
            },
        )
        .await
        .unwrap();

    let engine = Arc::new(engine);
    let barrier = Arc::new(Barrier::new(2));

    // Concurrently try to update both datasets to use the same table_name
    let engine1 = engine.clone();
    let barrier1 = barrier.clone();
    let id1 = dataset1.id.clone();
    let task1 = tokio::spawn(async move {
        barrier1.wait().await;
        engine1
            .update_dataset(&id1, None, Some("target_table"))
            .await
    });

    let engine2 = engine.clone();
    let barrier2 = barrier.clone();
    let id2 = dataset2.id.clone();
    let task2 = tokio::spawn(async move {
        barrier2.wait().await;
        engine2
            .update_dataset(&id2, None, Some("target_table"))
            .await
    });

    let (result1, result2) = tokio::join!(task1, task2);
    let result1 = result1.unwrap();
    let result2 = result2.unwrap();

    // Exactly one should succeed, one should fail
    let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();
    let failure_count = [&result1, &result2].iter().filter(|r| r.is_err()).count();

    assert_eq!(success_count, 1, "Exactly one update should succeed");
    assert_eq!(failure_count, 1, "Exactly one update should fail");

    // The failure should indicate table name conflict
    let failed = if result1.is_err() { result1 } else { result2 };
    let err = failed.unwrap_err().to_string();
    assert!(
        err.contains("in use") || err.contains("already"),
        "Error should indicate table name conflict: {}",
        err
    );
}

/// Test that corrupted arrow_schema_json falls back to parquet schema inference.
#[tokio::test(flavor = "multi_thread")]
async fn test_corrupted_schema_falls_back_to_parquet_inference() {
    use sqlx::SqlitePool;

    let temp_dir = TempDir::new().unwrap();

    // Create engine with known path so we can access the catalog database
    let db_path = temp_dir.path().join("catalog.db");
    let engine = RuntimeEngine::builder()
        .base_dir(temp_dir.path().to_path_buf())
        .secret_key(test_secret_key())
        .build()
        .await
        .expect("Failed to create engine");

    // Create a dataset with valid data
    let csv_data = "name,value\nAlice,100\nBob,200";
    let dataset = engine
        .create_dataset(
            "Test Dataset",
            Some("test_table"),
            DatasetSource::Inline {
                inline: InlineData {
                    format: "csv".to_string(),
                    content: csv_data.to_string(),
                },
            },
        )
        .await
        .expect("Failed to create dataset");

    // Verify the dataset works before corruption
    let result = engine
        .execute_query("SELECT name, value FROM datasets.default.test_table ORDER BY name")
        .await
        .expect("Query should succeed before corruption");

    assert_eq!(result.results[0].num_rows(), 2);

    // Now corrupt the schema in the database
    let db_uri = format!("sqlite:{}?mode=rw", db_path.display());
    let pool = SqlitePool::connect(&db_uri).await.unwrap();

    sqlx::query("UPDATE datasets SET arrow_schema_json = 'not valid json' WHERE id = ?")
        .bind(&dataset.id)
        .execute(&pool)
        .await
        .expect("Failed to corrupt schema");

    pool.close().await;

    // Invalidate the cache so we force a re-read
    engine.invalidate_dataset_cache("test_table");

    // Query should still work due to fallback to parquet schema inference
    let result = engine
        .execute_query("SELECT name, value FROM datasets.default.test_table ORDER BY name")
        .await
        .expect("Query should succeed with fallback schema inference");

    // Verify we got the correct data
    assert_eq!(result.results[0].num_rows(), 2);
    let batch = &result.results[0];
    let name_col = batch.column_by_name("name").unwrap();
    let value_col = batch.column_by_name("value").unwrap();

    assert_eq!(get_string_value(name_col.as_ref(), 0), "Alice");
    assert_eq!(get_string_value(name_col.as_ref(), 1), "Bob");
    assert_eq!(get_i64_value(value_col.as_ref(), 0), 100);
    assert_eq!(get_i64_value(value_col.as_ref(), 1), 200);
}
