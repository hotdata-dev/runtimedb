use anyhow::Result;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use std::sync::Arc;

/// Metadata for a discovered table
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub catalog_name: Option<String>,
    pub schema_name: String,
    pub table_name: String,
    pub table_type: String,
    pub columns: Vec<ColumnMetadata>,
}

/// Metadata for a table column
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: ArrowDataType,
    pub nullable: bool,
    pub ordinal_position: i16,
}

impl TableMetadata {
    /// Convert column metadata to an Arrow Schema
    pub fn to_arrow_schema(&self) -> Arc<Schema> {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.clone(), col.nullable))
            .collect();
        Arc::new(Schema::new(fields))
    }
}

/// Deserialize an Arrow Schema from JSON string
pub fn deserialize_arrow_schema(json: &str) -> Result<Arc<Schema>> {
    let schema: Schema = serde_json::from_str(json)?;
    Ok(Arc::new(schema))
}