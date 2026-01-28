use anyhow::Result;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata for a discovered table
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub catalog_name: Option<String>,
    pub schema_name: String,
    pub table_name: String,
    pub table_type: String,
    pub columns: Vec<ColumnMetadata>,
    /// Geometry column metadata for GeoParquet support
    pub geometry_columns: HashMap<String, GeometryColumnInfo>,
}

/// Metadata for a table column
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: ArrowDataType,
    pub nullable: bool,
    pub ordinal_position: i32,
}

/// Geometry column information for GeoParquet metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeometryColumnInfo {
    /// The SRID (Spatial Reference System Identifier) for this geometry column.
    /// Common values: 4326 (WGS84), 3857 (Web Mercator), 0 (unspecified)
    pub srid: i32,
    /// The geometry type if known (e.g., "Point", "Polygon", "Geometry")
    pub geometry_type: Option<String>,
}

impl Default for TableMetadata {
    fn default() -> Self {
        Self {
            catalog_name: None,
            schema_name: String::new(),
            table_name: String::new(),
            table_type: String::new(),
            columns: Vec::new(),
            geometry_columns: HashMap::new(),
        }
    }
}

/// Key used to store geometry column metadata in Arrow schema metadata
pub const GEOMETRY_COLUMNS_METADATA_KEY: &str = "runtimedb:geometry_columns";

impl TableMetadata {
    /// Convert column metadata to an Arrow Schema
    /// Includes geometry column metadata in schema metadata if present
    pub fn to_arrow_schema(&self) -> Arc<Schema> {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|col| Field::new(&col.name, col.data_type.clone(), col.nullable))
            .collect();

        let mut schema = Schema::new(fields);

        // Store geometry column info in schema metadata
        if !self.geometry_columns.is_empty() {
            if let Ok(json) = serde_json::to_string(&self.geometry_columns) {
                let mut metadata = HashMap::new();
                metadata.insert(GEOMETRY_COLUMNS_METADATA_KEY.to_string(), json);
                schema = schema.with_metadata(metadata);
            }
        }

        Arc::new(schema)
    }
}

/// Extract geometry column info from Arrow schema metadata
pub fn extract_geometry_columns(schema: &Schema) -> HashMap<String, GeometryColumnInfo> {
    schema
        .metadata()
        .get(GEOMETRY_COLUMNS_METADATA_KEY)
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default()
}

/// Normalize geometry type names to GeoParquet standard capitalization.
///
/// Converts case-insensitive input ("polygon", "MULTIPOINT") to the standard
/// mixed-case form ("Polygon", "MultiPoint"). Unknown types are passed through as-is.
pub fn normalize_geometry_type(geom_type: &str) -> String {
    match geom_type.to_uppercase().as_str() {
        "POINT" => "Point",
        "LINESTRING" => "LineString",
        "POLYGON" => "Polygon",
        "MULTIPOINT" => "MultiPoint",
        "MULTILINESTRING" => "MultiLineString",
        "MULTIPOLYGON" => "MultiPolygon",
        "GEOMETRYCOLLECTION" => "GeometryCollection",
        "GEOMETRY" => "Geometry",
        _ => geom_type,
    }
    .to_string()
}

/// Deserialize an Arrow Schema from JSON string
pub fn deserialize_arrow_schema(json: &str) -> Result<Arc<Schema>> {
    let schema: Schema = serde_json::from_str(json)?;
    Ok(Arc::new(schema))
}
