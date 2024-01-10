use crate::config::ColumnSchema;
use datafusion_expr::Volatility as ArrowVolatility;
use deltalake::arrow::datatypes::DataType;
use deltalake::Schema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use yummy_core::common::EntityValue;

#[derive(Serialize)]
pub struct ResponseStores {
    pub stores: Vec<ResponseStore>,
}

#[derive(Serialize)]
pub struct ResponseStore {
    pub store: String,
    pub path: String,
}

#[derive(Serialize)]
pub struct ResponseTables {
    pub store: String,
    pub path: String,
    pub tables: Vec<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ResponseTable {
    pub store: String,
    pub path: String,
    pub table: String,
    pub schema: Schema,
    pub version: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub struct CreateRequest {
    pub table: String,
    pub schema: Vec<ColumnSchema>,
    pub partition_columns: Option<Vec<String>>,
    pub comment: Option<String>,
    pub configuration: Option<HashMap<String, Option<String>>>,
    pub metadata: Option<Map<String, Value>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateResponse {
    pub table: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WriteRequest {
    pub record_batch_dict: Option<HashMap<String, Vec<EntityValue>>>,
    pub record_batch_list: Option<Vec<HashMap<String, EntityValue>>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteResponse {
    pub table: String,
    pub version: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailsQuery {
    pub table_version: Option<i64>,
    pub table_date: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    pub query: String,
    pub braces: Option<bool>,
    pub table_version: Option<i64>,
    pub table_date: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryBatch {
    pub columns: Option<HashMap<String, Vec<EntityValue>>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResponse {
    pub batches: Vec<BTreeMap<String, Vec<EntityValue>>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PartitionFilter {
    pub column: String,
    pub operator: String,
    pub value: EntityValue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OptimizeRequest {
    pub target_size: i64,
    pub filters: Option<Vec<PartitionFilter>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OptimizeResponse {
    pub metrics: deltalake::operations::optimize::Metrics,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub struct VacuumRequest {
    pub retention_period_seconds: Option<i64>,
    pub enforce_retention_duration: Option<bool>,
    pub dry_run: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
pub struct VacuumResponse {
    pub dry_run: bool,
    pub files_deleted: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum JobTable {
    Parquet { name: String, path: String },
    Csv { name: String, path: String },
    Json { name: String, path: String },
    Delta { name: String, table: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobSource {
    pub store: String,
    pub tables: Vec<JobTable>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobSink {
    pub name: String,
    pub store: String,
    pub table: String,
    pub save_mode: deltalake::action::SaveMode,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JobRequest {
    pub source: JobSource,
    pub sql: String,
    pub sink: JobSink,
    pub dry_run: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JobResponse {
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UdfSpec {
    pub name: String,
    pub input_types: Vec<SchemaPrimitiveType>,
    pub return_type: SchemaPrimitiveType,
    pub volatility: Volatility,
    pub config: UdfConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UdfConfig {
    Dummy,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

impl TryFrom<Volatility> for ArrowVolatility {
    type Error = Box<dyn Error>;

    fn try_from(volatility: Volatility) -> Result<Self, Box<dyn Error>> {
        match volatility {
            Volatility::Immutable => Ok(ArrowVolatility::Immutable),
            Volatility::Stable => Ok(ArrowVolatility::Stable),
            Volatility::Volatile => Ok(ArrowVolatility::Volatile),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SchemaPrimitiveType {
    String,
    Long,
    Integer,
    Short,
    Byte,
    Float,
    Double,
    Boolean,
    Binary,
    Date,
    Timestamp,
}

impl TryFrom<SchemaPrimitiveType> for DataType {
    type Error = Box<dyn Error>;

    fn try_from(primitive_type: SchemaPrimitiveType) -> Result<Self, Box<dyn Error>> {
        match primitive_type {
            SchemaPrimitiveType::String => Ok(DataType::Utf8),
            SchemaPrimitiveType::Long => Ok(DataType::Int64),
            SchemaPrimitiveType::Integer => Ok(DataType::Int32),
            SchemaPrimitiveType::Short => Ok(DataType::Int16),
            SchemaPrimitiveType::Byte => Ok(DataType::Int8),
            SchemaPrimitiveType::Float => Ok(DataType::Float32),
            SchemaPrimitiveType::Double => Ok(DataType::Float64),
            SchemaPrimitiveType::Boolean => Ok(DataType::Boolean),
            SchemaPrimitiveType::Binary => Ok(DataType::Binary),
            SchemaPrimitiveType::Date => Ok(DataType::Date32),
            SchemaPrimitiveType::Timestamp => Ok(DataType::Date64),
        }
    }
}
