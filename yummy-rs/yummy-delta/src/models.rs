use crate::common::EntityValue;
use crate::config::ColumnSchema;
use deltalake::Schema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};

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
pub struct DetailsQuery {
    pub table_version: Option<i64>,
    pub table_date: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
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
pub struct OptimizeRequest {
    pub target_size: i64,
    pub filters: Option<Vec<PartitionFilter>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OptimizeResponse {
    pub metrics: deltalake::operations::optimize::Metrics,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VacuumRequest {
    pub retention_period_seconds: Option<i64>,
    pub enforce_retention_duration: Option<bool>,
    pub dry_run: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VacuumResponse {
    pub dry_run: bool,
    pub files_deleted: Vec<String>,
}
