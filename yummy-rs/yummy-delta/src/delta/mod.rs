pub mod commands;
pub mod error;
pub mod info;
pub mod read;
pub mod write;
use crate::config::{DeltaConfig, DeltaStoreConfig};
use crate::delta::error::DeltaError;
use crate::models::{
    CreateRequest, CreateResponse, OptimizeRequest, OptimizeResponse, QueryResponse, ResponseStore,
    ResponseTable, ResponseTables, VacuumRequest, VacuumResponse, WriteRequest, WriteResponse,
};
use async_trait::async_trait;
use chrono::Duration;
use datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::arrow::{datatypes::DataType, record_batch::RecordBatch};
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::{action::SaveMode, DeltaOps, DeltaTable, Schema, SchemaDataType};
use std::error::Error;

#[async_trait]
pub trait DeltaInfo {
    fn list_stores(&self) -> Result<Vec<ResponseStore>, Box<dyn Error>>;

    async fn details(
        &self,
        store_name: &String,
        table_name: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<ResponseTable, Box<dyn Error>>;

    async fn list_tables(&self, store_name: &String) -> Result<ResponseTables, Box<dyn Error>>;
}

#[async_trait]
pub trait DeltaCommands {
    async fn create(
        &self,
        store_name: &String,
        create_request: CreateRequest,
    ) -> Result<CreateResponse, Box<dyn Error>>;

    async fn optimize(
        &self,
        store_name: &String,
        table_name: &String,
        optimize_requst: OptimizeRequest,
    ) -> Result<OptimizeResponse, Box<dyn Error>>;
}

#[async_trait]
pub trait DeltaRead {
    async fn query_stream(
        &self,
        store_name: &String,
        table_name: &String,
        query: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<SendableRecordBatchStream, Box<dyn Error>>;

    async fn query_stream_partitioned(
        &self,
        store_name: &String,
        table_name: &String,
        query: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<Vec<SendableRecordBatchStream>, Box<dyn Error>>;

    async fn query(
        &self,
        store_name: &String,
        table_name: &String,
        query: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<QueryResponse, Box<dyn Error>>;
}

#[async_trait]
pub trait DeltaWrite {
    async fn write_batches(&self,
        store_name: &String,
        table_name: &String,
        record_batches: Vec<RecordBatch>,
        save_mode: SaveMode,
    ) -> Result<WriteResponse, Box<dyn Error>>;

    async fn write(
        &self,
        store_name: &String,
        table_name: &String,
        write_request: WriteRequest,
        save_mode: SaveMode,
    ) -> Result<WriteResponse, Box<dyn Error>>;
}

pub struct DeltaManager {
    pub config: DeltaConfig,
}

impl DeltaManager {
    pub fn new(config_path: String) -> Result<Self, Box<dyn Error>> {
        let config = DeltaConfig::new(&config_path)?;
        Ok(DeltaManager { config })
    }

    fn store(&self, store_name: &String) -> Result<&DeltaStoreConfig, Box<dyn Error>> {
        let store = self
            .config
            .stores
            .iter()
            .filter(|s| &s.name == store_name)
            .last()
            .ok_or(Box::new(DeltaError::StoreNotExists))?;

        Ok(store)
    }

    fn path(&self, store_name: &String, table_name: &String) -> Result<String, Box<dyn Error>> {
        let store = self.store(store_name)?.path.to_string();
        let path = if store.ends_with("/") {
            format!("{}{}", &store, table_name)
        } else {
            format!("{}/{}", &store, table_name)
        };
        Ok(path)
    }

    pub async fn table(
        &self,
        store_name: &String,
        table_name: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<DeltaTable, Box<dyn Error>> {
        let path = self.path(&store_name, &table_name)?;
        let table = if let Some(ver) = table_version {
            deltalake::open_table_with_version(path, ver).await?
        } else if let Some(ds) = table_date {
            deltalake::open_table_with_ds(path, ds).await?
        } else {
            deltalake::open_table(path).await?
        };
        Ok(table)
    }

    #[allow(dead_code)]
    async fn schema(
        &self,
        store_name: &String,
        table_name: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<Schema, Box<dyn Error>> {
        let table = self
            .table(store_name, table_name, table_version, table_date)
            .await?;
        let schema = table.schema().ok_or(Box::new(DeltaError::InvalidSchema))?;
        Ok(schema.clone())
    }

    pub fn map_primitive(&self, schema_type: &SchemaDataType) -> Result<DataType, Box<dyn Error>> {
        let primitive_type = match schema_type {
            SchemaDataType::primitive(type_name) => Ok(type_name),
            _ => Err(Box::new(DeltaError::TypeConversionError)),
        }?;

        let data_type = match primitive_type.as_str() {
            "string" => Ok(DataType::Utf8),
            "long" => Ok(DataType::Int64),
            "integer" => Ok(DataType::Int32),
            "short" => Ok(DataType::Int16),
            "byte" => Ok(DataType::Int8),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "boolean" => Ok(DataType::Boolean),
            "binary" => Ok(DataType::Binary),
            "date" => Ok(DataType::Date32),
            "timestamp" => Ok(DataType::Date64),
            _ => Err(Box::new(DeltaError::UnknownTypeError)),
        }?;

        Ok(data_type)
    }

    pub async fn update(
        &self,
        store_name: &String,
        table_name: &String,
        batch: RecordBatch,
        predicate: impl Into<String>,
    ) -> Result<(), Box<dyn Error>> {
        let table = self.table(store_name, table_name, None, None).await?;
        let table1 = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where(predicate)
            .await?;

        println!("Version: {}", table1.version());

        Ok(())
    }

    pub async fn vacuum(
        &self,
        store_name: &String,
        table_name: &String,
        vacuum_request: VacuumRequest,
    ) -> Result<VacuumResponse, Box<dyn Error>> {
        let table = self.table(store_name, table_name, None, None).await?;

        let mut vacuum_op = VacuumBuilder::new(table.object_store(), table.state.clone());

        if let Some(retention_period_seconds) = vacuum_request.retention_period_seconds {
            vacuum_op =
                vacuum_op.with_retention_period(Duration::seconds(retention_period_seconds));
        }

        if let Some(dry_run) = vacuum_request.dry_run {
            vacuum_op = vacuum_op.with_dry_run(dry_run);
        }

        if let Some(enforce_retention_duration) = vacuum_request.enforce_retention_duration {
            vacuum_op = vacuum_op.with_enforce_retention_duration(enforce_retention_duration);
        }

        let (_table, result) = vacuum_op.await?;

        Ok(VacuumResponse {
            dry_run: result.dry_run,
            files_deleted: result.files_deleted,
        })
    }
}

#[cfg(test)]
pub mod test_delta_util {
    use crate::config::ColumnSchema;
    use crate::delta::{DeltaCommands, DeltaManager};
    use crate::models::CreateRequest;
    use std::error::Error;
    use std::fs;

    pub async fn create_manager() -> Result<DeltaManager, Box<dyn Error>> {
        let path = "../tests/delta/config.yaml".to_string();
        Ok(DeltaManager::new(path)?)
    }

    pub async fn create_delta(
        store_name: &String,
        table_name: &String,
    ) -> Result<deltalake::DeltaTable, Box<dyn Error>> {
        let path = "../tests/delta/config.yaml".to_string();
        let delta_manager = DeltaManager::new(path)?;

        //let store_name = String::from("az");
        let mut schema: Vec<ColumnSchema> = Vec::new();
        schema.push(ColumnSchema {
            name: String::from("col1"),
            r#type: String::from("integer"),
            nullable: false,
        });
        schema.push(ColumnSchema {
            name: String::from("col2"),
            r#type: String::from("string"),
            nullable: false,
        });

        let partition_columns = vec![String::from("col2")];

        let comment = String::from("Hello from delta");

        let request = CreateRequest {
            table: table_name.to_string(),
            schema,
            partition_columns: Some(partition_columns),
            comment: Some(comment),
            configuration: None,
            metadata: None,
        };

        let _res_create = delta_manager.create(&store_name, request).await?;
        Ok(delta_manager
            .table(&store_name, &table_name, None, None)
            .await?)
    }

    pub async fn drop_delta(table_name: &String) -> Result<(), Box<dyn Error>> {
        fs::remove_dir_all(format!("/tmp/delta-test-1/{}", table_name))?;
        Ok(())
    }
}
