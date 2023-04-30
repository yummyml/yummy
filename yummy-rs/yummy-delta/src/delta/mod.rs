pub mod commands;
pub mod error;
pub mod info;
pub mod jobs;
pub mod read;
pub mod write;
use crate::config::{DeltaConfig, DeltaStoreConfig};
use crate::delta::error::DeltaError;
use crate::models::{
    CreateRequest, CreateResponse, JobRequest, JobResponse, OptimizeRequest, OptimizeResponse,
    QueryResponse, ResponseStore, ResponseTable, ResponseTables, VacuumRequest, VacuumResponse,
    WriteRequest, WriteResponse,
};
use async_trait::async_trait;
use chrono::Duration;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::arrow::{datatypes::DataType, record_batch::RecordBatch};
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::{
    action::SaveMode, builder::DeltaTableBuilder, DeltaOps, DeltaTable, Schema, SchemaDataType,
};
use prettytable::{row, Table};
use std::error::Error;

#[async_trait]
pub trait DeltaJobs {
    async fn job(&self, job_request: JobRequest) -> Result<JobResponse, Box<dyn Error>>;
}

#[async_trait]
pub trait DeltaInfo {
    fn list_stores(&self) -> Result<Vec<ResponseStore>, Box<dyn Error>>;

    async fn details(
        &self,
        store_name: &str,
        table_name: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<ResponseTable, Box<dyn Error>>;

    async fn list_tables(&self, store_name: &str) -> Result<ResponseTables, Box<dyn Error>>;
}

#[async_trait]
pub trait DeltaCommands {
    async fn create(
        &self,
        store_name: &str,
        create_request: CreateRequest,
    ) -> Result<CreateResponse, Box<dyn Error>>;

    async fn optimize(
        &self,
        store_name: &str,
        table_name: &str,
        optimize_requst: OptimizeRequest,
    ) -> Result<OptimizeResponse, Box<dyn Error>>;
}

#[async_trait]
pub trait DeltaRead {
    async fn query_stream(
        &self,
        store_name: &str,
        table_name: &str,
        query: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<SendableRecordBatchStream, Box<dyn Error>>;

    async fn query_stream_partitioned(
        &self,
        store_name: &str,
        table_name: &str,
        query: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<Vec<SendableRecordBatchStream>, Box<dyn Error>>;

    async fn query(
        &self,
        store_name: &str,
        table_name: &str,
        query: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<QueryResponse, Box<dyn Error>>;
}

#[async_trait]
pub trait DeltaWrite {
    async fn write_batches(
        &self,
        store_name: &str,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
        save_mode: SaveMode,
    ) -> Result<WriteResponse, Box<dyn Error>>;

    async fn write(
        &self,
        store_name: &str,
        table_name: &str,
        write_request: WriteRequest,
        save_mode: SaveMode,
    ) -> Result<WriteResponse, Box<dyn Error>>;
}

#[derive(Debug, Clone)]
pub struct DeltaManager {
    pub config: DeltaConfig,
    pub udfs: Option<Vec<ScalarUDF>>,
}

impl DeltaManager {
    pub async fn new(
        config_path: String,
        udfs: Option<Vec<ScalarUDF>>,
    ) -> Result<Self, Box<dyn Error>> {
        let config = DeltaConfig::new(&config_path).await?;
        Ok(DeltaManager { config, udfs })
    }

    fn store(&self, store_name: &str) -> Result<&DeltaStoreConfig, Box<dyn Error>> {
        let store = self
            .config
            .stores
            .iter()
            .filter(|s| s.name == store_name)
            .last()
            .ok_or(Box::new(DeltaError::StoreNotExists))?;

        Ok(store)
    }

    fn path(&self, store_path: &str, table_name: &str) -> Result<String, Box<dyn Error>> {
        let path = if store_path.ends_with('/') {
            format!("{}{}", &store_path, table_name)
        } else {
            format!("{}/{}", &store_path, table_name)
        };
        Ok(path)
    }

    pub async fn table(
        &self,
        store_name: &str,
        table_name: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<DeltaTable, Box<dyn Error>> {
        let store = self.store(store_name)?;
        let table = self
            .table_from_store(store, table_name, table_version, table_date)
            .await?;
        Ok(table)
    }

    async fn table_from_store(
        &self,
        store: &DeltaStoreConfig,
        table_name: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<DeltaTable, Box<dyn Error>> {
        let table_uri = self.path(&store.path, table_name)?;

        let mut builder = DeltaTableBuilder::from_uri(table_uri);

        if let Some(ver) = table_version {
            builder = builder.with_version(ver);
        } else if let Some(ds) = table_date {
            builder = builder.with_datestring(ds)?;
        }

        if let Some(storage_options) = &store.storage_options {
            builder = builder.with_storage_options(storage_options.clone());
        }

        let table = builder.load().await?;

        Ok(table)
    }

    #[allow(dead_code)]
    async fn schema(
        &self,
        store_name: &str,
        table_name: &str,
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
        store_name: &str,
        table_name: &str,
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
        store_name: &str,
        table_name: &str,
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

    fn print_schema(&self, df: &datafusion::dataframe::DataFrame) {
        //println!("{:#?}", &df.schema());
        let mut tbl = Table::new();

        tbl.set_titles(row!["column_name", "arrow_type", "delta_type", "nullable"]);
        for field in df.schema().fields() {
            let f = field.field();
            let delta_type = match &f.data_type() {
                DataType::Utf8 => "string",
                DataType::Int64 => "long",
                DataType::Int32 => "integer",
                DataType::Int16 => "short",
                DataType::Int8 => "byte",
                DataType::Float32 => "float",
                DataType::Float64 => "double",
                DataType::Boolean => "boolean",
                DataType::Binary => "binary",
                DataType::Decimal128(_x, _p) => "decimal",
                DataType::Date32 => "date",
                DataType::Timestamp(_x, _u) => "timestamp",
                _ => "unknown",
            };
            tbl.add_row(row![
                &f.name(),
                &f.data_type(),
                delta_type,
                &f.is_nullable()
            ]);
        }

        tbl.printstd();
    }
}

#[cfg(test)]
pub mod test_delta_util {
    use crate::config::ColumnSchema;
    use crate::delta::{DeltaCommands, DeltaManager};
    use crate::models::{CreateRequest, SchemaPrimitiveType};
    use deltalake::arrow::datatypes::DataType;
    use std::error::Error;
    use std::fs;

    #[tokio::test]
    async fn test_map_data_type() -> Result<(), Box<dyn Error>> {
        let data_type: DataType = SchemaPrimitiveType::String.try_into()?;

        assert_eq!(data_type, DataType::Utf8);
        Ok(())
    }

    pub async fn create_manager() -> Result<DeltaManager, Box<dyn Error>> {
        let path = "../tests/delta/config.yaml".to_string();
        Ok(DeltaManager::new(path, None).await?)
    }

    pub async fn create_delta(
        store_name: &String,
        table_name: &String,
    ) -> Result<deltalake::DeltaTable, Box<dyn Error>> {
        let path = "../tests/delta/config.yaml".to_string();
        let delta_manager = DeltaManager::new(path, None).await?;

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
        delta_manager
            .table(&store_name, &table_name, None, None)
            .await
    }

    pub async fn drop_delta(table_name: &String) -> Result<(), Box<dyn Error>> {
        fs::remove_dir_all(format!("/tmp/delta-test-1/{table_name}"))?;
        Ok(())
    }
}
