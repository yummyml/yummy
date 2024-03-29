use crate::config::ColumnSchema;
use crate::delta::{DeltaCommands, DeltaManager};
use crate::models::{CreateRequest, CreateResponse, OptimizeRequest, OptimizeResponse};
use async_trait::async_trait;
use deltalake::table::config::DeltaConfigKey;
use deltalake::PartitionFilter;
use deltalake::{table::builder::DeltaTableBuilder, DeltaOps, SchemaDataType, SchemaField};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::str::FromStr;

#[async_trait]
impl DeltaCommands for DeltaManager {
    async fn create(
        &self,
        store_name: &str,
        create_request: CreateRequest,
    ) -> Result<CreateResponse, Box<dyn Error>> {
        let table_name = create_request.table;
        let store = self.store(store_name)?;
        let mut path = store.path.clone();
        let schema: Vec<ColumnSchema> = create_request.schema;
        let partition_columns: Option<Vec<String>> = create_request.partition_columns;
        let comment: Option<String> = create_request.comment;
        let configuration = create_request.configuration;
        let metadata = create_request.metadata;

        if (path.starts_with("file://") || path.starts_with('/')) && !Path::exists(Path::new(&path))
        {
            fs::create_dir_all(&path)?;
        }

        path = self.path(&store.path, &table_name)?;

        let delta_schema: Vec<SchemaField> = schema
            .iter()
            .map(|s| {
                SchemaField::new(
                    s.name.clone(),
                    SchemaDataType::primitive(s.r#type.clone()),
                    s.nullable,
                    Default::default(),
                )
            })
            .collect();

        let mut builder = DeltaTableBuilder::from_uri(&path);
        if let Some(storage_options) = &store.storage_options {
            builder = builder.with_storage_options(storage_options.clone());
        }

        let ops: DeltaOps = builder.build()?.into();

        let mut table = ops
            .create()
            .with_columns(delta_schema)
            .with_table_name(table_name.to_string());

        if let Some(comm) = comment {
            table = table.with_comment(comm);
        }

        if let Some(par_col) = partition_columns {
            table = table.with_partition_columns(par_col);
        }

        if let Some(config) = configuration {
            for (k, v) in config.into_iter() {
                table = table.with_configuration_property(DeltaConfigKey::from_str(&k).unwrap(), v);
            }
        }

        if let Some(meta) = metadata {
            table = table.with_metadata(meta);
        }

        table.await?;
        Ok(CreateResponse { table: table_name })
    }

    async fn optimize(
        &self,
        store_name: &str,
        table_name: &str,
        optimize_requst: OptimizeRequest,
    ) -> Result<OptimizeResponse, Box<dyn Error>> {
        let table = self.table(store_name, table_name, None, None).await?;

        let optimize_filters: Vec<crate::models::PartitionFilter> =
            if let Some(f) = optimize_requst.filters {
                f
            } else {
                Vec::new()
            };

        let filter_params = optimize_filters
            .iter()
            .map(|f| -> Result<(String, String, String), Box<dyn Error>> {
                let v: String = (&f.value).try_into()?;
                Ok((f.column.to_string(), f.operator.to_string(), v))
            })
            .collect::<Result<Vec<(String, String, String)>, Box<dyn Error>>>()?;

        let filters = filter_params
            .iter()
            .map(|f| -> Result<PartitionFilter, Box<dyn Error>> {
                Ok(PartitionFilter::try_from((
                    f.0.as_str(),
                    f.1.as_str(),
                    f.2.as_str(),
                ))?)
            })
            .collect::<Result<Vec<PartitionFilter>, Box<dyn Error>>>()?;

        let mut optimize = DeltaOps(table)
            .optimize()
            .with_target_size(optimize_requst.target_size);

        if !filters.is_empty() {
            optimize = optimize.with_filters(&filters);
        }

        let (_dt, metrics) = optimize.await?;

        //let commit_info = table.history(None).await?;

        Ok(OptimizeResponse { metrics })
    }
}
