use crate::common::Result;
use crate::delta::{DeltaJobs, DeltaManager, DeltaWrite};
use crate::models::{JobRequest, JobResponse, JobTable};
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use deltalake::delta_config::DeltaConfigKey;
use deltalake::PartitionFilter;
use deltalake::{
    action::SaveMode, builder::DeltaTableBuilder, DeltaOps, SchemaDataType, SchemaField,
};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use url::Url;

#[async_trait]
impl DeltaJobs for DeltaManager {
    async fn job(&self, job_request: JobRequest) -> Result<JobResponse> {
        let store = &self.store(&job_request.source.store)?;
        let store_path = &store.path;

        let mut builder = DeltaTableBuilder::from_uri(&store_path);
        if let Some(storage_options) = &store.storage_options {
            builder = builder.with_storage_options(storage_options.clone());
        }

        let ops: DeltaOps = builder.build()?.into();
        let os = ops.0.object_store();
        let url = Url::parse(&store_path)?;

        let ctx = SessionContext::new();

        ctx.runtime_env().register_object_store(
            url.scheme(),
            url.host_str().unwrap_or_default(),
            os.storage_backend(),
        );

        for table in job_request.source.tables {
            match table {
                JobTable::Parquet { name, path } => {
                    ctx.register_parquet(&name, &path, ParquetReadOptions::default())
                        .await?;
                }
                JobTable::Csv { name, path } => {}
                JobTable::Json { name, path } => {}
                JobTable::Delta { name, table } => {}
            }
        }

        let df = ctx.sql(&job_request.sql).await?;
        let dry_run = if let Some(dry) = &job_request.dry_run {
            dry.clone()
        } else {
            false
        };

        if dry_run {
            println!("{:#?}", &df.schema());
            df.show_limit(10).await?;
        } else {
            let rb = df.collect().await?;
            self.write_batches(
                &job_request.sink.store,
                &job_request.sink.table,
                rb,
                job_request.sink.save_mode,
            )
            .await?;
        }

        Ok(JobResponse { success: true })
    }
}

#[cfg(test)]
mod test {
    use crate::common::Result;
    use crate::delta::test_delta_util::{create_delta, create_manager, drop_delta};
    use crate::delta::DeltaJobs;
    use crate::models::{JobRequest, JobResponse, JobSink, JobSource, JobTable};
    use deltalake::action::SaveMode;
    use std::error::Error;
    use std::fs;
    /*
        #[tokio::test]
        async fn test_delta_job_run() -> Result<()> {
            let mut tables = Vec::new();
            tables.push(JobTable::Parquet {
                name: "test".to_string(),
                path: "az://test/data.parquet".to_string(),
            });

            let sink = JobSink {
                name: "sink".to_string(),
                store: "az".to_string(),
                table: "test_delta_5".to_string(),
                save_mode: SaveMode::Append,
            };

            let job = JobRequest {
                source: JobSource {
                    store: "az2".to_string(),
                    tables,
                },
                sql: "SELECT * FROM test limit 2".to_string(),
                sink,
                dry_run: Some(true),
            };

            let res = create_manager().await?.job(job).await?;

            assert_eq!(res.success, true);

            Ok(())
        }
    */
}
