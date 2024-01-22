use crate::delta::{DeltaJobs, DeltaManager, DeltaWrite};
use crate::models::{JobRequest, JobResponse, JobTable};
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store;
use datafusion::prelude::*;
use deltalake::{table::builder::DeltaTableBuilder, DeltaOps};
use std::sync::Arc;
use url::Url;
use yummy_core::common::Result;

#[async_trait]
impl DeltaJobs for DeltaManager {
    async fn job(&self, job_request: JobRequest) -> Result<JobResponse> {
        let store = &self.store(&job_request.source.store)?;
        let store_path = &store.path;

        let mut builder = DeltaTableBuilder::from_uri(store_path);
        if let Some(storage_options) = &store.storage_options {
            builder = builder.with_storage_options(storage_options.clone());
        }

        let ops: DeltaOps = builder.build()?.into();
        let os = ops.0.object_store();
        let url = Url::parse(store_path)?;

        let ctx = SessionContext::new();

        ctx.runtime_env()
            .register_object_store(&url, os.storage_backend());

        if let Some(udfs) = &self.udfs {
            for udf in udfs {
                ctx.register_udf(udf.clone());
            }
        }

        for table in job_request.source.tables {
            match table {
                JobTable::Parquet { name, path } => {
                    ctx.register_parquet(&name, &path, ParquetReadOptions::default())
                        .await?;
                }
                JobTable::Csv { name, path } => {
                    ctx.register_csv(&name, &path, CsvReadOptions::default())
                        .await?;
                }
                JobTable::Json { name, path } => {
                    ctx.register_json(&name, &path, NdJsonReadOptions::default())
                        .await?;
                }
                JobTable::Delta { name, table } => {
                    let delta_table = self
                        .table(&job_request.source.store, &table, None, None)
                        .await?;
                    ctx.register_table(name.as_str(), Arc::new(delta_table))?;
                }
            }
        }

        let df = ctx.sql(&job_request.sql).await?;
        let dry_run = if let Some(dry) = &job_request.dry_run {
            *dry
        } else {
            false
        };

        self.print_schema(&df);

        if dry_run {
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
