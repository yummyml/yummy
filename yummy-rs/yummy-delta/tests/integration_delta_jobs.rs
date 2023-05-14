mod common;
use common::delta::{create_delta, create_manager, drop_delta};
use yummy_delta::delta::DeltaJobs;
use yummy_delta::models::{JobRequest, JobResponse, JobSink, JobSource, JobTable};
use deltalake::action::SaveMode;
use std::error::Error;
use std::fs;
use yummy_core::common::Result;
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
