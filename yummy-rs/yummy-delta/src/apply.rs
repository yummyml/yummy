use crate::common::Result;
use crate::config::DeltaConfig;
use crate::delta::{
    read::map_record_batch, DeltaCommands, DeltaInfo, DeltaJobs, DeltaManager, DeltaRead,
    DeltaWrite,
};
use crate::err;
use crate::models::{CreateRequest, JobRequest, OptimizeRequest, VacuumRequest, WriteRequest};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use deltalake::DeltaOps;
use serde::Deserialize;
use std::fs;
use url::{ParseError, Url};

#[derive(thiserror::Error, Debug)]
pub enum ApplyError {
    #[error("Delta config kind required")]
    NoConfig,
    #[error("Delta table kind must contain store in metadata")]
    WrongTableMetadata,
    #[error("Delta optimize kind must contain store and table in metadata")]
    WrongOptimizeMetadata,
    #[error("Delta vacuum kind must contain store and table in metadata")]
    WrongVacuumMetadata,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub name: String,
    pub store: Option<String>,
    pub table: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum DeltaObject {
    Config {
        metadata: Metadata,
        spec: DeltaConfig,
    },
    Table {
        metadata: Metadata,
        spec: CreateRequest,
    },
    Optimize {
        metadata: Metadata,
        spec: OptimizeRequest,
    },
    Vacuum {
        metadata: Metadata,
        spec: VacuumRequest,
    },
    Job {
        metadata: Metadata,
        spec: JobRequest,
    },
}

#[derive(Deserialize, Debug)]
pub struct DeltaApply {
    pub delta_objects: Vec<DeltaObject>,
    pub config: DeltaObject,
}

impl DeltaApply {
    pub async fn new(path: &String) -> Result<DeltaApply> {
        let s = if Url::parse(path).is_ok() {
            reqwest::get(path).await?.text().await?
        } else {
            fs::read_to_string(path)?
        };

        let mut objects = Vec::new();
        for document in serde_yaml::Deserializer::from_str(&s) {
            let o = DeltaObject::deserialize(document)?;
            objects.push(o);
        }

        let config = objects
            .clone()
            .into_iter()
            .filter(|x| match x {
                DeltaObject::Config {
                    metadata: _m,
                    spec: _s,
                } => true,
                _ => false,
            })
            .last();

        if let Some(c) = config {
            Ok(DeltaApply {
                delta_objects: objects,
                config: c,
            })
        } else {
            return Err(err!(ApplyError::NoConfig));
        }
    }

    pub async fn apply(&self) -> Result<()> {
        let conf = if let DeltaObject::Config { metadata, spec } = &self.config {
            spec.clone()
        } else {
            return Err(err!(ApplyError::NoConfig));
        };
        let delta_manager = DeltaManager { config: conf };

        for o in &self.delta_objects {
            match o {
                DeltaObject::Table { metadata, spec } => {
                    let store_name = metadata
                        .clone()
                        .store
                        .ok_or(err!(ApplyError::WrongTableMetadata))?;
                    let table_name = spec.clone().table;

                    match &delta_manager.create(&store_name, spec.clone()).await {
                        Ok(r) => {
                            println!("\x1b[92mSuccess - table created\x1b[0m");
                            println!("\x1b[92m{:#?}\x1b[0m", spec.clone());
                            println!("\x1b[92m{:#?}\x1b[0m", r);
                        }
                        Err(e) => {
                            println!("\x1b[93mSkipped - {:#?}\x1b[0m", e.source().unwrap());
                        }
                    }

                    /*
                    let table = &delta_manager
                        .details(&store_name, &table_name, None, None)
                        .await?;

                    println!("\x1b[92m{:#?}\x1b[0m", table);
                    println!("\x1b[92m{:#?}\x1b[0m", spec);
                    */
                }
                DeltaObject::Optimize { metadata, spec } => {
                    let store_name = metadata
                        .clone()
                        .store
                        .ok_or(err!(ApplyError::WrongOptimizeMetadata))?;
                    let table_name = metadata
                        .clone()
                        .table
                        .ok_or(err!(ApplyError::WrongOptimizeMetadata))?;

                    match &delta_manager
                        .optimize(&store_name, &table_name, spec.clone())
                        .await
                    {
                        Ok(r) => {
                            println!(
                                "\x1b[92mSuccess - table {:#?} optimized\x1b[0m",
                                &table_name
                            );
                            println!("\x1b[92m{:#?}\x1b[0m", spec.clone());
                            println!("\x1b[92m{:#?}\x1b[0m", r);
                        }
                        Err(e) => {
                            println!("\x1b[93mSkipped - {:#?}\x1b[0m", e.source().unwrap());
                        }
                    }
                }
                DeltaObject::Vacuum { metadata, spec } => {
                    let store_name = metadata
                        .clone()
                        .store
                        .ok_or(err!(ApplyError::WrongOptimizeMetadata))?;
                    let table_name = metadata
                        .clone()
                        .table
                        .ok_or(err!(ApplyError::WrongOptimizeMetadata))?;

                    match &delta_manager
                        .vacuum(&store_name, &table_name, spec.clone())
                        .await
                    {
                        Ok(r) => {
                            println!("\x1b[92mSuccess - table {:#?} vacuumed\x1b[0m", &table_name);
                            println!("\x1b[92m{:#?}\x1b[0m", spec.clone());
                            println!("\x1b[92m{:#?}\x1b[0m", r);
                        }
                        Err(e) => {
                            println!("\x1b[93mSkipped - {:#?}\x1b[0m", e.source().unwrap());
                        }
                    }
                }
                DeltaObject::Config { metadata, spec } => {}
                DeltaObject::Job { metadata, spec } => {
                    match &delta_manager.job(spec.clone()).await {
                        Ok(r) => {
                            println!("\x1b[92mSuccess - job finished\x1b[0m");
                            println!("\x1b[92m{:#?}\x1b[0m", spec.clone());
                            println!("\x1b[92m{:#?}\x1b[0m", r);
                        }
                        Err(e) => {
                            println!("\x1b[93mSkipped - {:#?}\x1b[0m", e.source().unwrap());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_config_local() -> Result<()> {
    let path = "../tests/delta/apply.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    println!("{:?}", delta_apply);
    Ok(())
}

#[tokio::test]
async fn test_config_url() -> Result<()> {
    let path = "../tests/delta/apply.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    println!("{:?}", delta_apply);
    Ok(())
}


#[tokio::test]
async fn test_apply() -> Result<()> {
    let path = "../tests/delta/apply.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    //println!("{:?}", delta_apply);

    delta_apply.apply().await?;

    //https://github.com/mackwic/colored/blob/master/src/color.rs
    //
    //println!("\x1b[91mError\x1b[0m");
    //println!("\x1b[92mSuccess\x1b[0m");
    //println!("\x1b[93mWarning\x1b[0m");
    //assert_eq!(delta_apply.delta_objects.len(), 4);
    Ok(())
}

#[tokio::test]
async fn test_apply_job() -> Result<()> {
    let path = "../tests/delta/apply_job.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    //println!("{:?}", delta_apply);

    delta_apply.apply().await?;

    //https://github.com/mackwic/colored/blob/master/src/color.rs
    //
    //println!("\x1b[91mError\x1b[0m");
    //println!("\x1b[92mSuccess\x1b[0m");
    //println!("\x1b[93mWarning\x1b[0m");
    //assert_eq!(delta_apply.delta_objects.len(), 4);
    Ok(())
}

#[tokio::test]
async fn test_read() -> Result<()> {
    let path = "az://test/".to_string();

    let ops = DeltaOps::try_from_uri(&path).await?;
    let os = ops.0.object_store();
    let url = Url::parse(&path)?;

    println!("{:?}", os);

    let ctx = SessionContext::new();

    ctx.runtime_env().register_object_store(
        url.scheme(),
        url.host_str().unwrap_or_default(),
        os.storage_backend(),
    );

    ctx.register_parquet(
        "alltypes_plain",
        &"az://test/data.parquet".to_string(),
        ParquetReadOptions::default(),
    )
    .await?;

    // execute the query
    let df = ctx.sql("SELECT * FROM alltypes_plain limit 10").await?;

    // print the results
    //df.show().await?;
    //df.collect_partitioned()
    //df.coll
    //
    let path = "../tests/delta/apply.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    let config = delta_apply.config;

    let conf = if let DeltaObject::Config { metadata, spec } = &config {
        spec.clone()
    } else {
        panic!("TTT");
    };
    let delta_manager = DeltaManager { config: conf };

    let rb = df.collect().await?;

    /*
    println!("##############################################");
    println!("{:#?}", rb[0].schema());
    let tb = delta_manager.table(&"az".to_string(),&"test_delta_5".to_string(), None, None).await?;
    let meta = tb.get_metadata()?;
    //let curr_schema: ArrowSchemaRef = Arc::new((&meta.schema).try_into()?);
    println!("##############################################");
    println!("{:#?}", &meta.schema);
    */
    delta_manager
        .write_batches(
            &"az".to_string(),
            &"test_delta_5".to_string(),
            rb,
            deltalake::action::SaveMode::Append,
        )
        .await?;

    Ok(())
}
