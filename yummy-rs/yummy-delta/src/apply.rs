use crate::config::DeltaConfig;
use crate::delta::{
    read::map_record_batch, DeltaCommands, DeltaInfo, DeltaJobs, DeltaManager, DeltaRead,
    DeltaWrite,
};
use crate::models::{CreateRequest, JobRequest, OptimizeRequest, VacuumRequest, WriteRequest};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use deltalake::DeltaOps;
use serde::Deserialize;
use std::fs;
use url::Url;
use yummy_core::common::{ReplaceTokens, Result};
use yummy_core::config::read_config_str;
use yummy_core::err;

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
        let configuration_str = read_config_str(path, Some(true)).await?;
        let mut objects = Vec::new();
        for document in serde_yaml::Deserializer::from_str(&configuration_str) {
            let o = DeltaObject::deserialize(document)?;
            objects.push(o);
        }

        let config = objects
            .clone()
            .into_iter()
            .filter(|x| {
                matches!(
                    x,
                    DeltaObject::Config {
                        metadata: _m,
                        spec: _s,
                    }
                )
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

    pub fn delta_manager(&self) -> Result<DeltaManager> {
        let conf = if let DeltaObject::Config { metadata: _, spec } = &self.config {
            spec.clone()
        } else {
            return Err(err!(ApplyError::NoConfig));
        };
        Ok(DeltaManager { config: conf })
    }

    pub async fn apply(&self) -> Result<()> {
        let delta_manager = self.delta_manager()?;

        for o in &self.delta_objects {
            match o {
                DeltaObject::Table { metadata, spec } => {
                    let store_name = metadata
                        .clone()
                        .store
                        .ok_or(err!(ApplyError::WrongTableMetadata))?;
                    let _table_name = spec.clone().table;

                    match &delta_manager.create(&store_name, spec.clone()).await {
                        Ok(r) => {
                            println!("\x1b[92mSuccess - table created\x1b[0m");
                            //println!("\x1b[92m{:#?}\x1b[0m", spec.clone());
                            println!("\x1b[92m{r:#?}\x1b[0m");
                        }
                        Err(e) => {
                            println!("\x1b[93mSkipped - {e:#?}\x1b[0m");
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
                            //println!("\x1b[92m{:#?}\x1b[0m", spec.clone());
                            println!("\x1b[92m{r:#?}\x1b[0m");
                        }
                        Err(e) => {
                            println!("\x1b[93mSkipped - {e:#?}\x1b[0m");
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
                            //println!("\x1b[92m{:#?}\x1b[0m", spec.clone());
                            println!("\x1b[92m{r:#?}\x1b[0m");
                        }
                        Err(e) => {
                            println!("\x1b[93mSkipped - {e:#?}\x1b[0m");
                        }
                    }
                }
                DeltaObject::Config {
                    metadata: _,
                    spec: _,
                } => {}
                DeltaObject::Job { metadata, spec } => {
                    match &delta_manager.job(spec.clone()).await {
                        Ok(r) => {
                            println!("\x1b[92mSuccess - job finished\x1b[0m");
                            println!("\x1b[92m{:#?}\x1b[0m", metadata.name.clone());
                            println!("\x1b[92m{r:#?}\x1b[0m");
                        }
                        Err(e) => {
                            println!("\x1b[93mSkipped - {e:#?}\x1b[0m");
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
    println!("{delta_apply:?}");
    Ok(())
}

#[tokio::test]
async fn test_config_url() -> Result<()> {
    let path = "https://raw.githubusercontent.com/yummyml/yummy/yummy-rs-delta-0.7.0/yummy-rs/tests/delta/apply.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    println!("{delta_apply:?}");
    Ok(())
}

#[tokio::test]
async fn test_apply_table() -> Result<()> {
    let path = "../../examples/delta/gameplay_tables.yaml".to_string();
    //let delta_apply = DeltaApply::new(&path).await?;
    //println!("{delta_apply:?}");

    //delta_apply.apply().await?;

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
    let path = "../../examples/delta/gameplay_move_data.yaml".to_string();
    //let delta_apply = DeltaApply::new(&path).await?;
    //println!("{:?}", delta_apply);

    //delta_apply.apply().await?;

    //https://github.com/mackwic/colored/blob/master/src/color.rs
    //
    //println!("\x1b[91mError\x1b[0m");
    //println!("\x1b[92mSuccess\x1b[0m");
    //println!("\x1b[93mWarning\x1b[0m");
    //assert_eq!(delta_apply.delta_objects.len(), 4);
    Ok(())
}
