use crate::common::Result;
use crate::config::DeltaConfig;
use crate::delta::{
    read::map_record_batch, DeltaCommands, DeltaInfo, DeltaManager, DeltaRead, DeltaWrite,
};
use crate::err;
use crate::models::{CreateRequest, OptimizeRequest, VacuumRequest, WriteRequest};
use serde::Deserialize;
use std::fs;

#[derive(thiserror::Error, Debug)]
pub enum ApplyError {
    #[error("Delta config kind required")]
    NoConfig,
    #[error("Delta table kind must contain store in metadata")]
    WrongTableMetadata,
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
}

#[derive(Deserialize, Debug)]
pub struct DeltaApply {
    pub delta_objects: Vec<DeltaObject>,
    pub config: DeltaObject,
}

impl DeltaApply {
    pub fn new(path: &String) -> Result<DeltaApply> {
        let s = fs::read_to_string(path)?;
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
            if let DeltaObject::Table { metadata, spec } = o {
                let store_name = metadata
                    .clone()
                    .store
                    .ok_or(err!(ApplyError::WrongTableMetadata))?;
                let table_name = spec
                    .clone()
                    .table;
                let table = &delta_manager
                    .details(&store_name, &table_name, None, None)
                    .await?;

                println!("{:?}", table);
            }
        }

        Ok(())
    }
}

//#[test]
#[tokio::test]
async fn test_apply() -> Result<()> {
    let path = "../tests/delta/apply.yaml".to_string();
    let delta_apply = DeltaApply::new(&path)?;
    //println!("{:?}", delta_apply);

    delta_apply.apply().await?;

    //assert_eq!(delta_apply.delta_objects.len(), 4);
    Ok(())
}
