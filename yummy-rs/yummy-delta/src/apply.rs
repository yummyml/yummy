use crate::common::EntityValue;
use crate::config::{ColumnSchema, DeltaConfig};
use crate::models::{CreateRequest, OptimizeRequest, VacuumRequest, WriteRequest};
use anyhow::{anyhow, Result};
use deltalake::Schema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub name: String,
    pub store: String,
    pub table: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum DeltaObject {
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
}

impl DeltaApply {
    pub fn new(path: &String) -> Result<DeltaApply> {
        let s = fs::read_to_string(path)?;
        let mut objects = Vec::new();
        for document in serde_yaml::Deserializer::from_str(&s) {
            let o = DeltaObject::deserialize(document)?;
            objects.push(o);
        }

        Ok(DeltaApply {
            delta_objects: objects,
        })
    }
}


#[test]
fn test_apply() -> Result<()> {
    let path = "../tests/delta/apply.yaml".to_string();
    let config = DeltaApply::new(&path)?;
    println!("{:?}", config);

    assert_eq!(config.delta_objects.len(), 3);
    Ok(())
}

