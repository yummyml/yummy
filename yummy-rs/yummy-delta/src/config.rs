use crate::common::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct DeltaConfig {
    pub stores: Vec<DeltaStoreConfig>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct DeltaStoreConfig {
    pub name: String,
    pub path: String,
    pub storage_options: Option<HashMap<String, String>>,
}

impl DeltaConfig {
    pub fn new(path: &String) -> Result<DeltaConfig> {
        let s = fs::read_to_string(path)?;
        let config: DeltaConfig = serde_yaml::from_str(&s)?;
        Ok(config)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub r#type: String,
    pub nullable: bool,
}

#[test]
fn test_config() -> Result<()> {
    let path = "../tests/delta/config.yaml".to_string();
    let config = DeltaConfig::new(&path)?;
    println!("{config:?}");

    assert_eq!(config.stores.len(), 4);
    assert_eq!(config.stores[0].name, "local");
    Ok(())
}
