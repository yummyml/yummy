use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use yummy_core::common::Result;
use yummy_core::config::read_config_str;

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
    pub async fn new(path: &String) -> Result<DeltaConfig> {
        let s = read_config_str(path, Some(true)).await?;
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

#[tokio::test]
async fn test_config() -> Result<()> {
    let path = "../tests/delta/config.yaml".to_string();
    let config = DeltaConfig::new(&path).await?;
    println!("{config:?}");

    assert_eq!(config.stores.len(), 4);
    assert_eq!(config.stores[0].name, "local");
    Ok(())
}
