use serde::{Deserialize, Serialize};
use std::error::Error;
use yummy_core::config::read_config_str;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Config {
    pub project: String,
    pub registry: String,
    pub online_store: OnlineStoreConfig,
    pub entity_key_serialization_version: i32,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct OnlineStoreConfig {
    #[serde(alias = "type")]
    pub store_type: String,
    pub connection_string: String,
}

impl Config {
    pub async fn new(path: &String) -> Result<Config, Box<dyn Error>> {
        let s = read_config_str(path, Some(true)).await?;
        let config: Config = serde_yaml::from_str(&s)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parse_config() -> Result<(), Box<dyn Error>> {
        let path = "../tests/feature_store.yaml".to_string();
        let config = Config::new(&path).await?;
        println!("{config:?}");

        match config.online_store {
            OnlineStoreConfig {
                store_type,
                connection_string,
            } => {
                assert_eq!(store_type, "redis");
                assert_eq!(connection_string, "redis://redis/");
            }
            _ => panic!("wrong job destination"),
        }

        assert_eq!(config.project, "adjusted_drake");
        Ok(())
    }
}
