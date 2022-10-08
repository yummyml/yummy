use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Config {
    pub project: String,
    pub registry: String,
    pub online_store: OnlineStoreConfig,
    pub entity_key_serialization_version: i32
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct OnlineStoreConfig {
    #[serde(alias = "type")]
    pub store_type: String,
    pub connection_string: String,
}

impl Config {
    pub fn new(path: &String) -> Config {
        let s = fs::read_to_string(path).unwrap();
        let config: Config = serde_yaml::from_str(&s).unwrap();
        config
    }
}

#[test]
fn parse_config() {
    let path = "./tests/feature_store.yaml".to_string();
    let config = Config::new(&path);
    println!("{:?}", config);

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
}
