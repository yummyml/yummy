use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct MLConfig {
    pub base_path: Option<String>,
    pub artifact_path: String,
    pub mlflow_version: String,
    pub model_uuid: String,
    pub run_id: String,
    pub utc_time_created: String,
    pub flavors: Flavours,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct Flavours {
    pub catboost: Option<CatboostConfig>,
    pub lightgbm: Option<LightgbmConfig>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct CatboostConfig {
    pub catboost_version: String,
    pub data: String,
    pub model_type: String,
    pub save_format: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct LightgbmConfig {
    pub data: String,
    pub lgb_version: String,
    pub model_class: String,
}

impl MLConfig {
    pub fn new(path: &String) -> MLConfig {
        let config_path = format!("{}/MLmodel",path);
        let s = fs::read_to_string(config_path).unwrap();
        let mut config: MLConfig = serde_yaml::from_str(&s).unwrap();
        config.base_path = Some(path.to_string());
        config
    }
}

#[test]
fn parse_config() {
    let path = "../tests/mlflow/catboost_model/my_model".to_string();
    let config = MLConfig::new(&path);
    println!("{:?}", config);

    match config.flavors.catboost {
        Some(CatboostConfig {
            catboost_version,
            data,
            model_type,
            save_format,
        }) => {
            assert_eq!(catboost_version, "1.1");
            assert_eq!(data, "model.cb");
            assert_eq!(model_type, "CatBoostClassifier");
            assert_eq!(save_format, "cbm");
        }
        _ => panic!("wrong job destination"),
    }
    assert_eq!(config.base_path.unwrap(), path);
    assert_eq!(config.artifact_path, "my_model");
}
