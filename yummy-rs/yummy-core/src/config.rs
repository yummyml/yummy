use crate::common::{ReplaceTokens, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs;
use url::Url;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub name: String,
    pub store: Option<String>,
    pub table: Option<String>,
}

pub async fn read_config_str(path: &String, replace_env: Option<bool>) -> Result<String> {
    let configuration_str = if Url::parse(path).is_ok() {
        reqwest::get(path).await?.text().await?
    } else {
        fs::read_to_string(path)?
    };

    if let Some(true) = replace_env {
        ReplaceTokens::replace(&configuration_str)
    } else {
        Ok(configuration_str)
    }
}

pub async fn read_config_bytes(path: &String) -> Result<Vec<u8>> {
    let res = if Url::parse(path).is_ok() {
        reqwest::get(path).await?.bytes().await?.to_vec()
    } else {
        fs::read(path)?
    };
    Ok(res)
}

pub async fn read_config<T>(path: &String, replace_env: Option<bool>) -> Result<T>
where
    T: DeserializeOwned,
{
    let config = read_config_str(path, replace_env).await?;
    let o: T = serde_yaml::from_str(&config)?;

    Ok(o)
}
