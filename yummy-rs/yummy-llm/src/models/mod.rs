pub mod e5_model;
use std::path::PathBuf;

use crate::config::{LLMEndpoint, LLMSpec, ModelType};
use e5_model::E5Model;
use serde::{Deserialize, Serialize};
use yummy_core::common::Result;

#[derive(thiserror::Error, Debug)]
pub enum ModelFactoryError {
    #[error("Wrong model type")]
    WrongModelType,
}

pub trait EmbeddingsModel {
    fn forward(&self, input: Vec<String>) -> Result<EmbeddingsResponse>;
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EmbeddingsResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EmbeddingData {
    embedding: Vec<f32>,
}

pub struct LLMModelFactory {}

impl LLMModelFactory {
    pub fn embedding_model(config: &LLMEndpoint) -> Result<Box<dyn EmbeddingsModel>> {
        if let LLMEndpoint::Embeddings { metadata: _, spec } = config {
            if let ModelType::E5 = spec.model_type {
                Ok(Box::new(E5Model::new(spec)?))
            } else {
                todo!()
            }
        } else {
            Err(Box::new(ModelFactoryError::WrongModelType))
        }
    }
}

pub fn weights_pathbuf(config: &LLMSpec, hf_repo_id: &str) -> Result<PathBuf> {
    use hf_hub::{api::sync::Api, Repo, RepoType};
    let model = match &config.model {
        Some(model_path) => std::path::PathBuf::from(model_path),
        None => {
            let repo_id = if let Some(rid) = &config.hf_model_repo_id {
                rid
            } else {
                hf_repo_id
            };

            let api = Api::new()?.repo(Repo::new(repo_id.to_string(), RepoType::Model));

            if let Some(true) = config.use_pth {
                api.get("pytorch_model.bin")?
            } else {
                api.get("model.safetensors")?
            }
        }
    };

    Ok(model)
}

pub fn tokenizer_pathbuf(config: &LLMSpec, hf_repo_id: &str) -> Result<PathBuf> {
    use hf_hub::{api::sync::Api, Repo, RepoType};
    let tokenizer = match &config.tokenizer {
        Some(tokenizer_path) => std::path::PathBuf::from(tokenizer_path),
        None => {
            let repo_id = if let Some(rid) = &config.hf_tokenizer_repo_id {
                rid
            } else {
                hf_repo_id
            };

            let api = Api::new()?.repo(Repo::new(repo_id.to_string(), RepoType::Model));

            api.get("tokenizer.json")?
        }
    };

    Ok(tokenizer)
}

pub fn config_pathbuf(config: &LLMSpec, hf_repo_id: &str) -> Result<String> {
    use hf_hub::{api::sync::Api, Repo, RepoType};
    let config = match &config.config {
        Some(config_path) => std::path::PathBuf::from(config_path),
        None => {
            let repo_id = if let Some(rid) = &config.hf_tokenizer_repo_id {
                rid
            } else {
                hf_repo_id
            };

            let api = Api::new()?.repo(Repo::new(repo_id.to_string(), RepoType::Model));

            api.get("config.json")?
        }
    };
    let config = std::fs::read_to_string(config)?;

    Ok(config)
}
