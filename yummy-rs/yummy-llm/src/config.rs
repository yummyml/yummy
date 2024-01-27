use serde::Deserialize;
use yummy_core::common::Result;
use yummy_core::config::{read_config_str, Metadata};

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LLMEndpoint {
    Embeddings { metadata: Metadata, spec: LLMSpec },
    Completions { metadata: Metadata, spec: LLMSpec },
    Chat { metadata: Metadata, spec: LLMSpec },
}

#[derive(Deserialize, Debug, Clone)]
pub enum ModelType {
    E5,
    JinaBert,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LLMSpec {
    pub model: Option<String>,
    pub tokenizer: Option<String>,
    pub config: Option<String>,
    pub hf_model_repo_id: Option<String>,
    pub hf_tokenizer_repo_id: Option<String>,
    pub hf_config_repo_id: Option<String>,
    pub normalize_embeddings: Option<bool>,
    pub model_type: ModelType,
    pub use_pth: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LLMConfig {
    pub endpoints: Vec<LLMEndpoint>,
}

impl LLMConfig {
    pub async fn new(path: &String) -> Result<LLMConfig> {
        let s = read_config_str(path, Some(true)).await?;
        let mut endpoints = Vec::new();
        for document in serde_yaml::Deserializer::from_str(&s) {
            let o = LLMEndpoint::deserialize(document)?;
            endpoints.push(o);
        }

        Ok(LLMConfig { endpoints })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parse_config() -> Result<()> {
        let path = "../tests/llm/config.yaml".to_string();
        let config = LLMConfig::new(&path).await?;
        println!("{config:?}");

        Ok(())
    }
}
