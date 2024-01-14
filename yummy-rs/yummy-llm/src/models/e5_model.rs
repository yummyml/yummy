use super::{
    config_pathbuf, tokenizer_pathbuf, weights_pathbuf, EmbeddingData, EmbeddingsModel,
    EmbeddingsResponse,
};
use crate::config::LLMSpec;
use anyhow::Error as E;
use candle::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use tokenizers::{PaddingParams, Tokenizer};
use yummy_core::common::Result;

#[derive(thiserror::Error, Debug)]
pub enum E5Error {
    #[error("Wrong E5 config")]
    WrongConfig,
}

const E5_HF_REPO_ID: &str = "intfloat/e5-small-v2";

pub struct E5Model {
    model: BertModel,
    tokenizer: Tokenizer,
    normalize_embeddings: Option<bool>,
}

impl E5Model {
    pub fn new(config: &LLMSpec) -> Result<E5Model> {
        let model = weights_pathbuf(config, E5_HF_REPO_ID)?;
        let tokenizer = tokenizer_pathbuf(config, E5_HF_REPO_ID)?;
        let candle_config = config_pathbuf(config, E5_HF_REPO_ID)?;
        let candle_config: Config = serde_json::from_str(&candle_config)?;
        let device = &Device::Cpu;
        let mut tokenizer = tokenizers::Tokenizer::from_file(tokenizer).map_err(E::msg)?;

        if let Some(pp) = tokenizer.get_padding_mut() {
            pp.strategy = tokenizers::PaddingStrategy::BatchLongest
        } else {
            let pp = PaddingParams {
                strategy: tokenizers::PaddingStrategy::BatchLongest,
                ..Default::default()
            };
            tokenizer.with_padding(Some(pp));
        }

        let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[model], DType::F32, device)? };
        let model = BertModel::load(vb, &candle_config)?;
        Ok(E5Model {
            model,
            tokenizer,
            normalize_embeddings: config.normalize_embeddings,
        })
    }
}

impl EmbeddingsModel for E5Model {
    fn forward(&self, input: Vec<String>) -> Result<EmbeddingsResponse> {
        let device = &Device::Cpu;
        let tokens = self.tokenizer.encode_batch(input, true).unwrap();
        let token_ids: Vec<Tensor> = tokens
            .iter()
            .map(|tokens| {
                let tokens = tokens.get_ids().to_vec();
                Tensor::new(tokens.as_slice(), device)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let token_ids = Tensor::stack(&token_ids, 0)?;
        let token_type_ids = token_ids.zeros_like()?;

        let embeddings = self.model.forward(&token_ids, &token_type_ids)?;
        let (_n_sentence, n_tokens, _hidden_size) = embeddings.dims3()?;
        let embeddings = (embeddings.sum(1)? / (n_tokens as f64))?;
        let embeddings = if let Some(true) = self.normalize_embeddings {
            embeddings.broadcast_div(&embeddings.sqr()?.sum_keepdim(1)?.sqrt()?)?
        } else {
            embeddings
        };
        let embeddings_data = embeddings
            .to_vec2()?
            .iter()
            .map(|x| EmbeddingData {
                embedding: x.to_vec(),
            })
            .collect();

        Ok(EmbeddingsResponse {
            data: embeddings_data,
        })
    }
}

#[cfg(test)]
mod tests {}
