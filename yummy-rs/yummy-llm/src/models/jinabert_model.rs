use super::{
    tokenizer_pathbuf, weights_pathbuf, EmbeddingData, EmbeddingsModel, EmbeddingsResponse,
};
use crate::config::LLMSpec;
use anyhow::Error as E;
use candle::{DType, Device, Module, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::jina_bert::{BertModel, Config};
use tokenizers::{PaddingParams, Tokenizer};
use yummy_core::common::Result;

#[derive(thiserror::Error, Debug)]
pub enum E5Error {
    #[error("Wrong E5 config")]
    WrongConfig,
}

const JINABERT_HF_REPO_ID: &str = "jinaai/jina-embeddings-v2-base-en";
const JINABERT_HF_TOKENIZER_REPO_ID: &str = "sentence-transformers/all-MiniLM-L6-v2";

pub struct JinaBertModel {
    model: BertModel,
    tokenizer: Tokenizer,
    normalize_embeddings: Option<bool>,
}

impl JinaBertModel {
    pub fn new(config: &LLMSpec) -> Result<JinaBertModel> {
        let model = weights_pathbuf(config, JINABERT_HF_REPO_ID)?;
        let tokenizer = tokenizer_pathbuf(config, JINABERT_HF_TOKENIZER_REPO_ID)?;
        let candle_config = Config::v2_base();
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

        let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[model], DType::F32, &device)? };
        let model = BertModel::new(vb, &candle_config)?;

        Ok(JinaBertModel {
            model,
            tokenizer,
            normalize_embeddings: config.normalize_embeddings,
        })
    }
}

impl EmbeddingsModel for JinaBertModel {
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
        let embeddings = self.model.forward(&token_ids)?;
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
