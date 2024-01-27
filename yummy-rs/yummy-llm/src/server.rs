use crate::config::LLMConfig;
use crate::models::EmbeddingsModel;
use actix_web::http::StatusCode;
use actix_web::{error, web, HttpResponse, Responder};
use derive_more::{Display, Error};
use serde::{Deserialize, Serialize};
use yummy_core::common::Result;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EmbeddingsRequest {
    input: Vec<String>,
}

#[derive(Debug, Display, Error)]
#[display(fmt = "request error: {name}")]
pub struct LLMError {
    name: &'static String,
    status_code: u16,
}

impl error::ResponseError for LLMError {
    fn status_code(&self) -> StatusCode {
        let status_code: StatusCode =
            StatusCode::from_u16(self.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        status_code
    }
}

pub async fn health() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

pub async fn embeddings(
    embeddings_request: web::Json<EmbeddingsRequest>,
    embeddings_model: web::Data<Box<dyn EmbeddingsModel>>,
    _config: web::Data<LLMConfig>,
) -> Result<impl Responder> {
    let resp = embeddings_model.forward(embeddings_request.input.clone())?;
    Ok(web::Json(resp))
}
