use crate::config::MLConfig;
use crate::models::MLModel;
use actix_web::http::StatusCode;
use actix_web::{error, web, HttpResponse, Responder, Result};
use derive_more::{Display, Error};
use serde::{Deserialize, Serialize};
use std::error::Error;
use yummy_core::common::EntityValue;

#[derive(Deserialize, Serialize, Debug)]
pub struct MLModelRequest {
    columns: Vec<String>,
    data: Vec<Vec<EntityValue>>,
}

#[derive(Debug, Display, Error)]
#[display(fmt = "request error: {name}")]
pub struct FeaturesError {
    name: &'static String,
    status_code: u16,
}

impl error::ResponseError for FeaturesError {
    fn status_code(&self) -> StatusCode {
        let status_code: StatusCode = StatusCode::from_u16(self.status_code)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        status_code
    }
}

pub async fn health() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

pub async fn invocations(
    mlmodel_request: web::Json<MLModelRequest>,
    mlmodel: web::Data<Box<dyn MLModel>>,
    _config: web::Data<MLConfig>,
) -> Result<impl Responder, Box<dyn Error>> {
    let resp = mlmodel.predict(
        mlmodel_request.columns.clone(),
        mlmodel_request.data.clone(),
    )?;
    Ok(web::Json(resp))
}
