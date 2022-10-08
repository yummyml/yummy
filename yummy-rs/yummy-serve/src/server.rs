use crate::stores::OnlineStore;
use actix_web::http::StatusCode;
use actix_web::{error, web, HttpResponse, Responder, Result};
use chrono;
use derive_more::{Display, Error};
use protobuf::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use yummy_core::config::Config;
use yummy_core::encoding::{parse_value, serialize_entity_keys, serialize_fields, EntityValue};
use yummy_core::registry::Registry;
use yummy_core::types::Value;

#[derive(Debug, Display, Error)]
#[display(fmt = "request error: {}", name)]
pub struct FeaturesError {
    name: &'static str,
    status_code: u16,
}

impl error::ResponseError for FeaturesError {
    fn status_code(&self) -> StatusCode {
        let status_code: StatusCode = StatusCode::from_u16((&self.status_code).clone())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        status_code
    }
}

#[derive(Deserialize)]
pub struct FeaturesRequest {
    feature_service: Option<String>,
    features: Option<Vec<String>>,
    entities: HashMap<String, Vec<EntityValue>>,
    #[serde(default = "default_full_feature_names")]
    full_feature_names: bool,
}

fn default_full_feature_names() -> bool {
    false
}

#[derive(Serialize)]
pub struct Metadata {
    feature_names: Vec<String>,
}

#[derive(Serialize)]
pub struct ResponseResult {
    values: Vec<EntityValue>,
    statuses: Vec<String>,
    event_timestamps: Vec<String>,
}

#[derive(Serialize)]
pub struct FeaturesResponse {
    metadata: Metadata,
    results: Vec<ResponseResult>,
}

pub async fn health() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

pub async fn get_online_features(
    feature_request: web::Json<FeaturesRequest>,
    online_store: web::Data<Box<dyn OnlineStore>>,
    config: web::Data<Config>,
    registry: web::Data<Registry>,
) -> Result<impl Responder, FeaturesError> {
    let project_name = &config.project;
    let serialization_version: i32 = config.entity_key_serialization_version;

    let join_keys: Vec<String> = feature_request
        .entities
        .keys()
        .into_iter()
        .map(|x| x.to_string())
        .collect();

    let entity_keys: Vec<Vec<u8>> = serialize_entity_keys(
        project_name.clone(),
        &join_keys,
        &feature_request.entities,
        serialization_version,
    );
    let (features_names, fields) = match &feature_request.features {
        Some(f) => {
            if registry.check_features(f.to_vec()) {
                (f.to_vec(), serialize_fields(f.to_vec()))
            } else {
                let err = FeaturesError {
                    name: "Incorrect features list",
                    status_code: 400,
                };
                return Err(err);
            }
        }
        None => match &feature_request.feature_service {
            Some(feature_service) => {
                let ff =
                    registry.get_feature_service(feature_service.clone(), project_name.clone());
                (ff.clone(), serialize_fields(ff.clone()))
            }
            None => {
                let err = FeaturesError {
                    name: "Specify features or feature_service",
                    status_code: 400,
                };
                return Err(err);
            }
        },
    };

    let result: Vec<Vec<Vec<u8>>> = online_store.get_online_features(entity_keys, fields).await;

    let resp: FeaturesResponse = prepare_response(
        features_names,
        feature_request.full_feature_names,
        &join_keys,
        &feature_request.entities,
        result,
    );

    Ok(web::Json(resp))
}

pub fn prepare_response(
    features: Vec<String>,
    full_feature_names: bool,
    join_keys: &Vec<String>,
    entities: &HashMap<String, Vec<EntityValue>>,
    result: Vec<Vec<Vec<u8>>>,
) -> FeaturesResponse {
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S");
    let n_join_keys = &join_keys.len();
    let fields_names: Vec<String> = features
        .to_vec()
        .into_iter()
        .map(|x| {
            let f_name = if full_feature_names {
                x.replace(":", "__")
            } else {
                x.split(":").collect::<Vec<&str>>()[1].to_string()
            };
            f_name
        })
        .collect();

    let feature_names: Vec<String> = [join_keys.clone(), fields_names].concat();

    let mut response_results: Vec<ResponseResult> = (0..feature_names.len())
        .into_iter()
        .map(|_x| ResponseResult {
            event_timestamps: Vec::new(),
            statuses: Vec::new(),
            values: Vec::new(),
        })
        .collect();

    for j in 0..result.len() {
        let r = &result[j];
        for i in 0..n_join_keys.clone() {
            response_results[i].event_timestamps.push(now.to_string());
            response_results[i].statuses.push("PRESENT".to_string());

            let ev = entities[&join_keys[i]][j].clone();
            let e_value: EntityValue = ev;
            response_results[i].values.push(e_value);
        }

        for i in 0..r.len() {
            let z = i + n_join_keys.clone();
            let v = &r[i];
            let vv: Value::Value = Message::parse_from_bytes(v).unwrap();
            response_results[z].event_timestamps.push(now.to_string());
            response_results[z].statuses.push("PRESENT".to_string());
            let ev: EntityValue = parse_value(vv);
            response_results[z].values.push(ev);
        }
    }

    let resp = FeaturesResponse {
        metadata: Metadata { feature_names },
        results: response_results,
    };

    resp
}
