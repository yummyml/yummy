use crate::delta::{
    read::map_record_batch, DeltaCommands, DeltaInfo, DeltaManager, DeltaRead, DeltaWrite,
};
use crate::models::{
    CreateRequest, DetailsQuery, OptimizeRequest, QueryRequest, ResponseStores, ResponseTables,
    VacuumRequest, WriteRequest,
};
use actix_web::http::StatusCode;
use actix_web::{error, get, post, web, HttpResponse, Responder, Result};
use deltalake::action::SaveMode;
use derive_more::{Display, Error};
use serde::{Deserialize, Serialize};
use std::error::Error;

use crate::delta::error::DeltaError;
use bytes::{BufMut, Bytes, BytesMut};

use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::StreamExt;

/*
#[derive(Debug, Display, Error)]
#[display(fmt = "request error: {}", name)]
pub struct DeltaError {
    name: &'static str,
    status_code: u16,
}

impl error::ResponseError for DeltaError {
    fn status_code(&self) -> StatusCode {
        let status_code: StatusCode = StatusCode::from_u16((&self.status_code).clone())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        status_code
    }
}
*/

pub async fn health() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

pub async fn list_stores(
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let stores = delta_manager.list_stores()?;
    let resp = ResponseStores { stores };
    Ok(web::Json(resp))
}

pub async fn list_tables(
    path: web::Path<String>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let store_name = path.into_inner();
    let tables = delta_manager.list_tables(&store_name).await?;
    Ok(web::Json(tables))
}

pub async fn details(
    path: web::Path<(String, String)>,
    details_query: web::Query<DetailsQuery>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let (store_name, table_name) = path.into_inner();
    let table = delta_manager
        .details(
            &store_name,
            &table_name,
            details_query.table_version.clone(),
            details_query.table_date.clone(),
        )
        .await?;
    Ok(web::Json(table))
}

pub async fn query(
    path: web::Path<(String, String)>,
    query_request: web::Query<QueryRequest>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let (store_name, table_name) = path.into_inner();
    let table = delta_manager
        .query(
            &store_name,
            &table_name,
            &query_request.query,
            query_request.table_version.clone(),
            query_request.table_date.clone(),
        )
        .await?;
    Ok(web::Json(table))
}

pub async fn create_table(
    path: web::Path<String>,
    create_request: web::Json<CreateRequest>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let store_name = path.into_inner();
    let resp = delta_manager.create(&store_name, create_request.0).await?;
    Ok(web::Json(resp))
}

pub async fn append(
    path: web::Path<(String, String)>,
    write_request: web::Json<WriteRequest>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let (store_name, table_name) = path.into_inner();
    let resp = delta_manager
        .write(&store_name, &table_name, write_request.0, SaveMode::Append)
        .await?;
    Ok(web::Json(resp))
}

pub async fn overwrite(
    path: web::Path<(String, String)>,
    write_request: web::Json<WriteRequest>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let (store_name, table_name) = path.into_inner();
    let resp = delta_manager
        .write(
            &store_name,
            &table_name,
            write_request.0,
            SaveMode::Overwrite,
        )
        .await?;
    Ok(web::Json(resp))
}

pub async fn optimize(
    path: web::Path<(String, String)>,
    optimize_request: web::Json<OptimizeRequest>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let (store_name, table_name) = path.into_inner();
    let resp = delta_manager
        .optimize(&store_name, &table_name, optimize_request.0)
        .await?;
    Ok(web::Json(resp))
}

pub async fn vacuum(
    path: web::Path<(String, String)>,
    vacuum_request: web::Json<VacuumRequest>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let (store_name, table_name) = path.into_inner();
    let resp = delta_manager
        .vacuum(&store_name, &table_name, vacuum_request.0)
        .await?;
    Ok(web::Json(resp))
}

pub async fn query_stream(
    path: web::Path<(String, String)>,
    query_request: web::Query<QueryRequest>,
    delta_manager: web::Data<DeltaManager>,
) -> Result<impl Responder, Box<dyn Error>> {
    let (store_name, table_name) = path.into_inner();
    let braces = (&query_request.braces).unwrap_or(true).clone();

    let streams: Vec<SendableRecordBatchStream> = delta_manager
        .query_stream_partitioned(
            &store_name,
            &table_name,
            &query_request.query,
            query_request.table_version.clone(),
            query_request.table_date.clone(),
        )
        .await?;

    let stream_tasks = async_stream::stream! {
        let mut bytes = BytesMut::new();
        let mut is_first = true;

        if braces {
            bytes.extend_from_slice("[".as_bytes());
            let byte = bytes.split().freeze();
            yield Ok::<Bytes, Box<dyn Error>>(byte);
        }

        for mut stream in streams {
            while let Some(item) = stream.next().await {
                let it: datafusion::arrow::record_batch::RecordBatch = item?;
                let qr = map_record_batch(&it)?;

                if qr.keys().len() > 0usize {

                    if braces && !is_first {
                        bytes.extend_from_slice(",".as_bytes());
                        let byte = bytes.split().freeze();
                        yield Ok::<Bytes, Box<dyn Error>>(byte);
                    }
                    else if is_first {
                        is_first = false;
                    }

                    match serde_json::to_string(&qr) {
                        Ok(task) => {
                            bytes.extend_from_slice(task.as_bytes());
                            let byte = bytes.split().freeze();
                            yield Ok::<Bytes, Box<dyn Error>>(byte)
                        },
                        Err(_err) => yield Err(Box::new(DeltaError::TypeConversionError))
                    }
                }
            }
        }

        if braces {
            bytes.extend_from_slice("]".as_bytes());
            let byte = bytes.split().freeze();
            yield Ok::<Bytes, Box<dyn Error>>(byte);
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .streaming(Box::pin(stream_tasks)))
}
