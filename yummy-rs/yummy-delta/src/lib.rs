pub mod apply;
pub mod common;
pub mod config;
pub mod delta;
pub mod models;
pub mod server;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use apply::DeltaApply;
use delta::DeltaManager;
use pyo3::prelude::*;
use server::{
    append, create_table, details, health, list_stores, list_tables, optimize, overwrite,
    query_stream, vacuum,
};

#[pyfunction]
fn run_apply(config_path: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(apply_delta(config_path))
        .unwrap();
    Ok("Ok".to_string())
}

#[pyfunction]
fn run(config_path: String, host: String, port: u16, log_level: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_delta_server(config_path, host, port, log_level))
        .unwrap();
    Ok("Ok".to_string())
}

#[pymodule]
fn yummy_delta(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(run_apply, m)?)?;

    Ok(())
}

pub async fn apply_delta(config_path: String) -> std::io::Result<()> {
    DeltaApply::new(&config_path)
        .await
        .unwrap()
        .apply()
        .await
        .unwrap();
    Ok(())
}

pub async fn run_delta_server(
    config_path: String,
    host: String,
    port: u16,
    log_level: String,
) -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or(log_level));
    println!("Yummy delta server running on http://{}:{}", host, port);

    let _ = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(
                DeltaManager::new(config_path.clone()).unwrap(),
            ))
            .route("/health", web::get().to(health))
            .service(
                web::scope("/api").service(
                    web::scope("/1.0").service(
                        web::scope("/delta")
                            .route("/", web::get().to(list_stores))
                            .route("/{store_name}", web::get().to(list_tables))
                            .route("/{store_name}/{table_name}", web::get().to(query_stream))
                            .route("/{store_name}/{table_name}/details", web::get().to(details))
                            .route("/{store_name}", web::post().to(create_table))
                            .route("/{store_name}/{table_name}", web::post().to(append))
                            .route("/{store_name}/{table_name}", web::put().to(overwrite))
                            .route(
                                "/{store_name}/{table_name}/optimize",
                                web::post().to(optimize),
                            )
                            .route("/{store_name}/{table_name}/vacuum", web::post().to(vacuum)),
                    ),
                ),
            )
            .wrap(Logger::default())
    })
    .bind((host, port))?
    .run()
    .await;

    Ok(())
}
