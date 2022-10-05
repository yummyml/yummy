pub mod config;
pub mod encoding;
pub mod registry;
pub mod server;
pub mod stores;
pub mod types;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use config::Config;
use pyo3::prelude::*;
use registry::Registry;
use server::{get_online_features, health};
use stores::OnlineStoreFactory;

#[pyfunction]
fn serve(config_path: String, host: String, port: u16, log_level: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(serve_wrapper(config_path, host, port, log_level))
        .unwrap();
    Ok("Ok".to_string())
}

#[pymodule]
fn yummy_rs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(serve, m)?)?;

    Ok(())
}

async fn serve_wrapper(
    config_path: String,
    host: String,
    port: u16,
    log_level: String,
) -> std::io::Result<()> {
    let config = Config::new(&config_path);

    env_logger::init_from_env(env_logger::Env::new().default_filter_or(log_level));
    println!("Feature server running on http://{}:{}", host, port);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(OnlineStoreFactory::new(config.clone())))
            .app_data(web::Data::new(config.clone()))
            .app_data(web::Data::new(Registry::new(config.clone())))
            .wrap(Logger::default())
            .route("/health", web::post().to(health))
            .route("/get-online-features", web::post().to(get_online_features))
    })
    .bind((host, port))?
    .run()
    .await
}
