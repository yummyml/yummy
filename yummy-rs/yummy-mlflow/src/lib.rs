pub mod config;
pub mod models;
pub mod server;
pub mod common;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use server::{invocations, health};
use models::MLModelFactory;
use config::MLConfig;

pub async fn serve_mlflow_model(
    model_path: String,
    host: String,
    port: u16,
    log_level: String,
) -> std::io::Result<()> {
    let config = MLConfig::new(&model_path);

    env_logger::init_from_env(env_logger::Env::new().default_filter_or(log_level));
    println!("Yummy mlflow server running on http://{}:{}", host, port);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(MLModelFactory::new(config.clone())))
            .app_data(web::Data::new(config.clone()))
            .wrap(Logger::default())
            .route("/health", web::post().to(health))
            .route("/invocations", web::post().to(invocations))
    })
    .bind((host, port))?
    .run()
    .await
}
