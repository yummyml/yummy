pub mod common;
pub mod config;
pub mod models;
pub mod server;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use config::MLConfig;
use models::MLModelFactory;
use num_traits::Zero;
use server::{health, invocations};

pub async fn serve_ml_model(
    model_path: String,
    host: String,
    port: u16,
    log_level: Option<&String>,
    workers: Option<&usize>,
) -> std::io::Result<()> {
    let config = MLConfig::new(&model_path).await.unwrap();
    if let Some(v) = log_level {
        env_logger::init_from_env(env_logger::Env::new().default_filter_or(v));
    }

    println!("Yummy ml server running on http://{host}:{port}");
    let mut server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(MLModelFactory::new(config.clone()).unwrap()))
            .app_data(web::Data::new(config.clone()))
            .wrap(Logger::default())
            .route("/health", web::get().to(health))
            .route("/invocations", web::post().to(invocations))
    });

    if let Some(num_workers) = workers {
        if !num_workers.is_zero() {
            server = server.workers(*num_workers);
        }
    }

    server.bind((host, port))?.run().await
}
