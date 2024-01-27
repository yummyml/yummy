pub mod config;
pub mod encoding;
pub mod registry;
pub mod server;
pub mod stores;
pub mod types;

use crate::config::Config;
use crate::registry::Registry;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use server::{get_online_features, health};
use stores::OnlineStoreFactory;

pub async fn serve_wrapper(
    config_path: String,
    host: String,
    port: u16,
    log_level: String,
) -> std::io::Result<()> {
    let config = Config::new(&config_path).await.unwrap();
    let registry = Registry::new(config.clone()).await.unwrap();

    env_logger::init_from_env(env_logger::Env::new().default_filter_or(log_level));
    println!("Feature server running on http://{host}:{port}");
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(
                OnlineStoreFactory::build(config.clone()).unwrap(),
            ))
            .app_data(web::Data::new(config.clone()))
            .app_data(web::Data::new(registry.clone()))
            .wrap(Logger::default())
            .route("/health", web::post().to(health))
            .route("/get-online-features", web::post().to(get_online_features))
    })
    .bind((host, port))?
    .run()
    .await
}
