use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
pub mod config;
pub mod encoding;
pub mod registry;
pub mod server;
pub mod stores;
pub mod types;

use crate::config::Config;
use crate::registry::Registry;
use server::{get_online_features, health};
use stores::OnlineStoreFactory;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let path = "./feature_store.yaml".to_string();
    let config = Config::new(&path);

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("error"));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(OnlineStoreFactory::new(config.clone())))
            .app_data(web::Data::new(config.clone()))
            .app_data(web::Data::new(Registry::new(config.clone())))
            .wrap(Logger::new("%a %{User-Agent}i"))
            .route("/health", web::post().to(health))
            .route("/get-online-features", web::post().to(get_online_features))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
