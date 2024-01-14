pub mod common;
pub mod config;
pub mod models;
pub mod server;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use config::LLMConfig;
use models::LLMModelFactory;
use server::{embeddings, health};

use crate::config::LLMEndpoint;

pub async fn serve_llm(
    model_path: String,
    host: String,
    port: u16,
    log_level: String,
) -> std::io::Result<()> {
    let config = LLMConfig::new(&model_path).await.unwrap();

    env_logger::init_from_env(env_logger::Env::new().default_filter_or(log_level));
    println!("Yummy llm server running on http://{host}:{port}");
    HttpServer::new(move || {
        let mut app = App::new()
            .app_data(web::Data::new(config.clone()))
            .wrap(Logger::default())
            .route("/health", web::get().to(health));

        let embeddings_config = config
            .endpoints
            .iter()
            .filter(|x| {
                matches!(
                    x,
                    LLMEndpoint::Embeddings {
                        metadata: _m,
                        spec: _s,
                    }
                )
            })
            .last();

        if let Some(config) = embeddings_config {
            app = app
                .app_data(web::Data::new(
                    LLMModelFactory::embedding_model(config).unwrap(),
                ))
                .route("/embeddings", web::post().to(embeddings));
        }

        app
    })
    .bind((host, port))?
    .run()
    .await
}
