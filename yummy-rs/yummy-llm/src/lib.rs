pub mod common;
pub mod config;
pub mod models;
pub mod server;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use config::{LLMConfig, LLMSpec, ModelType};
use models::LLMModelFactory;
use num_traits::Zero;
use server::{embeddings, health};
use yummy_core::config::Metadata;

use crate::config::LLMEndpoint;

pub async fn serve_llm(
    model_path: String,
    host: String,
    port: u16,
    log_level: Option<&String>,
    workers: Option<&usize>,
) -> std::io::Result<()> {
    let config = LLMConfig::new(&model_path).await.unwrap();

    serve(config, host, port, log_level, workers).await
}

pub async fn serve_embeddings(
    model: String,
    normalize_embeddings: bool,
    host: String,
    port: u16,
    log_level: Option<&String>,
    workers: Option<&usize>,
) -> std::io::Result<()> {
    let config = LLMConfig {
        endpoints: vec![LLMEndpoint::Embeddings {
            metadata: Metadata {
                name: "embedding".to_string(),
                store: None,
                table: None,
            },
            spec: LLMSpec {
                model: None,
                tokenizer: None,
                config: None,
                hf_model_repo_id: None,
                hf_tokenizer_repo_id: None,
                hf_config_repo_id: None,
                normalize_embeddings: Some(normalize_embeddings),
                model_type: if model.to_uppercase() == "E5" {
                    ModelType::E5
                } else {
                    ModelType::JinaBert
                },
                use_pth: None,
            },
        }],
    };

    serve(config, host, port, log_level, workers).await
}

async fn serve(
    config: LLMConfig,
    host: String,
    port: u16,
    log_level: Option<&String>,
    workers: Option<&usize>,
) -> std::io::Result<()> {
    if let Some(v) = log_level {
        env_logger::init_from_env(env_logger::Env::new().default_filter_or(v));
    }

    println!("Yummy llm server running on http://{host}:{port}");
    let mut server = HttpServer::new(move || {
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
    });

    if let Some(num_workers) = workers {
        if !num_workers.is_zero() {
            server = server.workers(*num_workers);
        }
    }

    server.bind((host, port))?.run().await
}
