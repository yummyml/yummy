pub mod apply;
pub mod config;
pub mod delta;
pub mod models;
pub mod output;
pub mod server;
pub mod udf;

use crate::delta::{
    read::map_record_batch, DeltaCommands, DeltaInfo, DeltaManager, DeltaRead, DeltaWrite,
};
use crate::output::PrettyOutput;
use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use apply::DeltaApply;
use server::{
    append, create_table, details, health, list_stores, list_tables, optimize, overwrite,
    query_stream, vacuum,
};

pub async fn pprint_stores(config_path: String) -> std::io::Result<()> {
    let manager = DeltaApply::new(&config_path).await.unwrap().delta_manager().unwrap();
    let stores = manager.list_stores().unwrap();
    let p = stores.to_prettytable().unwrap();
    println!("{p}");
    Ok(())
}

pub async fn pprint_table(
    config_path: String,
    store_name: String,
    table_name: String,
) -> std::io::Result<()> {
    let manager = DeltaApply::new(&config_path).await.unwrap().delta_manager().unwrap();
    let table = manager
        .details(&store_name, &table_name, None, None)
        .await
        .unwrap();
    let t = table.to_prettytable().unwrap();
    println!("{t}");
    Ok(())
}

pub async fn pprint_tables(config_path: String, store_name: String) -> std::io::Result<()> {
    let manager = DeltaApply::new(&config_path).await.unwrap().delta_manager().unwrap();
    let tables = manager.list_tables(&store_name).await.unwrap();
    let t = tables.to_prettytable().unwrap();
    println!("{t}");
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
    println!("Yummy delta server running on http://{host}:{port}");
    let delta_manager = DeltaApply::new(&config_path.clone()).await.unwrap().delta_manager().unwrap();

    let _ = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(delta_manager.clone()))
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
