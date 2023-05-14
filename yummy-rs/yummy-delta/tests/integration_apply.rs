use std::fs;
use yummy_core::common::Result;
use yummy_delta::apply::DeltaApply;
use yummy_delta::delta::DeltaRead;

mod common;

#[tokio::test]
async fn test_config_url() -> Result<()> {
    let path = "https://raw.githubusercontent.com/yummyml/yummy/yummy-rs-delta-0.7.0/yummy-rs/tests/delta/apply.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    Ok(())
}

#[tokio::test]
async fn test_apply_table() -> Result<()> {
    common::setup();
    let delta_path = "/tmp/test_apply_table".to_string();
    fs::create_dir_all(&delta_path)?;
    let path = "./tests/config/apply_table.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;

    delta_apply.apply().await?;

    fs::remove_dir_all(delta_path)?;
    Ok(())
}

#[tokio::test]
async fn test_apply_job() -> Result<()> {
    let delta_path = "/tmp/test_apply_gameplay".to_string();
    fs::create_dir_all(&delta_path)?;

    let path = "./tests/config/01_bronze_tables.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    delta_apply.apply().await?;

    std::env::set_var("date", "2023-04-23");
    let path_jobs = "./tests/config/02_bronze_jobs.yaml".to_string();
    let delta_jobs = DeltaApply::new(&path_jobs).await?;
    delta_jobs.apply().await?;

    let query = "select * from game_purchase".to_string();
    let store = "gameplay_bronze".to_string();
    let table = "game_purchase".to_string();
    let batches = delta_apply.delta_manager()?
        .query(&store, &table, &query, None, None)
        .await?;
    println!("BATCHES: {batches:?}");


    fs::remove_dir_all(delta_path)?;
    Ok(())
}
