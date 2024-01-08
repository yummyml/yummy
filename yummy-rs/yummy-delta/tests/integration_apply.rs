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
    let paths = vec![
        delta_path.clone(),
        format!("{delta_path:?}/bronze"),
        format!("{delta_path:?}/silver"),
        format!("{delta_path:?}/gold"),
    ];

    for path in paths {
        fs::create_dir_all(&path.to_string())?;
    }

    std::env::set_var("date", "2023-04-23");

    let configs = vec![
        "./tests/config/01_bronze_tables.yaml",
        "./tests/config/02_bronze_jobs.yaml",
        "./tests/config/03_silver_tables.yaml",
        "./tests/config/04_silver_jobs.yaml",
        //        "./tests/config/05_gold_tables.yaml",
        //        "./tests/config/06_gold_jobs.yaml",
    ];

    for config in configs {
        println!("##### {config:?} #####");
        let delta_apply = DeltaApply::new(&config.to_string()).await?;
        delta_apply.apply().await?;
    }

    let query = "select cast(purchase_date as string) as data from game_purchase limit 2".to_string();
    let store = "gameplay_silver".to_string();
    let table = "game_purchase".to_string();


    let config = "./tests/config/04_silver_jobs.yaml";
    let delta_apply = DeltaApply::new(&config.to_string()).await?;
    let batches = delta_apply.delta_manager()?
        .query(&store, &table, &query, None, None)
        .await?;
    println!("BATCHES: {batches:?}");

    fs::remove_dir_all(delta_path)?;
    Ok(())
}
