use yummy_delta::apply::DeltaApply;
use yummy_core::common::Result;
use std::fs;

mod common;

#[tokio::test]
async fn test_config_url() -> Result<()> {
    let path = "https://raw.githubusercontent.com/yummyml/yummy/yummy-rs-delta-0.7.0/yummy-rs/tests/delta/apply.yaml".to_string();
    let delta_apply = DeltaApply::new(&path).await?;
    println!("{delta_apply:?}");
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
    let path = "../../examples/delta/gameplay_move_data.yaml".to_string();
    //let delta_apply = DeltaApply::new(&path).await?;
    //println!("{:?}", delta_apply);

    //delta_apply.apply().await?;

    //https://github.com/mackwic/colored/blob/master/src/color.rs
    //
    //println!("\x1b[91mError\x1b[0m");
    //println!("\x1b[92mSuccess\x1b[0m");
    //println!("\x1b[93mWarning\x1b[0m");
    //assert_eq!(delta_apply.delta_objects.len(), 4);
    Ok(())
}
