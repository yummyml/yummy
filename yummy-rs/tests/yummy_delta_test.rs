use yummy_delta::delta::DeltaConfig;

#[tokio::test]
async fn test_config() -> Result<()> {
    let path = "./config.yaml".to_string();
    let config = DeltaConfig::new(&path).await?;
    println!("{config:?}");

    assert_eq!(config.stores.len(), 4);
    assert_eq!(config.stores[0].name, "local");
    Ok(())
}
