mod common;
use common::delta::{create_delta, create_manager, drop_delta};
use std::error::Error;
use std::fs;
use yummy_delta::delta::DeltaInfo;

#[tokio::test]
async fn test_delta_list_stores() -> Result<(), Box<dyn Error>> {
    let stores = create_manager().await?.list_stores()?;

    assert_eq!(stores.len(), 4);

    Ok(())
}

#[tokio::test]
async fn test_delta_list_tables_local() -> Result<(), Box<dyn Error>> {
    let store_name = String::from("local2");
    let table_name1 = String::from("test_delta_1");
    let table_name2 = String::from("test_delta_2");

    let _table1 = create_delta(&store_name, &table_name1).await?;
    let _table2 = create_delta(&store_name, &table_name2).await?;

    let tables = create_manager().await?.list_tables(&store_name).await?;
    println!("{:?}", tables.tables);
    assert_eq!(tables.tables.len(), 2);
    fs::remove_dir_all("/tmp/delta-test-2")?;
    Ok(())
}

#[tokio::test]
async fn test_delta_details() -> Result<(), Box<dyn Error>> {
    let store_name = String::from("local");
    let table_name = String::from("test_delta");

    let table = create_delta(&store_name, &table_name).await?;

    let resp = create_manager()
        .await?
        .details(&store_name, &table_name, None, None)
        .await?;
    assert_eq!(table.version(), 0);
    assert_eq!(resp.version, 0);

    drop_delta(&table_name).await?;
    Ok(())
}
