mod common;
use common::delta::{create_delta, create_manager, drop_delta};
use deltalake::action::SaveMode;
use std::collections::HashMap;
use std::error::Error;
use yummy_core::common::EntityValue;
use yummy_delta::delta::DeltaWrite;
use yummy_delta::models::WriteRequest;

#[tokio::test]
async fn test_delta_write_dict() -> Result<(), Box<dyn Error>> {
    let store_name = String::from("local");
    let table_name = String::from("test_delta_1_wr_dc");

    let table = create_delta(&store_name, &table_name).await?;
    assert_eq!(table.version(), 0);

    let mut batch1: HashMap<String, Vec<EntityValue>> = HashMap::new();
    batch1.insert("col1".to_string(), vec![EntityValue::INT32(1)]);
    batch1.insert(
        "col2".to_string(),
        vec![EntityValue::STRING("A".to_string())],
    );

    let mut batch2: HashMap<String, Vec<EntityValue>> = HashMap::new();
    batch2.insert("col1".to_string(), vec![EntityValue::INT32(1)]);
    batch2.insert(
        "col2".to_string(),
        vec![EntityValue::STRING("A".to_string())],
    );

    let write_request1 = WriteRequest {
        record_batch_dict: Some(batch1),
        record_batch_list: None,
    };

    let resp1 = create_manager()
        .await?
        .write(&store_name, &table_name, write_request1, SaveMode::Append)
        .await?;
    assert_eq!(resp1.version, 1);

    let write_request2 = WriteRequest {
        record_batch_dict: Some(batch2),
        record_batch_list: None,
    };
    let resp2 = create_manager()
        .await?
        .write(&store_name, &table_name, write_request2, SaveMode::Append)
        .await?;
    assert_eq!(resp2.version, 2);

    drop_delta(&table_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_delta_write_list() -> Result<(), Box<dyn Error>> {
    let store_name = String::from("local");
    let table_name = String::from("test_delta_1_wr_ls");

    let table = create_delta(&store_name, &table_name).await?;
    assert_eq!(table.version(), 0);

    let mut batch1: HashMap<String, EntityValue> = HashMap::new();
    batch1.insert("col1".to_string(), EntityValue::INT32(1));
    batch1.insert("col2".to_string(), EntityValue::STRING("A".to_string()));

    let mut batch2: HashMap<String, EntityValue> = HashMap::new();
    batch2.insert("col1".to_string(), EntityValue::INT32(1));
    batch2.insert("col2".to_string(), EntityValue::STRING("A".to_string()));

    let write_request1 = WriteRequest {
        record_batch_dict: None,
        record_batch_list: Some(vec![batch1, batch2]),
    };

    let resp1 = create_manager()
        .await?
        .write(&store_name, &table_name, write_request1, SaveMode::Append)
        .await?;
    assert_eq!(resp1.version, 1);

    drop_delta(&table_name).await?;
    Ok(())
}
