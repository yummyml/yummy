mod common;
use common::delta::{create_delta, create_manager, drop_delta};
use deltalake::protocol::SaveMode;
use std::collections::HashMap;
use std::error::Error;
use yummy_core::common::EntityValue;
use yummy_delta::delta::{DeltaCommands, DeltaWrite};
use yummy_delta::models::{OptimizeRequest, VacuumRequest, WriteRequest};

#[tokio::test]
async fn test_delta_create() -> Result<(), Box<dyn Error>> {
    let store_name = String::from("local");
    let table_name = String::from("test_delta_1_com_cr");

    let table = create_delta(&store_name, &table_name).await?;

    assert_eq!(table.version(), 0);

    drop_delta(&table_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_delta_optimize() -> Result<(), Box<dyn Error>> {
    let store_name = String::from("local");
    let table_name = String::from("test_delta_1_com_opt");

    let table = create_delta(&store_name, &table_name).await?;
    assert_eq!(table.version(), 0);

    let mut batch1: HashMap<String, EntityValue> = HashMap::new();
    batch1.insert("col1".to_string(), EntityValue::INT32(1));
    batch1.insert("col2".to_string(), EntityValue::STRING("A".to_string()));

    let mut batch2: HashMap<String, EntityValue> = HashMap::new();
    batch2.insert("col1".to_string(), EntityValue::INT32(1));
    batch2.insert("col2".to_string(), EntityValue::STRING("A".to_string()));

    for _i in 0..15 {
        let write_request1 = WriteRequest {
            record_batch_dict: None,
            record_batch_list: Some(vec![batch1.clone(), batch2.clone()]),
        };

        let _resp1 = create_manager()
            .await?
            .write(&store_name, &table_name, write_request1, SaveMode::Append)
            .await?;
    }

    let optimize_request = OptimizeRequest {
        target_size: 2_000_000,
        filters: None,
    };

    let optimize_response = create_manager()
        .await?
        .optimize(&store_name, &table_name, optimize_request)
        .await?;

    println!("{optimize_response:?}");

    let vacuum_request = VacuumRequest {
        retention_period_seconds: Some(0),
        enforce_retention_duration: Some(false),
        dry_run: Some(true),
    };

    let vacuum_response = create_manager()
        .await?
        .vacuum(&store_name, &table_name, vacuum_request)
        .await?;

    println!("{vacuum_response:?}");

    drop_delta(&table_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_delta_optimize_filters() -> Result<(), Box<dyn Error>> {
    let store_name = String::from("local");
    let table_name = String::from("test_delta_1_com_optf");

    let table = create_delta(&store_name, &table_name).await?;
    assert_eq!(table.version(), 0);

    let mut batch1: HashMap<String, EntityValue> = HashMap::new();
    batch1.insert("col1".to_string(), EntityValue::INT32(1));
    batch1.insert("col2".to_string(), EntityValue::STRING("A".to_string()));

    let mut batch2: HashMap<String, EntityValue> = HashMap::new();
    batch2.insert("col1".to_string(), EntityValue::INT32(1));
    batch2.insert("col2".to_string(), EntityValue::STRING("A".to_string()));

    for _i in 0..15 {
        let write_request1 = WriteRequest {
            record_batch_dict: None,
            record_batch_list: Some(vec![batch1.clone(), batch2.clone()]),
        };

        let _resp1 = create_manager()
            .await?
            .write(&store_name, &table_name, write_request1, SaveMode::Append)
            .await?;
    }

    let optimize_request = OptimizeRequest {
        target_size: 2_000_000,
        filters: Some(vec![yummy_delta::models::PartitionFilter {
            column: "col2".to_string(),
            operator: "=".to_string(),
            value: EntityValue::STRING("A".to_string()),
        }]),
    };

    let optimize_response = create_manager()
        .await?
        .optimize(&store_name, &table_name, optimize_request)
        .await?;

    println!("{optimize_response:?}");

    let vacuum_request = VacuumRequest {
        retention_period_seconds: Some(0),
        enforce_retention_duration: Some(false),
        dry_run: Some(false),
    };
    let vacuum_response = create_manager()
        .await?
        .vacuum(&store_name, &table_name, vacuum_request)
        .await?;

    println!("{vacuum_response:?}");

    drop_delta(&table_name).await?;
    Ok(())
}
