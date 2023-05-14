mod common;
use common::delta::{create_delta, create_manager, drop_delta};
use yummy_delta::delta::{DeltaRead, DeltaWrite};
use yummy_delta::models::WriteRequest;
use deltalake::action::SaveMode;
use std::collections::HashMap;
use std::error::Error;
use yummy_core::common::EntityValue;

#[tokio::test]
async fn test_delta_query() -> Result<(), Box<dyn Error>> {
    let store_name = String::from("local");
    let table_name = String::from("test_delta_1_rd_q");

    let _table = create_delta(&store_name, &table_name).await?;

    let mut batch: HashMap<String, Vec<EntityValue>> = HashMap::new();
    batch.insert(
        "col1".to_string(),
        vec![
            EntityValue::INT32(1),
            EntityValue::INT32(2),
            EntityValue::INT32(3),
        ],
    );
    batch.insert(
        "col2".to_string(),
        vec![
            EntityValue::STRING("A".to_string()),
            EntityValue::STRING("A".to_string()),
            EntityValue::STRING("A".to_string()),
        ],
    );

    let write_request = WriteRequest {
        record_batch_dict: Some(batch),
        record_batch_list: None,
    };

    let resp1 = create_manager()
        .await?
        .write(&store_name, &table_name, write_request, SaveMode::Append)
        .await?;
    assert_eq!(resp1.version, 1);

    //let query = format!("SELECT count(string) FROM {} WHERE CAST(int as INT) = 1 and  CAST(string as CHAR)='A' ", table_name);
    let query = format!(
        "SELECT CAST(col2 as STRING), SUM(col1), COUNT(col1), MAX(col1) FROM {} GROUP BY col2 ",
        table_name
    );
    //let query = format!("SELECT col2 FROM {} WHERE CAST(col1 as CHAR)='A' ",&table_name);

    //let query = format!("SELECT int FROM {} WHERE CAST(string as CHAR)='B' ",&table_name);

    //time_travel(path.clone(), String::from("2020-05-25T22:47:31-07:00")).await?;
    //time_travel(path.clone(), String::from("2022-12-18T16:47:31-07:00")).await?;

    let batches = create_manager()
        .await?
        .query(&store_name, &table_name, &query, None, None)
        .await?;
    //assert_eq!(tables.tables.len(), 2);
    println!("BATCHES: {batches:?}");

    drop_delta(&table_name).await?;
    Ok(())
}
