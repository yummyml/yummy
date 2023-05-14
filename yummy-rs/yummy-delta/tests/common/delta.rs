use yummy_delta::config::ColumnSchema;
use yummy_delta::delta::{DeltaCommands, DeltaManager};
use yummy_delta::models::{CreateRequest, SchemaPrimitiveType};
use deltalake::arrow::datatypes::DataType;
use std::error::Error;
use std::fs;

pub async fn create_manager() -> Result<DeltaManager, Box<dyn Error>> {
    let path = "../tests/delta/config.yaml".to_string();
    Ok(DeltaManager::new(path, None).await?)
}

pub async fn create_delta(
    store_name: &String,
    table_name: &String,
) -> Result<deltalake::DeltaTable, Box<dyn Error>> {
    let path = "../tests/delta/config.yaml".to_string();
    let delta_manager = DeltaManager::new(path, None).await?;

    //let store_name = String::from("az");
    let mut schema: Vec<ColumnSchema> = Vec::new();
    schema.push(ColumnSchema {
        name: String::from("col1"),
        r#type: String::from("integer"),
        nullable: false,
    });
    schema.push(ColumnSchema {
        name: String::from("col2"),
        r#type: String::from("string"),
        nullable: false,
    });

    let partition_columns = vec![String::from("col2")];

    let comment = String::from("Hello from delta");

    let request = CreateRequest {
        table: table_name.to_string(),
        schema,
        partition_columns: Some(partition_columns),
        comment: Some(comment),
        configuration: None,
        metadata: None,
    };

    let _res_create = delta_manager.create(&store_name, request).await?;
    delta_manager
        .table(&store_name, &table_name, None, None)
        .await
}

pub async fn drop_delta(table_name: &String) -> Result<(), Box<dyn Error>> {
    fs::remove_dir_all(format!("/tmp/delta-test-1/{table_name}"))?;
    Ok(())
}

