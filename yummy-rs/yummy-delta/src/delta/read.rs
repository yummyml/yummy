use crate::delta::error::DeltaError;
use crate::delta::{DeltaManager, DeltaRead};
use crate::models::QueryResponse;
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::{
    array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray,
    },
    datatypes::DataType,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use yummy_core::common::EntityValue;

#[async_trait]
impl DeltaRead for DeltaManager {
    async fn query_stream(
        &self,
        store_name: &String,
        table_name: &String,
        query: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<SendableRecordBatchStream, Box<dyn Error>> {
        let table = self
            .table(store_name, table_name, table_version, table_date)
            .await?;
        let ctx = SessionContext::new();

        ctx.register_table(table_name.as_str(), Arc::new(table))?;
        let batches = ctx.sql(query.as_str()).await?;
        Ok(batches.execute_stream().await?)
    }

    async fn query_stream_partitioned(
        &self,
        store_name: &String,
        table_name: &String,
        query: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<Vec<SendableRecordBatchStream>, Box<dyn Error>> {
        let table = self
            .table(store_name, table_name, table_version, table_date)
            .await?;
        let ctx = SessionContext::new();

        ctx.register_table(table_name.as_str(), Arc::new(table))?;
        let batches = ctx.sql(query.as_str()).await?;
        Ok(batches.execute_stream_partitioned().await?)
    }

    async fn query(
        &self,
        store_name: &String,
        table_name: &String,
        query: &String,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<QueryResponse, Box<dyn Error>> {
        let table = self
            .table(store_name, table_name, table_version, table_date)
            .await?;
        let ctx = SessionContext::new();
        //let schema = &table.schema().unwrap();

        ctx.register_table(table_name.as_str(), Arc::new(table))?;
        let batches = ctx.sql(query.as_str()).await?.collect().await?;

        let query_batches: Vec<BTreeMap<String, Vec<EntityValue>>> = batches
            .iter()
            .map(
                |batch| -> Result<BTreeMap<String, Vec<EntityValue>>, Box<dyn Error>> {
                    map_record_batch(batch)
                },
            )
            .collect::<Result<Vec<BTreeMap<String, Vec<EntityValue>>>, Box<dyn Error>>>()?;
        Ok(QueryResponse {
            batches: query_batches,
        })
    }
}

pub fn map_record_batch(
    batch: &RecordBatch,
) -> Result<BTreeMap<String, Vec<EntityValue>>, Box<dyn Error>> {
    let mapped_batches: Vec<(String, Vec<EntityValue>)> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(
            |(ix, field)| -> Result<(String, Vec<EntityValue>), Box<dyn Error>> {
                let col = batch.column(ix).as_any();
                let batch_columns: Result<Vec<EntityValue>, Box<dyn Error>> =
                    match field.data_type() {
                        DataType::Utf8 => Ok(col
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::STRING(ar.unwrap().to_string()))
                            .collect()),

                        DataType::Int64 => Ok(col
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::INT64(ar.unwrap()))
                            .collect()),

                        DataType::Int32 => Ok(col
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::INT32(ar.unwrap()))
                            .collect()),

                        DataType::Int16 => Ok(col
                            .downcast_ref::<Int16Array>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::INT16(ar.unwrap()))
                            .collect()),

                        DataType::Int8 => Ok(col
                            .downcast_ref::<Int8Array>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::INT8(ar.unwrap()))
                            .collect()),

                        DataType::Float32 => Ok(col
                            .downcast_ref::<Float32Array>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::FLOAT32(ar.unwrap()))
                            .collect()),

                        DataType::Float64 => Ok(col
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::FLOAT64(ar.unwrap()))
                            .collect()),

                        DataType::Boolean => Ok(col
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::BOOL(ar.unwrap()))
                            .collect()),

                        DataType::Binary => Ok(col
                            .downcast_ref::<BinaryArray>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::BYTES(ar.unwrap().to_vec()))
                            .collect()),

                        DataType::Date32 => Ok(col
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::INT32(ar.unwrap()))
                            .collect()),

                        DataType::Date64 => Ok(col
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                            .map(|ar| EntityValue::INT64(ar.unwrap()))
                            .collect()),

                        _ => return Err(Box::new(DeltaError::ReadTypeConversionError)),
                    };

                let bc = batch_columns?;
                Ok((field.name().to_string(), bc))
            },
        )
        .collect::<Result<Vec<(String, Vec<EntityValue>)>, Box<dyn Error>>>()?;

    let filtered_batches: Vec<(String, Vec<EntityValue>)> = mapped_batches
        .into_iter()
        .filter(|x| !x.1.is_empty())
        .collect();
    Ok(BTreeMap::from_iter(filtered_batches))
}

#[cfg(test)]
mod test {
    use crate::delta::test_delta_util::{create_delta, create_manager, drop_delta};
    use crate::delta::{DeltaRead, DeltaWrite};
    use crate::models::WriteRequest;
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
}
