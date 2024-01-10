use crate::delta::error::DeltaError;
use crate::delta::{DeltaManager, DeltaRead};
use crate::models::QueryResponse;
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray,
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
        store_name: &str,
        table_name: &str,
        query: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<SendableRecordBatchStream, Box<dyn Error>> {
        let table = self
            .table(store_name, table_name, table_version, table_date)
            .await?;
        let ctx = SessionContext::new();

        ctx.register_table(table_name, Arc::new(table))?;
        let batches = ctx.sql(query).await?;
        Ok(batches.execute_stream().await?)
    }

    async fn query_stream_partitioned(
        &self,
        store_name: &str,
        table_name: &str,
        query: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<Vec<SendableRecordBatchStream>, Box<dyn Error>> {
        let table = self
            .table(store_name, table_name, table_version, table_date)
            .await?;
        let ctx = SessionContext::new();

        ctx.register_table(table_name, Arc::new(table))?;
        let batches = ctx.sql(query).await?;
        Ok(batches.execute_stream_partitioned().await?)
    }

    async fn query(
        &self,
        store_name: &str,
        table_name: &str,
        query: &str,
        table_version: Option<i64>,
        table_date: Option<String>,
    ) -> Result<QueryResponse, Box<dyn Error>> {
        let table = self
            .table(store_name, table_name, table_version, table_date)
            .await?;
        let ctx = SessionContext::new();
        //let schema = &table.schema().unwrap();

        ctx.register_table(table_name, Arc::new(table))?;
        let batches = ctx.sql(query).await?.collect().await?;

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

pub fn map_array(arr: &ArrayRef) -> Result<Vec<EntityValue>, Box<dyn Error>> {
    let col = arr.as_any();
    let batch_columns: Result<Vec<EntityValue>, Box<dyn Error>> = match arr.data_type() {
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

    batch_columns
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
                let bc = map_array(batch.column(ix))?;
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
