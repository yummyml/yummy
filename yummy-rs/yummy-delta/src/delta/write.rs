use crate::common::EntityValue;
use crate::delta::error::DeltaError;
use crate::delta::{DeltaManager, DeltaWrite};
use crate::models::{WriteRequest, WriteResponse};
use async_trait::async_trait;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    },
    datatypes::{DataType, Field, Schema as ArrowSchema},
};
use deltalake::{action::SaveMode, DeltaOps};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

#[async_trait]
impl DeltaWrite for DeltaManager {

    async fn write_batches(&self,
        store_name: &String,
        table_name: &String,
        record_batches: Vec<RecordBatch>,
        save_mode: SaveMode,
    ) -> Result<WriteResponse, Box<dyn Error>> {
        let table = self.table(store_name, table_name, None, None).await?;
        let table_new = DeltaOps(table)
            .write(record_batches)
            .with_save_mode(save_mode)
            .await?;

        Ok(WriteResponse {
            table: table_name.to_string(),
            version: table_new.version(),
        })
    }

    async fn write(
        &self,
        store_name: &String,
        table_name: &String,
        write_request: WriteRequest,
        save_mode: SaveMode,
    ) -> Result<WriteResponse, Box<dyn Error>> {
        let table = self.table(store_name, table_name, None, None).await?;

        let table_schema = table.schema().ok_or(Box::new(DeltaError::InvalidSchema))?;

        let mut batch_fields: Vec<Field> = Vec::new();
        let mut batch_data: Vec<ArrayRef> = Vec::new();

        let data = if let Some(d) = write_request.record_batch_dict {
            d
        } else if let Some(d) = write_request.record_batch_list {
            let mut new_data: HashMap<String, Vec<EntityValue>> = HashMap::new();
            let _ = table_schema.get_fields().iter().try_for_each(
                |f| -> Result<(), Box<dyn Error>> {
                    new_data.insert(f.get_name().to_string(), Vec::new());
                    Ok(())
                },
            )?;
            d.iter().try_for_each(|v| -> Result<(), Box<dyn Error>> {
                v.iter().try_for_each(|kv| -> Result<(), Box<dyn Error>> {
                    let key = kv.0;
                    let val = kv.1.to_owned();
                    new_data
                        .get_mut(key)
                        .ok_or(Box::new(DeltaError::WriteBatchDataError))?
                        .push(val);
                    Ok(())
                })?;
                Ok(())
            })?;
            new_data
        } else {
            return Err(Box::new(DeltaError::WriteBatchDataError));
        };

        let _ =
            table_schema.get_fields().iter().try_for_each(
                |field| -> Result<(), Box<dyn Error>> {
                    let field_name = field.get_name();
                    let values = data.get(field_name).ok_or(Box::new(
                        DeltaError::WriteBatchNoColumnError(field_name.to_string()),
                    ))?;
                    let is_nullable = field.is_nullable();
                    let data_type = self.clone().map_primitive(field.get_type())?;
                    convert_values(&data_type, values, &mut batch_data)?;
                    batch_fields.push(Field::new(field_name, data_type, is_nullable));
                    Ok(())
                },
            )?;

        let batch_schema = Arc::new(ArrowSchema::new(batch_fields));
        let batch = RecordBatch::try_new(batch_schema, batch_data)?;

        let table_new = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(save_mode)
            .await?;

        Ok(WriteResponse {
            table: table_name.to_string(),
            version: table_new.version(),
        })
    }
}

fn type_mapper<U>(
    values: &Vec<EntityValue>,
    mapper: impl Fn(&EntityValue) -> crate::common::Result<U>,
) -> Result<Vec<U>, Box<dyn Error>> {
    let arr = values
        .iter()
        .map(|x| -> crate::common::Result<U> { mapper(x) })
        .collect::<crate::common::Result<Vec<U>>>()?;
    Ok(arr)
}

fn convert_values(
    data_type: &DataType,
    values: &Vec<EntityValue>,
    arrays: &mut Vec<ArrayRef>,
) -> Result<(), Box<dyn Error>> {
    match data_type {
        DataType::Utf8 => {
            let arr = type_mapper(values, crate::common::map_string)?;
            arrays.push(Arc::new(StringArray::from(arr)));
        }
        DataType::Int64 => {
            let arr = type_mapper(values, crate::common::map_i64)?;
            arrays.push(Arc::new(Int64Array::from(arr)));
        }
        DataType::Int32 => {
            let arr = type_mapper(values, crate::common::map_i32)?;
            arrays.push(Arc::new(Int32Array::from(arr)));
        }
        DataType::Int16 => {
            let arr = type_mapper(values, crate::common::map_i16)?;
            arrays.push(Arc::new(Int16Array::from(arr)));
        }
        DataType::Int8 => {
            let arr = type_mapper(values, crate::common::map_i8)?;
            arrays.push(Arc::new(Int8Array::from(arr)));
        }
        DataType::Float32 => {
            let arr = type_mapper(values, crate::common::map_f32)?;
            arrays.push(Arc::new(Float32Array::from(arr)));
        }
        DataType::Float64 => {
            let arr = type_mapper(values, crate::common::map_f64)?;
            arrays.push(Arc::new(Float64Array::from(arr)));
        }
        DataType::Boolean => {
            let arr = type_mapper(values, crate::common::map_bool)?;
            arrays.push(Arc::new(BooleanArray::from(arr)));
        }
        DataType::Binary => {
            let arr = type_mapper(values, crate::common::map_binary)?;
            arrays.push(Arc::new(BinaryArray::from_iter_values(arr)));
        }
        DataType::Date32 => {
            let arr = type_mapper(values, crate::common::map_i32)?;
            arrays.push(Arc::new(Date32Array::from_iter_values(arr)));
        }
        DataType::Date64 => {
            let arr = type_mapper(values, crate::common::map_i64)?;
            arrays.push(Arc::new(Date64Array::from_iter_values(arr)));
        }
        _ => return Err(Box::new(DeltaError::TypeConversionError)),
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::common::EntityValue;
    use crate::delta::test_delta_util::{create_delta, create_manager, drop_delta};
    use crate::delta::DeltaWrite;
    use crate::models::WriteRequest;
    use deltalake::action::SaveMode;
    use std::collections::HashMap;
    use std::error::Error;

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
}
