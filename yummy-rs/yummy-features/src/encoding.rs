use crate::types::{EntityKey, Value};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use yummy_core::common::EntityValue;
use yummy_core::encoding::mmh3;

#[derive(thiserror::Error, Debug)]
pub enum EncodingError {
    #[error("Wrong Entity value.")]
    WrongEntityValue,
}

pub fn serialize_key(
    project_name: String,
    entity_key: EntityKey::EntityKey,
    serialization_version: i32,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let sorted_keys = entity_key.join_keys;
    let sorted_values = entity_key.entity_values;

    let mut wtr = vec![];

    for k in sorted_keys {
        wtr.write_u32::<LittleEndian>(Value::value_type::Enum::STRING as u32)?;
        wtr.append(&mut k.as_bytes().to_vec());
    }

    for v in sorted_values {
        let mut v_vec = Vec::new();

        if v.has_int32_val() {
            wtr.write_u32::<LittleEndian>(Value::value_type::Enum::INT32 as u32)?;
            v_vec = LittleEndian::read_i32(&v.int32_val().to_be_bytes())
                .to_be_bytes()
                .to_vec();
        } else if v.has_string_val() {
            wtr.write_u32::<LittleEndian>(Value::value_type::Enum::STRING as u32)?;
            v_vec = v.string_val().as_bytes().to_vec();
        } else if v.has_int64_val() {
            wtr.write_u32::<LittleEndian>(Value::value_type::Enum::INT64 as u32)?;
            if serialization_version <= 1 {
                v_vec = LittleEndian::read_i32(&(v.int64_val() as i32).to_be_bytes())
                    .to_be_bytes()
                    .to_vec();
            } else {
                v_vec = LittleEndian::read_i64(&v.int64_val().to_be_bytes())
                    .to_be_bytes()
                    .to_vec();
            }
        }

        wtr.write_u32::<LittleEndian>(v_vec.len() as u32)?;
        wtr.append(&mut v_vec);
    }

    wtr.append(&mut project_name.as_bytes().to_vec());
    Ok(wtr)
}

pub fn serialize_fields(fields: Vec<String>) -> Vec<Vec<u8>> {
    fields.into_iter().map(|x| mmh3(format!("{x}"))).collect()
}

pub fn serialize_entity_keys(
    project_name: String,
    join_keys: &[String],
    entities: &HashMap<String, Vec<EntityValue>>,
    serialization_version: i32,
) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let n_keys: usize = entities[&join_keys[0]].len();
    let entity_keys: Vec<Vec<u8>> = (0..n_keys)
        .into_iter()
        .map(|i| {
            let entity_values = join_keys
                .to_owned()
                .into_iter()
                .map(|k| {
                    let sf_v = protobuf::SpecialFields::new();
                    let ev = match &entities[&k][i] {
                        EntityValue::INT64(v) => Ok(Value::value::Val::Int64Val(v.clone())),
                        EntityValue::INT32(v) => Ok(Value::value::Val::Int32Val(v.clone())),
                        EntityValue::INT16(v) => Ok(Value::value::Val::Int32Val(v.clone() as i32)),
                        EntityValue::INT8(v) => Ok(Value::value::Val::Int32Val(v.clone() as i32)),
                        EntityValue::FLOAT32(v) => Ok(Value::value::Val::FloatVal(v.clone())),
                        EntityValue::FLOAT64(v) => Ok(Value::value::Val::DoubleVal(v.clone())),
                        EntityValue::BOOL(v) => Ok(Value::value::Val::BoolVal(v.clone())),
                        EntityValue::STRING(v) => Ok(Value::value::Val::StringVal(v.clone())),
                        EntityValue::BYTES(v) => Ok(Value::value::Val::BytesVal(v.clone())),
                        EntityValue::None => Err(Box::new(EncodingError::WrongEntityValue)),
                    };

                    let val = Value::Value {
                        val: Some(ev?),
                        special_fields: sf_v,
                    };

                    Ok(val)
                })
                .collect::<Result<Vec<Value::Value>, Box<dyn Error>>>()?;

            let sf = protobuf::SpecialFields::new();
            let entity_key = EntityKey::EntityKey {
                join_keys: join_keys.to_owned(),
                entity_values,
                special_fields: sf,
            };

            let wtr = serialize_key(project_name.clone(), entity_key, serialization_version)?;

            Ok(wtr)
        })
        .collect::<Result<Vec<Vec<u8>>, Box<dyn Error>>>()?;

    Ok(entity_keys)
}

pub fn parse_value(v: Value::Value) -> EntityValue {
    if v.has_bool_val() {
        return EntityValue::BOOL(v.bool_val());
    } else if v.has_int32_val() {
        return EntityValue::INT32(v.int32_val());
    } else if v.has_int64_val() {
        return EntityValue::INT64(v.int64_val());
    } else if v.has_float_val() {
        return EntityValue::FLOAT32(v.float_val());
    } else if v.has_double_val() {
        return EntityValue::FLOAT64(v.double_val());
    } else if v.has_string_val() {
        return EntityValue::STRING(v.string_val().to_string());
    } else if v.has_bytes_val() {
        return EntityValue::BYTES(v.bytes_val().to_vec());
    }

    EntityValue::None
}

#[test]
fn test_murmur3() {
    //let sf = protobuf::SpecialFields::new();
    //let ev = Value::value::Val::Int32Val(186);
    //let val = Value::Value {
    //    val: Some(ev),
    //    special_fields: sf.clone(),
    //};

    //let val = ValueProto(int32_val=186);
    //let kp = EntityKey::EntityKey {
    //    join_keys: vec!["entity_id".to_string()],
    //    entity_values: vec![val],
    //    special_fields: sf.clone(),
    //};

    //let bb = Value::value_type::Enum::STRING.to_string().as_bytes();
    //let bb = "2".as_bytes();
    //let kpp = LittleEndian::read_u32(&bb).to_be_bytes();
    //println!("{:?}", &kpp);
    //assert_eq!(&kpp, b"\x02\x00\x00\x00");
    //assert_eq!(kpp, b"\x02\x00\x00\x00entity_id\x03\x00\x00\x00\x04\x00\x00\x00\xba\x00\x00\x00");

    //let kp = EntityKey(join_keys=vec!["entity_id"], entity_values=vec![ev]);

    let key = "test";
    let hash = murmur3::hash32(key).to_be_bytes();
    println!("{hash:?}");
    assert_eq!(hash, [186, 107, 210, 19]);
    //let val = LittleEndian::read_u32(&hash).to_be_bytes().to_vec();
}
