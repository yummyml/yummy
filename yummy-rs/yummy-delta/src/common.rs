use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::error::Error;

pub fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

#[derive(thiserror::Error, Debug)]
pub enum EntityValueError {
    #[error("Can't convert type")]
    TypeConversionError,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
#[derive(Clone)]
pub enum EntityValue {
    None,
    INT64(i64),
    INT32(i32),
    INT16(i16),
    INT8(i8),
    FLOAT32(f32),
    FLOAT64(f64),
    BOOL(bool),
    STRING(String),
    BYTES(Vec<u8>),
}

impl TryFrom<&EntityValue> for String {
    type Error = Box<dyn std::error::Error>;

    fn try_from(ev: &EntityValue) -> Result<Self, Box<dyn Error>> {
        if let EntityValue::INT64(v) = ev {
            Ok(v.to_string())
        } else if let EntityValue::INT32(v) = ev {
            Ok(v.to_string())
        } else if let EntityValue::INT16(v) = ev {
            Ok(v.to_string())
        } else if let EntityValue::INT8(v) = ev {
            Ok(v.to_string())
        } else if let EntityValue::FLOAT32(v) = ev {
            Ok(v.to_string())
        } else if let EntityValue::FLOAT64(v) = ev {
            Ok(v.to_string())
        } else if let EntityValue::STRING(v) = ev {
            Ok(v.to_string())
        } else {
            Err(Box::new(EntityValueError::TypeConversionError))
        }
    }
}

pub fn map_string(ev: &EntityValue) -> Result<String, Box<dyn Error>> {
    if let EntityValue::STRING(v) = ev {
        Ok(v.to_owned())
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}

pub fn map_i64(ev: &EntityValue) -> Result<i64, Box<dyn Error>> {
    if let EntityValue::INT64(v) = ev {
        Ok(v.to_owned() as i64)
    } else if let EntityValue::INT32(v) = ev {
        Ok(v.to_owned() as i64)
    } else if let EntityValue::INT16(v) = ev {
        Ok(v.to_owned() as i64)
    } else if let EntityValue::INT8(v) = ev {
        Ok(v.to_owned() as i64)
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}

pub fn map_i32(ev: &EntityValue) -> Result<i32, Box<dyn Error>> {
    if let EntityValue::INT64(v) = ev {
        Ok(v.to_owned() as i32)
    } else if let EntityValue::INT32(v) = ev {
        Ok(v.to_owned() as i32)
    } else if let EntityValue::INT16(v) = ev {
        Ok(v.to_owned() as i32)
    } else if let EntityValue::INT8(v) = ev {
        Ok(v.to_owned() as i32)
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}

pub fn map_i16(ev: &EntityValue) -> Result<i16, Box<dyn Error>> {
    if let EntityValue::INT64(v) = ev {
        Ok(v.to_owned() as i16)
    } else if let EntityValue::INT32(v) = ev {
        Ok(v.to_owned() as i16)
    } else if let EntityValue::INT16(v) = ev {
        Ok(v.to_owned() as i16)
    } else if let EntityValue::INT8(v) = ev {
        Ok(v.to_owned() as i16)
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}

pub fn map_i8(ev: &EntityValue) -> Result<i8, Box<dyn Error>> {
    if let EntityValue::INT64(v) = ev {
        Ok(v.to_owned() as i8)
    } else if let EntityValue::INT32(v) = ev {
        Ok(v.to_owned() as i8)
    } else if let EntityValue::INT16(v) = ev {
        Ok(v.to_owned() as i8)
    } else if let EntityValue::INT8(v) = ev {
        Ok(v.to_owned() as i8)
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}

pub fn map_f32(ev: &EntityValue) -> Result<f32, Box<dyn Error>> {
    if let EntityValue::FLOAT32(v) = ev {
        Ok(v.to_owned() as f32)
    } else if let EntityValue::FLOAT64(v) = ev {
        Ok(v.to_owned() as f32)
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}

pub fn map_f64(ev: &EntityValue) -> Result<f64, Box<dyn Error>> {
    if let EntityValue::FLOAT32(v) = ev {
        Ok(v.to_owned() as f64)
    } else if let EntityValue::FLOAT64(v) = ev {
        Ok(v.to_owned() as f64)
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}

pub fn map_bool(ev: &EntityValue) -> Result<bool, Box<dyn Error>> {
    if let EntityValue::BOOL(v) = ev {
        Ok(v.to_owned())
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}

pub fn map_binary(ev: &EntityValue) -> Result<Vec<u8>, Box<dyn Error>> {
    if let EntityValue::BYTES(v) = ev {
        Ok(v.to_owned())
    } else {
        Err(Box::new(EntityValueError::TypeConversionError))
    }
}