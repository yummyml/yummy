use regex::Regex;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::env;
use std::error::Error;

pub type Result<T> = core::result::Result<T, Box<dyn Error>>;

#[macro_export]
macro_rules! err {
    ($e:expr) => {
        Box::new($e)
    };
}

/*
pub type Result<T> = anyhow::Result<T>;

macro_rules! err {
    ($e:expr) => {
        anyhow::anyhow!($e)
    };
}
*/

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
    type Error = Box<dyn Error>;

    fn try_from(ev: &EntityValue) -> Result<Self> {
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
            Err(err!(EntityValueError::TypeConversionError))
        }
    }
}

macro_rules! mapev {
    ($name:ident, [$($en:ident),*], $id:ty) => {
        pub fn $name(ev: &EntityValue) -> Result<$id> {
            $(
            if let EntityValue::$en(v) = ev {
                return Ok(v.to_owned() as $id);
            }
            )*

            Err(err!(EntityValueError::TypeConversionError))
        }
    };
}

mapev!(map_string, [STRING], String);
mapev!(map_i64, [INT64, INT32, INT16, INT8], i64);
mapev!(map_i32, [INT64, INT32, INT16, INT8], i32);
mapev!(map_i16, [INT64, INT32, INT16, INT8], i16);
mapev!(map_i8, [INT64, INT32, INT16, INT8], i8);
mapev!(map_f32, [FLOAT32, FLOAT64], f32);
mapev!(map_f64, [FLOAT32, FLOAT64], f64);
mapev!(map_bool, [BOOL], bool);
mapev!(map_binary, [BYTES], Vec<u8>);

#[derive(thiserror::Error, Debug)]
enum ReplaceTokensError {
    #[error("Environment variable: {0} not set")]
    NoEnv(String),
}

pub struct ReplaceTokens {}

impl ReplaceTokens {
    pub fn replace(template: &String) -> Result<String> {
        let mut text = template.clone();
        let tokens = Self::find_tokens(&template)?;
        for token in tokens {
            let from = format!("${{{}}}", &token);
            let to = match env::var(&token) {
                Ok(v) => v,
                Err(error) => return Err(err!(ReplaceTokensError::NoEnv(token.to_string()))),
            };
            text = text.replace(&from, &to);
        }
        Ok(text)
    }

    fn find_tokens(text: &String) -> Result<Vec<&str>> {
        let re = Regex::new(r"\$\{(?P<token>[a-zA-Z0-9_\-]+)\}").unwrap();
        let tokens: Vec<&str> = re
            .captures_iter(text)
            .map(|x| x.name("token").unwrap().as_str())
            .collect();
        Ok(tokens)
    }
}

#[tokio::test]
async fn test_replace_tokens() -> Result<()> {
    env::set_var("Q_1", "TOKEN1");
    env::set_var("Q_2", "TOKEN2");
    let mut text = "${Q_1} ${Q_2}".to_string();
    text = ReplaceTokens::replace(&text)?;
    env::remove_var("Q_1");
    env::remove_var("Q_2");

    assert_eq!(text, "TOKEN1 TOKEN2");

    Ok(())
}
