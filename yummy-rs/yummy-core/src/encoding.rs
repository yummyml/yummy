use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use fasthash::murmur3;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
#[derive(Clone)]
pub enum EntityValue {
    None,
    INT64(i64),
    INT32(i32),
    FLOAT32(f32),
    FLOAT64(f64),
    BOOL(bool),
    STRING(String),
    BYTES(Vec<u8>),
}

pub fn mmh3(key: String) -> Vec<u8> {
    let hash = murmur3::hash32(key).to_be_bytes();
    LittleEndian::read_u32(&hash).to_be_bytes().to_vec()
}

#[test]
fn test_murmur3() {
    let key = "test";
    let hash = murmur3::hash32(key).to_be_bytes();
    println!("{:?}", hash);
    assert_eq!(hash, [186, 107, 210, 19]);
}
