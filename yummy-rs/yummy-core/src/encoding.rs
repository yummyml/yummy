use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use fasthash::murmur3;
use serde::{Deserialize, Serialize};

pub fn mmh3(key: String) -> Vec<u8> {
    let hash = murmur3::hash32(key).to_be_bytes();
    LittleEndian::read_u32(&hash).to_be_bytes().to_vec()
}

#[test]
fn test_murmur3() {
    let key = "test";
    let hash = murmur3::hash32(key).to_be_bytes();
    println!("{hash:?}");
    assert_eq!(hash, [186, 107, 210, 19]);
}
