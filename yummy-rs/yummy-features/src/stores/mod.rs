pub mod redis_store;
use crate::config::Config;
use crate::stores::redis_store::RedisOnlineStore;
use async_trait::async_trait;
use std::error::Error;

#[derive(thiserror::Error, Debug)]
pub enum OnlineStoreFactoryError {
    #[error("Wrong online store type")]
    WrongStoreType,
}

#[async_trait]
pub trait OnlineStore {
    async fn get_online_features(
        &self,
        keys: Vec<Vec<u8>>,
        fields: Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<Vec<u8>>>, Box<dyn Error>>;
}

pub struct OnlineStoreFactory {}

impl OnlineStoreFactory {
    pub fn build(config: Config) -> Result<Box<dyn OnlineStore>, Box<dyn Error>> {
        let online_store: Box<dyn OnlineStore> = match config.online_store.store_type.as_str() {
            "redis" => Box::new(RedisOnlineStore::new(config)?),
            _ => return Err(Box::new(OnlineStoreFactoryError::WrongStoreType)),
        };

        Ok(online_store)
    }
}
