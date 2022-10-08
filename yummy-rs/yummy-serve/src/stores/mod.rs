pub mod redis_store;
use crate::stores::redis_store::RedisOnlineStore;
use async_trait::async_trait;
use yummy_core::config::Config;

#[async_trait]
pub trait OnlineStore {
    async fn get_online_features(
        &self,
        keys: Vec<Vec<u8>>,
        fields: Vec<Vec<u8>>,
    ) -> Vec<Vec<Vec<u8>>>;
}

pub struct OnlineStoreFactory {}

impl OnlineStoreFactory {
    pub fn new(config: Config) -> Box<dyn OnlineStore> {
        let online_store: Box<dyn OnlineStore> = match config.online_store.store_type.as_str() {
            "redis" => Box::new(RedisOnlineStore::new(config)),
            _ => panic!("OnlineStore not implemented"),
        };

        online_store
    }
}
