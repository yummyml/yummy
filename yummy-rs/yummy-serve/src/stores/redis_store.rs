use crate::stores::OnlineStore;
use async_trait::async_trait;
use r2d2::Pool;
use redis::{pipe, Client};
use yummy_core::config::Config;

#[derive(Clone)]
pub struct RedisOnlineStore {
    pub pool: Pool<Client>,
}

impl RedisOnlineStore {
    pub fn new(config: Config) -> RedisOnlineStore {
        let redis_client = redis::Client::open(config.online_store.connection_string).unwrap();
        let pool = r2d2::Pool::new(redis_client).unwrap();
        RedisOnlineStore { pool: pool }
    }
}

#[async_trait]
impl OnlineStore for RedisOnlineStore {
    async fn get_online_features(
        &self,
        keys: Vec<Vec<u8>>,
        fields: Vec<Vec<u8>>,
    ) -> Vec<Vec<Vec<u8>>> {
        let mut con = self.pool.get().unwrap();
        //let r: Vec<Vec<u8>> = cmd("HMGET").arg(key.clone()).arg(fields.clone()).query(&mut *con).unwrap();
        let mut p = pipe();
        for key in keys {
            p = p.cmd("HMGET").arg(key).arg(fields.clone()).clone();
        }
        let rr: Vec<Vec<Vec<u8>>> = p.query(&mut *con).unwrap();
        rr
    }
}
