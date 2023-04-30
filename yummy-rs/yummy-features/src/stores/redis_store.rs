use crate::config::Config;
use crate::stores::OnlineStore;
use async_trait::async_trait;
use r2d2::Pool;
use redis::{pipe, Client};
use std::error::Error;

#[derive(Clone)]
pub struct RedisOnlineStore {
    pub pool: Pool<Client>,
}

impl RedisOnlineStore {
    pub fn new(config: Config) -> Result<RedisOnlineStore, Box<dyn Error>> {
        let connection_string = if config
            .online_store
            .connection_string
            .starts_with("redis://")
        {
            config.online_store.connection_string
        } else {
            format!("redis://{}", config.online_store.connection_string)
        };
        let redis_client = redis::Client::open(connection_string)?;
        let pool = r2d2::Pool::new(redis_client)?;
        Ok(RedisOnlineStore { pool })
    }
}

#[async_trait]
impl OnlineStore for RedisOnlineStore {
    async fn get_online_features(
        &self,
        keys: Vec<Vec<u8>>,
        fields: Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<Vec<u8>>>, Box<dyn Error>> {
        let mut con = self.pool.get()?;
        //let r: Vec<Vec<u8>> = cmd("HMGET").arg(key.clone()).arg(fields.clone()).query(&mut *con)?;
        let mut p = pipe();
        for key in keys {
            p = p.cmd("HMGET").arg(key).arg(fields.clone()).clone();
        }
        let rr: Vec<Vec<Vec<u8>>> = p.query(&mut *con)?;
        Ok(rr)
    }
}
