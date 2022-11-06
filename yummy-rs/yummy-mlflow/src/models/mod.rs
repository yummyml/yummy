pub mod catboost_model;
use crate::config::MLConfig;
use catboost_model::CatboostModel;
use yummy_core::encoding::EntityValue;

pub trait MLModel {
    fn predict(&self, columns: Vec<String>, data: Vec<Vec<EntityValue>>) -> Vec<f64>;
}

pub struct MLModelFactory {}

impl MLModelFactory {
    pub fn new(config: MLConfig) -> Box<dyn MLModel> {
        //let online_store: Box<dyn MLModel> = match config.online_store.store_type.as_str() {
        //    "redis" => Box::new(RedisOnlineStore::new(config)),
        //    _ => panic!("OnlineStore not implemented"),
        //};

        let online_store = Box::new(CatboostModel::new(config));
        online_store
    }
}
