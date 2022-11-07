pub mod catboost_model;
pub mod lightgbm_model;
use crate::config::MLConfig;
use catboost_model::CatboostModel;
use lightgbm_model::LightgbmModel;
use yummy_core::encoding::EntityValue;

pub trait MLModel {
    fn predict(&self, columns: Vec<String>, data: Vec<Vec<EntityValue>>) -> Vec<Vec<f64>>;
}

pub struct MLModelFactory {}

impl MLModelFactory {
    pub fn new(config: MLConfig) -> Box<dyn MLModel> {
        let ml_model: Box<dyn MLModel> = if let Some(ref _config) = config.flavors.catboost {
            Box::new(CatboostModel::new(config))
        } else if let Some(ref _config) = config.flavors.lightgbm {
            Box::new(LightgbmModel::new(config))
        } else {
            panic!("Wrong model type");
        };

        ml_model
    }
}
