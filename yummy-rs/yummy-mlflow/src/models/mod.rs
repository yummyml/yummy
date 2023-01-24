pub mod catboost_model;
pub mod lightgbm_model;
use crate::common::EntityValue;
use crate::config::MLConfig;
use catboost_model::CatboostModel;
use lightgbm_model::LightgbmModel;
use std::error::Error;

#[derive(thiserror::Error, Debug)]
pub enum ModelFactoryError {
    #[error("Wrong model type")]
    WrongModelType,
}

pub trait MLModel {
    fn predict(
        &self,
        columns: Vec<String>,
        data: Vec<Vec<EntityValue>>,
    ) -> Result<Vec<Vec<f64>>, Box<dyn Error>>;
}

pub struct MLModelFactory {}

impl MLModelFactory {
    pub fn new(config: MLConfig) -> Result<Box<dyn MLModel>, Box<dyn Error>> {
        let ml_model: Box<dyn MLModel> = if let Some(ref _config) = config.flavors.catboost {
            Box::new(CatboostModel::new(config)?)
        } else if let Some(ref _config) = config.flavors.lightgbm {
            Box::new(LightgbmModel::new(config)?)
        } else {
            return Err(Box::new(ModelFactoryError::WrongModelType));
        };

        Ok(ml_model)
    }
}
