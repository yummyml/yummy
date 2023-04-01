#[cfg(feature = "catboost")]
pub mod catboost_model;
pub mod lightgbm_model;
use crate::config::MLConfig;
#[cfg(feature = "catboost")]
use catboost_model::CatboostModel;
use lightgbm_model::LightgbmModel;
use std::error::Error;
use yummy_core::common::EntityValue;

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
        let ml_model: Box<dyn MLModel> = if let Some(ref _config) = config.flavors.lightgbm {
            Box::new(LightgbmModel::new(config)?)
        } else if let Some(ref _config) = config.flavors.catboost {
            catboost_model(config)?
        } else {
            return Err(Box::new(ModelFactoryError::WrongModelType));
        };

        Ok(ml_model)
    }
}

#[cfg(feature = "catboost")]
fn catboost_model(config: MLConfig) -> Result<Box<dyn MLModel>, Box<dyn Error>> {
    Box::new(CatboostModel::new(config)?)
}

#[cfg(not(feature = "catboost"))]
fn catboost_model(config: MLConfig) -> Result<Box<dyn MLModel>, Box<dyn Error>> {
    return Err(Box::new(ModelFactoryError::WrongModelType));
}
