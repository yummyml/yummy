use crate::common::reorder;
use crate::config::MLConfig;
use crate::models::MLModel;
use lightgbm;
use std::error::Error;
use yummy_core::common::EntityValue;

#[derive(thiserror::Error, Debug)]
pub enum LightgbmError {
    #[error("Wrong catboost config")]
    WrongConfig,

    #[error("Please provide numeric or categorical features")]
    ValidationNoFeatures,

    #[error("Wrong number of numeric features (required {0})")]
    ValidationWrongNumericFeatures(i32),

    #[error("Can't convert type")]
    TypeConversionError,
}

pub struct LightgbmModel {
    pub model: lightgbm::Booster,
    pub feature_names: Vec<String>,
    pub num_features: i32,
}

impl LightgbmModel {
    pub fn new(config: MLConfig) -> Result<LightgbmModel, Box<dyn Error>> {
        let model_data = match config.flavors.lightgbm {
            Some(c) => c.data,
            _ => return Err(Box::new(LightgbmError::WrongConfig)),
        };

        let model_path = format!("{}/{}", config.base_path.unwrap(), model_data);

        let model = lightgbm::Booster::from_file(model_path.as_str())?;
        let feature_names = model.feature_name()?;
        let num_features = model.num_feature()?;

        Ok(LightgbmModel {
            model,
            feature_names,
            num_features,
        })
    }

    fn validate(&self, numeric_features: &Vec<Vec<f64>>) -> Result<(), Box<dyn Error>> {
        if numeric_features.is_empty() {
            return Err(Box::new(LightgbmError::ValidationNoFeatures));
        }

        if numeric_features.first().unwrap().len() != self.num_features as usize {
            return Err(Box::new(LightgbmError::ValidationWrongNumericFeatures(
                self.num_features,
            )));
        }

        Ok(())
    }
}

impl MLModel for LightgbmModel {
    fn predict(
        &self,
        columns: Vec<String>,
        data: Vec<Vec<EntityValue>>,
    ) -> Result<Vec<Vec<f64>>, Box<dyn Error>> {
        let mut numeric_features: Vec<Vec<f64>> = Vec::new();
        let mut categorical_features: Vec<Vec<String>> = Vec::new();

        let num = data.len();
        for f in data {
            let mut num: Vec<f64> = Vec::new();
            let mut cat: Vec<String> = Vec::new();
            f.iter().try_for_each(|x| -> Result<(), Box<dyn Error>> {
                match x {
                    EntityValue::INT32(v) => {
                        num.push(v.to_owned() as f64);
                        Ok(())
                    }
                    EntityValue::INT64(v) => {
                        num.push(v.to_owned() as f64);
                        Ok(())
                    }
                    EntityValue::FLOAT32(v) => {
                        num.push(v.to_owned() as f64);
                        Ok(())
                    }
                    EntityValue::FLOAT64(v) => {
                        num.push(v.to_owned());
                        Ok(())
                    }
                    EntityValue::BOOL(v) => {
                        num.push(v.to_owned() as i32 as f64);
                        Ok(())
                    }
                    EntityValue::STRING(v) => {
                        cat.push(v.to_owned());
                        Ok(())
                    }
                    _ => Err(Box::new(LightgbmError::TypeConversionError)),
                }?;
                Ok(())
            })?;

            numeric_features.push(num);
            categorical_features.push(cat);
        }

        self.validate(&numeric_features)?;

        if !columns.is_empty() {
            numeric_features = reorder(&self.feature_names, columns, numeric_features)?;
        }

        let predictions = self.model.predict(numeric_features)?;

        let num_pred = (predictions[0]).len();
        if num_pred == num {
            Ok(predictions[0].iter().map(|x| vec![x.to_owned()]).collect())
        } else {
            Ok(predictions
                .iter()
                .map(|x| x.iter().map(|v| v.to_owned()).collect())
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_feature_names() -> Result<(), Box<dyn Error>> {
        let path = "../tests/mlflow/lightgbm_model/lightgbm_wine_model".to_string();
        //let path = "../tests/mlflow/catboost_model/iris_my_model".to_string();
        let config = MLConfig::new(&path).await?;
        println!("{config:?}");
        let lgb_model = LightgbmModel::new(config)?;
        let features = lgb_model.model.feature_name()?;
        let nfeatures = lgb_model.model.num_feature();
        println!("{nfeatures:?}");
        println!("{features:?}");
        Ok(())
    }
}
