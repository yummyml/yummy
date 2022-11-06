use crate::config::MLConfig;
use crate::models::MLModel;
use lightgbm;
use std::collections::HashMap;
use yummy_core::encoding::EntityValue;

fn sigmoid(x: f64) -> f64 {
    1. / (1. + (-x).exp())
}

pub struct LightgbmModel {
    pub model: lightgbm::Booster
}

impl LightgbmModel {
    pub fn new(config: MLConfig) -> LightgbmModel {
        let model_data = match config.flavors.lightgbm {
            Some(c) => c.data,
            _ => panic!("Wrong lightgbm config")
        };
        
        let model_path = format!(
            "{}/{}",
            config.base_path.unwrap().replace("/MLmodel", ""),
            model_data
        );

        let model = lightgbm::Booster::from_file(model_path.as_str()).unwrap();
        LightgbmModel { model }
    }
}

impl MLModel for LightgbmModel {

    fn predict(&self, columns: Vec<String>, data: Vec<Vec<EntityValue>>) -> Vec<Vec<f64>>{
        let mut numeric_features: Vec<Vec<f64>> = Vec::new();
        let mut categorical_features: Vec<Vec<String>> = Vec::new();

        for f in data {
            let mut num: Vec<f64> = Vec::new();
            let mut cat: Vec<String> = Vec::new();
            f.iter().for_each(|x| {
                match x {
                    EntityValue::INT32(v) => num.push(v.to_owned() as f64),
                    EntityValue::INT64(v) => num.push(v.to_owned() as f64),
                    EntityValue::FLOAT32(v) => num.push(v.to_owned() as f64),
                    EntityValue::FLOAT64(v) => num.push(v.to_owned() as f64),
                    EntityValue::BOOL(v) => num.push(v.to_owned() as i32 as f64),
                    EntityValue::STRING(v) => cat.push(v.to_owned()),
                    _ => panic!("Can't convert type"),
                };
            });

            numeric_features.push(num);
            categorical_features.push(cat);
        }

        println!("{:?}", numeric_features);
        println!("{:?}", categorical_features);


        let predictions = self.model.predict(numeric_features).unwrap();

        predictions.iter().map(|x| x.iter().map(|v| v.to_owned()).collect()).collect()
    }
}

#[test]
fn load_model_and_predict() {
    let path = "../tests/mlflow/lightgbm_model/lightgbm_my_model/MLmodel".to_string();
    //let path = "../tests/mlflow/catboost_model/iris_my_model/MLmodel".to_string();
    let config = MLConfig::new(&path);
    println!("{:?}", config);
    let lgb_model = LightgbmModel::new(config);

    let mut columns = Vec::new();
    let mut data = Vec::new();


    columns.push("age".to_string());

    let mut d1 = Vec::new();
    d1.push(EntityValue::INT32(8));
    data.push(d1);

    let mut d2 = Vec::new();
    d2.push(EntityValue::INT32(2));
    data.push(d2);


    println!("{:?}",data);

    let predictions = lgb_model.predict(columns, data);

    println!("{:?}", predictions);
}
