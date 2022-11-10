use crate::config::MLConfig;
use crate::models::MLModel;
use catboost;
use yummy_core::encoding::EntityValue;

fn sigmoid(x: f64) -> f64 {
    1. / (1. + (-x).exp())
}

pub struct CatboostModel {
    pub model: catboost::Model,
}

impl CatboostModel {
    pub fn new(config: MLConfig) -> CatboostModel {
        let model_data = match config.flavors.catboost {
            Some(c) => c.data,
            _ => panic!("Wrong catboost config")
        };
        
        let model_path = format!(
            "{}/{}",
            config.base_path.unwrap(),
            model_data
        );
        let model = catboost::Model::load(model_path).unwrap();
        CatboostModel { model }
    }
}

impl MLModel for CatboostModel {

    fn predict(&self, _columns: Vec<String>, data: Vec<Vec<EntityValue>>) -> Vec<Vec<f64>>{
        let mut numeric_features: Vec<Vec<f32>> = Vec::new();
        let mut categorical_features: Vec<Vec<String>> = Vec::new();

        for f in data {
            let mut num: Vec<f32> = Vec::new();
            let mut cat: Vec<String> = Vec::new();
            f.iter().for_each(|x| {
                match x {
                    EntityValue::INT32(v) => num.push(v.to_owned() as f32),
                    EntityValue::INT64(v) => num.push(v.to_owned() as f32),
                    EntityValue::FLOAT32(v) => num.push(v.to_owned() as f32),
                    EntityValue::FLOAT64(v) => num.push(v.to_owned() as f32),
                    EntityValue::BOOL(v) => num.push(v.to_owned() as i32 as f32),
                    EntityValue::STRING(v) => cat.push(v.to_owned()),
                    _ => panic!("Can't convert type"),
                };
            });

            numeric_features.push(num);
            categorical_features.push(cat);
        }

        let predictions = self
            .model
            .calc_model_prediction(numeric_features, categorical_features)
            .unwrap();

        predictions.iter().map(|x| vec![sigmoid(x.to_owned())]).collect()
    }
}

#[test]
fn load_model_and_predict() {
    let path = "../tests/mlflow/catboost_model/my_model".to_string();
    //let path = "../tests/mlflow/catboost_model/iris_my_model".to_string();
    let config = MLConfig::new(&path);
    println!("{:?}", config);
    let catboost_model = CatboostModel::new(config);

    let mut columns = Vec::new();
    let mut data = Vec::new();


    columns.push("age".to_string());
    columns.push("workclass".to_string());
    columns.push("fnlwgt".to_string());
    columns.push("education".to_string());
    columns.push("education-num".to_string());
    columns.push("marital-status".to_string());
    columns.push("occupation".to_string());
    columns.push("relationship".to_string());
    columns.push("race".to_string());
    columns.push("sex".to_string());
    columns.push("capital-gain".to_string());
    columns.push("capital-loss".to_string());
    columns.push("hours-per-week".to_string());
    columns.push("native-country".to_string());

    let mut d = Vec::new();
    d.push(EntityValue::FLOAT32(25.));
    d.push(EntityValue::STRING("Private".to_string()));
    d.push(EntityValue::FLOAT32(226_802.));
    d.push(EntityValue::STRING("11th".to_string()));
    d.push(EntityValue::FLOAT32(7.));
    d.push(EntityValue::STRING("Never-married".to_string()));
    d.push(EntityValue::STRING("Machine-op-inspct".to_string()));
    d.push(EntityValue::STRING("Own-child".to_string()));
    d.push(EntityValue::STRING("Black".to_string()));
    d.push(EntityValue::STRING("Male".to_string()));
    d.push(EntityValue::FLOAT32(0.));
    d.push(EntityValue::FLOAT32(0.));
    d.push(EntityValue::FLOAT32(40.));
    d.push(EntityValue::STRING("United-States".to_string()));

    data.push(d);
    println!("{:?}",data);

    let predictions = catboost_model.predict(columns, data);

    println!("{:?}", predictions);
}
