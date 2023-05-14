use yummy_ml::config::MLConfig;
use std::error::Error;
use yummy_ml::models::lightgbm_model::LightgbmModel;
use yummy_core::common::EntityValue;
use yummy_ml::models::MLModel;

#[tokio::test]
async fn load_model_and_predict() -> Result<(), Box<dyn Error>> {
    let path = "../tests/mlflow/lightgbm_model/lightgbm_my_model".to_string();
    //let path = "../tests/mlflow/catboost_model/iris_my_model".to_string();
    let config = MLConfig::new(&path).await?;
    println!("{config:?}");
    let lgb_model = LightgbmModel::new(config)?;

    let mut columns = Vec::new();
    let mut data = Vec::new();

    columns.push("".to_string());

    let mut d1 = Vec::new();
    d1.push(EntityValue::INT32(8));
    data.push(d1);

    let mut d2 = Vec::new();
    d2.push(EntityValue::INT32(2));
    data.push(d2);

    println!("{data:?}");

    let predictions = lgb_model.predict(columns, data)?;

    println!("{predictions:?}");
    Ok(())
}

#[tokio::test]
async fn load_model_and_predict_multiclass() -> Result<(), Box<dyn Error>> {
    let path = "../tests/mlflow/lightgbm_model/lightgbm_wine_model/".to_string();
    //let path = "../tests/mlflow/catboost_model/iris_my_model".to_string();
    let config = MLConfig::new(&path).await?;
    println!("{config:?}");
    let lgb_model = LightgbmModel::new(config)?;

    let mut columns = Vec::new();
    let mut data = Vec::new();

    columns.push("12".to_string());
    columns.push("1".to_string());
    columns.push("2".to_string());
    columns.push("3".to_string());
    columns.push("4".to_string());
    columns.push("5".to_string());
    columns.push("6".to_string());
    columns.push("7".to_string());
    columns.push("8".to_string());
    columns.push("9".to_string());
    columns.push("10".to_string());
    columns.push("11".to_string());
    columns.push("0".to_string());

    let mut d1 = Vec::new();
    d1.push(EntityValue::FLOAT32(0.997086));
    d1.push(EntityValue::FLOAT32(-0.598156));
    d1.push(EntityValue::FLOAT32(-0.425909));
    d1.push(EntityValue::FLOAT32(-0.929365));
    d1.push(EntityValue::FLOAT32(1.281985));
    d1.push(EntityValue::FLOAT32(0.488531));
    d1.push(EntityValue::FLOAT32(0.874184));
    d1.push(EntityValue::FLOAT32(-1.223610));
    d1.push(EntityValue::FLOAT32(0.050988));
    d1.push(EntityValue::FLOAT32(0.342557));
    d1.push(EntityValue::FLOAT32(-0.164303));
    d1.push(EntityValue::FLOAT32(0.830961));
    d1.push(EntityValue::FLOAT32(0.913333));
    data.push(d1.clone());
    data.push(d1.clone());

    println!("{data:?}");

    let predictions = lgb_model.predict(columns, data);

    println!("{predictions:?}");
    Ok(())
}
