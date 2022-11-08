
#[actix_web::main]
async fn main()  -> std::io::Result<()> {

    //let mlmodel_path = "../tests/mlflow/catboost_model/my_model".to_string();
    let mlmodel_path = "../tests/mlflow/lightgbm_model/lightgbm_my_model".to_string();

    yummy_mlflow::serve_mlflow_model(
        mlmodel_path,
        "0.0.0.0".to_string(),
        8080,
        "Debug".to_string(),
    ).await
}
