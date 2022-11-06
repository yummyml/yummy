
#[actix_web::main]
async fn main()  -> std::io::Result<()> {
    yummy_mlflow::serve_mlflow_wrapper(
        "../tests/mlflow/catboost_model/my_model/MLmodel".to_string(),
        "0.0.0.0".to_string(),
        8080,
        "Debug".to_string(),
    ).await
}
