use pyo3::prelude::*;
use yummy_serve::serve_wrapper;
use yummy_mlflow::serve_mlflow_model;


#[pyfunction]
fn serve(config_path: String, host: String, port: u16, log_level: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(serve_wrapper(config_path, host, port, log_level))
        .unwrap();
    Ok("Ok".to_string())
}

#[pyfunction]
fn model_serve(model_path: String, host: String, port: u16, log_level: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(serve_mlflow_model(model_path, host, port, log_level))
        .unwrap();
    Ok("Ok".to_string())
}


#[pymodule]
fn yummy_rs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(serve, m)?)?;

    Ok(())
}

