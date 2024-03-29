use pyo3::prelude::*;
use yummy_ml::serve_ml_model;

#[pyfunction]
fn serve(
    model_path: String,
    host: String,
    port: u16,
    log_level: String,
    workers: usize,
) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(serve_ml_model(
            model_path,
            host,
            port,
            Some(&log_level),
            Some(&workers),
        ))
        .unwrap();
    Ok("Ok".to_string())
}

#[pymodule]
fn yummy_ml(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(serve, m)?)?;

    Ok(())
}
