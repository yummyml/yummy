use pyo3::prelude::*;
use yummy_delta::{apply_delta, run_delta_server};

#[pyfunction]
fn run_apply(config_path: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(apply_delta(config_path))
        .unwrap();
    Ok("Ok".to_string())
}

#[pyfunction]
fn run(config_path: String, host: String, port: u16, log_level: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_delta_server(config_path, host, port, log_level))
        .unwrap();
    Ok("Ok".to_string())
}

#[pymodule]
fn yummy_delta(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(run_apply, m)?)?;

    Ok(())
}
