use pyo3::prelude::*;
use yummy_delta::{apply_delta, pprint_stores, pprint_table, pprint_tables, run_delta_server};

#[pyfunction]
fn run_pprint_stores(config_path: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(pprint_stores(config_path))
        .unwrap();
    Ok("Ok".to_string())
}

#[pyfunction]
fn run_pprint_tables(config_path: String, store_name: String) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(pprint_tables(config_path, store_name))
        .unwrap();
    Ok("Ok".to_string())
}

#[pyfunction]
fn run_pprint_table(
    config_path: String,
    store_name: String,
    table_name: String,
) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(pprint_table(config_path, store_name, table_name))
        .unwrap();
    Ok("Ok".to_string())
}

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
fn run(
    config_path: String,
    host: String,
    port: u16,
    log_level: String,
    workers: usize,
) -> PyResult<String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_delta_server(
            config_path,
            host,
            port,
            Some(&log_level),
            Some(&workers),
        ))
        .unwrap();
    Ok("Ok".to_string())
}

#[pymodule]
fn yummy_delta(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(run_apply, m)?)?;
    m.add_function(wrap_pyfunction!(run_pprint_stores, m)?)?;
    m.add_function(wrap_pyfunction!(run_pprint_tables, m)?)?;
    m.add_function(wrap_pyfunction!(run_pprint_table, m)?)?;

    Ok(())
}
