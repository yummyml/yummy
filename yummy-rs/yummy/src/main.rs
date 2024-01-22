use clap::{arg, Command};

fn cli() -> Command {
    Command::new("yummy")
        .about("yummy package")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("delta")
                .about("yummy delta")
                .subcommand_required(true)
                .subcommand(
                    Command::new("apply")
                        .about("yummy delta apply")
                        .args(vec![arg!(-f --filename <FILE> "Apply config file")])
                        .arg_required_else_help(true),
                )
                .subcommand(
                    Command::new("serve")
                        .about("yummy delta serve")
                        .args(vec![
                            arg!(--config <CONFIG> "config file"),
                            arg!(--host <HOST> "host"),
                            arg!(--port <PORT> "port"),
                            arg!(--loglevel <LOGLEVEL> "log level"),
                        ])
                        .arg_required_else_help(true),
                ),
        )
        .subcommand(
            Command::new("ml")
                .about("yummy ml")
                .subcommand_required(true)
                .subcommand(
                    Command::new("serve")
                        .about("yummy ml serve")
                        .args(vec![
                            arg!(--model <FILE> "model path"),
                            arg!(--host <HOST> "host"),
                            arg!(--port <PORT> "port"),
                            arg!(--loglevel <LOGLEVEL> "log level"),
                        ])
                        .arg_required_else_help(true),
                ),
        )
        .subcommand(
            Command::new("llm")
                .about("yummy llm")
                .subcommand_required(true)
                .subcommand(
                    Command::new("serve")
                        .about("yummy llm serve")
                        .args(vec![
                            arg!(--config <FILE>)
                                .required(true)
                                .help("config file path"),
                            arg!(--host <HOST> "host")
                                .required(false)
                                .help("host")
                                .default_value("0.0.0.0"),
                            arg!(--port <PORT> "port")
                                .required(false)
                                .help("port")
                                .default_value("8080")
                                .value_parser(clap::value_parser!(u16)),
                            arg!(--loglevel <LOGLEVEL>)
                                .required(false)
                                .help("log level")
                                .default_value("error"),
                            arg!(--workers <WORKERS>)
                                .required(false)
                                .help("number of workers")
                                .default_value("2")
                                .value_parser(clap::value_parser!(usize)),
                        ])
                        .arg_required_else_help(true),
                ),
        )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("delta", sub_matches)) => match sub_matches.subcommand() {
            Some(("apply", sub_sub_matches)) => {
                let file = sub_sub_matches
                    .get_one::<String>("filename")
                    .expect("required");
                delta_apply(file.clone()).await?
            }
            Some(("serve", sub_sub_matches)) => {
                let config = sub_sub_matches
                    .get_one::<String>("config")
                    .expect("required");
                let host = sub_sub_matches.get_one::<String>("host").expect("required");
                let port = sub_sub_matches
                    .get_one::<String>("port")
                    .expect("required")
                    .parse::<u16>()?;

                let log_level = sub_sub_matches
                    .get_one::<String>("loglevel")
                    .expect("required");

                delta_serve(config.clone(), host.clone(), port, log_level.clone()).await?
            }
            _ => unreachable!(),
        },
        Some(("ml", sub_matches)) => match sub_matches.subcommand() {
            Some(("serve", sub_sub_matches)) => {
                let model = sub_sub_matches
                    .get_one::<String>("model")
                    .expect("required");
                let host = sub_sub_matches.get_one::<String>("host").expect("required");
                let port = sub_sub_matches
                    .get_one::<String>("port")
                    .expect("required")
                    .parse::<u16>()?;

                let log_level = sub_sub_matches
                    .get_one::<String>("loglevel")
                    .expect("required");

                ml_serve(model.clone(), host.clone(), port, log_level.clone()).await?
            }
            _ => unreachable!(),
        },
        Some(("llm", sub_matches)) => match sub_matches.subcommand() {
            Some(("serve", sub_sub_matches)) => {
                let config = sub_sub_matches
                    .get_one::<String>("config")
                    .expect("required");
                let host = sub_sub_matches.get_one::<String>("host").expect("required");
                let port = sub_sub_matches.get_one::<u16>("port").expect("required");

                let log_level = sub_sub_matches.get_one::<String>("loglevel");
                let workers = sub_sub_matches.get_one::<usize>("workers");

                llm_serve(config.clone(), host.clone(), *port, log_level, workers).await?
            }
            _ => unreachable!(),
        },

        _ => unreachable!(),
    }

    Ok(())
}

#[cfg(feature = "yummy-delta")]
async fn delta_apply(file: String) -> std::io::Result<()> {
    yummy_delta::apply_delta(file).await
}

#[cfg(not(feature = "yummy-delta"))]
async fn delta_apply(_file: String) -> std::io::Result<()> {
    unreachable!()
}

#[cfg(feature = "yummy-delta")]
async fn delta_serve(
    config: String,
    host: String,
    port: u16,
    log_level: String,
) -> std::io::Result<()> {
    yummy_delta::run_delta_server(config, host, port, log_level).await
}

#[cfg(not(feature = "yummy-delta"))]
async fn delta_serve(
    _config: String,
    _host: String,
    _port: u16,
    _log_level: String,
) -> std::io::Result<()> {
    unreachable!()
}

#[cfg(feature = "yummy-ml")]
async fn ml_serve(
    model: String,
    host: String,
    port: u16,
    log_level: String,
) -> std::io::Result<()> {
    yummy_ml::serve_ml_model(model, host, port, log_level).await
}

#[cfg(not(feature = "yummy-ml"))]
async fn ml_serve(
    _model: String,
    _host: String,
    _port: u16,
    _log_level: String,
) -> std::io::Result<()> {
    unreachable!()
}

#[cfg(feature = "yummy-llm")]
async fn llm_serve(
    config: String,
    host: String,
    port: u16,
    log_level: Option<&String>,
    workers: Option<&usize>,
) -> std::io::Result<()> {
    yummy_llm::serve_llm(config, host, port, log_level, workers).await
}

#[cfg(not(feature = "yummy-llm"))]
async fn llm_serve(
    _config: String,
    _host: String,
    _port: u16,
    _log_level: Option<&String>,
    _workers: Option<&usize>,
) -> std::io::Result<()> {
    unreachable!()
}
