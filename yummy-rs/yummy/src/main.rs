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
                            arg!(--config <FILE> "config path"),
                            arg!(--host <HOST> "host"),
                            arg!(--port <PORT> "port"),
                            arg!(--loglevel <LOGLEVEL> "log level"),
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
                let port = sub_sub_matches
                    .get_one::<String>("port")
                    .expect("required")
                    .parse::<u16>()?;

                let log_level = sub_sub_matches
                    .get_one::<String>("loglevel")
                    .expect("required");

                llm_serve(config.clone(), host.clone(), port, log_level.clone()).await?
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
    log_level: String,
) -> std::io::Result<()> {
    yummy_llm::serve_llm(config, host, port, log_level).await
}

#[cfg(not(feature = "yummy-llm"))]
async fn llm_serve(
    _config: String,
    _host: String,
    _port: u16,
    _log_level: String,
) -> std::io::Result<()> {
    unreachable!()
}
