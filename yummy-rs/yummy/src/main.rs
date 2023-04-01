use clap::{arg, Command};
use yummy_delta::apply_delta;

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
                yummy_delta::apply_delta(file);
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }

    Ok(())
}
