import click

def package_installation_hint(package_name: str):
    OKGREEN = '\033[92m'
    ENDC = '\033[0m'
    BOLD = "\033[1m"
    print(f"\t{BOLD}Please install:{ENDC}\t{OKGREEN}{package_name}{ENDC}{BOLD}{ENDC}")

@click.group()
@click.pass_context
def cli(
    ctx: click.Context
):
    pass

@cli.group(name='delta')
def delta_cmd():
    """Delta server run command"""
    pass

@delta_cmd.command("server")
@click.option("--host","-h",
              type=click.STRING,
              default="127.0.0.1",
              show_default=True,
              help='Host for the feature server.'
              )
@click.option("--port","-p",
              type=click.INT,
              default=8080,
              show_default=True,
              help='Port for the feature server.'
              )
@click.option("--filename","-f",
              type=click.STRING,
              default="config.yaml",
              show_default=True,
              help='Configuration filename for the delta lake.'
              )
@click.option("--log-level",
              type=click.STRING,
              default="error",
              show_default=True,
              help='Log level for the feature server. One of: debug, info, error, warn'
              )
@click.pass_context
def delta_server_command(
    ctx: click.Context,
    host: str,
    port: int,
    filename: str,
    log_level: str,
):
    """Start rust delta."""
    try:
        import yummy_delta
        yummy_delta.run(filename, host, port, log_level)
    except ModuleNotFoundError:
        package_installation_hint("yummy[delta]")

@delta_cmd.command("apply")
@click.option("--filename","-f",
              type=click.STRING,
              default="apply.yaml",
              show_default=True,
              help='Configuration filename for the delta lake.'
              )
@click.pass_context
def delta_apply_command(
    ctx: click.Context,
    filename: str,
):
    """Start rust apply delta."""
    try:
        import yummy_delta
        yummy_delta.run_apply(filename)
    except ModuleNotFoundError:
        package_installation_hint("yummy[delta]")


@delta_cmd.command(name='get')
@click.argument('otype', nargs=1)
@click.option("--filename","-f",
              type=click.STRING,
              default="config.yaml",
              show_default=True,
              help='Configuration filename for the delta lake.'
              )
@click.option("--store","-s",
              type=click.STRING,
              required=False,
              help='Store name'
              )
@click.option("--table","-t",
              type=click.STRING,
              required=False,
              help='Table name'
              )
@click.pass_context
def delta_get_command(
    ctx: click.Context,
    otype: str,
    filename: str,
    store: str,
    table: str,
):
    """Delta get command"""
    try:
        import yummy_delta

        if otype == "stores":
            yummy_delta.run_pprint_stores(filename)
        elif otype == "tables":
            yummy_delta.run_pprint_tables(filename, store)
        elif otype == "table":
            yummy_delta.run_pprint_table(filename, store, table)

    except ModuleNotFoundError:
        package_installation_hint("yummy[delta]")


@cli.group(name='features')
def features_cmd():
    """Features related commands"""
    pass

@features_cmd.command("serve")
@click.option("--host","-h",
              type=click.STRING,
              default="127.0.0.1",
              show_default=True,
              help='Host for the feature server.'
              )
@click.option("--port","-p",
              type=click.INT,
              default=6566,
              show_default=True,
              help='Port for the feature server.'
              )
@click.option("--filename","-f",
              type=click.STRING,
              default="feature_store.yaml",
              show_default=True,
              help='Configuration filename for the feature server.'
              )
@click.option("--log-level",
              type=click.STRING,
              default="error",
              show_default=True,
              help='Log level for the feature server. One of: debug, info, error, warn'
              )
@click.pass_context
def features_serve_command(
    ctx: click.Context,
    host: str,
    port: int,
    filename: str,
    log_level: str,
):
    """Start rust feature server."""
    try:
        import yummy_features
        yummy_features.serve(filename, host, port, log_level)
    except ModuleNotFoundError:
        package_installation_hint("yummy[features]")



@cli.group(name='models')
def models_cmd():
    """Models related commands"""
    pass

@models_cmd.command("serve")
@click.option("--host","-h",
              type=click.STRING,
              default="127.0.0.1",
              show_default=True,
              help='Host for the feature server.'
              )
@click.option("--port","-p",
              type=click.INT,
              default=5000,
              show_default=True,
              help='Port for the feature server.'
              )
@click.option("--model-uri","-m",
              type=click.STRING,
              required=True,
              help='Local model path.'
              )
@click.option("--log-level",
              type=click.STRING,
              default="error",
              show_default=True,
              help='Log level for the feature server. One of: debug, info, error, warn'
              )
@click.pass_context
def models_serve_command(
    ctx: click.Context,
    host: str,
    port: int,
    model_uri: str,
    log_level: str,
):
    """Start mlflow model server."""
    try:
        import yummy_mlflow
        yummy_mlflow.serve(model_uri, host, port, log_level)
    except ModuleNotFoundError:
        package_installation_hint("yummy[mlflow]")


if __name__ == '__main__':
    cli()
