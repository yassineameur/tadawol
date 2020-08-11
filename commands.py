from tadawol.yahoo import update_data, check_data
from tadawol.strategies.base_strategy import get_best_config
from tadawol.strategies.record import Record
from tadawol.strategies.recovery import Recovery
import click


@click.group()
def cli():
    pass


@cli.command("update")
def update():
    update_data()


@cli.command("check")
@click.option("--ticker", default=None)
def check(ticker):
    check_data(ticker)


@cli.command("run_grid")
@click.argument("strategy", type=click.Choice(['Recovery', 'Record'], case_sensitive=False))
def check(strategy):
    if strategy == "Recovery":
        strategy = Recovery
    if strategy == "Record":
        strategy = Record

    get_best_config(strategy)
