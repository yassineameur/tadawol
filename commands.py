from tadawol.history import update_data, check_data as check_history_data
from tadawol.earnings import update_data as update_earnings, check_data as check_earnings_data
from tadawol.strategies.base_strategy import get_best_config
from tadawol.strategies.record import Record
from tadawol.strategies.recovery import Recovery
import click


@click.group()
def cli():
    pass


@cli.command("update_history")
def update():
    update_data()


@cli.command("update_earnings")
def update():
    update_earnings()


@cli.group()
def check():
    pass


@check.command("history")
@click.option("--ticker", default=None)
def check_history(ticker):
    check_history_data(ticker)


@check.command("earnings")
@click.option("--ticker", default=None)
def check(ticker):
    check_earnings_data(ticker)


@cli.command("run_grid")
@click.argument("strategy", type=click.Choice(['Recovery', 'Record'], case_sensitive=False))
def check(strategy):
    if strategy == "Recovery":
        strategy = Recovery
    if strategy == "Record":
        strategy = Record

    get_best_config(strategy)
