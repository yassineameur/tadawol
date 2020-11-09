from tadawol.history import update_data, check_data as check_history_data, update_tickers_from_scratch
from tadawol.earnings import update_data as update_earnings, check_data as check_earnings_data
from tadawol.strategies.base_strategy import get_best_config
from tadawol.strategies.reverse import Reverse
from tadawol.strategies.macd import MACD
import click


@click.group()
def cli():
    pass


@cli.command("update_history")
def update():
    update_tickers_from_scratch()


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
@click.argument("strategy", type=click.Choice(['MACD', 'Reverse'], case_sensitive=False))
def check(strategy):
    if strategy == "MACD":
        strategy = MACD

    if strategy == "Reverse":
        strategy = Reverse

    get_best_config(strategy)
