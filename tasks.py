from datetime import datetime
from typing import List

from celery import Celery
import pandas as pd

from tadawol.strategies.macd import MACD
from tadawol.strategies.base_strategy import BaseStrategy
from tadawol.strategies.earnings import Earnings
from tadawol.strategies.reverse import Reverse
from tadawol.history import get_top_tickers
from tadawol.earnings import get_earnings_data_on_all_dates
from tadawol.services import email
from tadawol.config import BrokerConfig


app = Celery("tasks", broker=BrokerConfig().url)


def _send_entry_and_exit(entry_df: pd.DataFrame, exit_df: pd.DataFrame, strategy: BaseStrategy):
    trades_columns = [
        "Date", "Ticker", "company_short_name", "Close", "week_previous_entries", "days_to_next_result", "exit_reason"]

    for col in strategy.get_hint_columns():
        trades_columns.append(col)

    exit_columns = ["Date", "Ticker", "Close", "week_previous_entries", "exit_reason"]

    html = """\
        <html>
          <head></head>
          <body>
            <h2> Entries </h2>
            {0}
            <h2> Exits </h2>
            {1}
          </body>
        </html>
        """.format(entry_df[trades_columns].to_html(), exit_df[exit_columns].to_html())
    subject = f"{strategy.name} on {datetime.today().date()}"
    email.send_email(html, subject)


@app.task
def execute_macd(
        min_top_ticker: int,
        max_top_ticker: int
):
    strategy = MACD()

    tickers = get_top_tickers(min_top_ticker, max_top_ticker)
    today_trades, today_exits = strategy.get_today_trades_and_exits(tickers)

    earnings_by_date = get_earnings_data_on_all_dates(today_trades)
    today_trades = pd.merge(today_trades, earnings_by_date, on=["Ticker", "Date"])

    _send_entry_and_exit(today_trades, today_exits, strategy)


@app.task
def execute_reverse(
        min_top_ticker: int,
        max_top_ticker: int
):
    strategy = Reverse()

    tickers = get_top_tickers(min_top_ticker, max_top_ticker)
    today_trades, today_exits = strategy.get_today_trades_and_exits(tickers)

    earnings_by_date = get_earnings_data_on_all_dates(today_trades)
    today_trades = pd.merge(today_trades, earnings_by_date, on=["Ticker", "Date"])

    _send_entry_and_exit(today_trades, today_exits, strategy)


@app.task
def execute_earnings(
        min_top_ticker: int,
        max_top_ticker: int,):
    strategy = Earnings()

    tickers = get_top_tickers(min_top_ticker, max_top_ticker)
    today_trades, today_exits = strategy.get_today_trades_and_exits(tickers)

    earnings_by_date = get_earnings_data_on_all_dates(today_trades)
    today_trades = pd.merge(today_trades, earnings_by_date, on=["Ticker", "Date"])

    today_trades = today_trades[today_trades["Close"] > today_trades["Open"]]
    today_trades = today_trades[today_trades["long_ema_evolution"] / today_trades["Close"] > 0.002]
    today_trades = today_trades[today_trades["week_previous_entries"] > 0]
    today_trades = today_trades[today_trades["short_ema_evolution"] > 0.1]
    today_trades = today_trades[today_trades["earnings_surprise"] < 20]

    _send_entry_and_exit(today_trades, today_exits, strategy)

