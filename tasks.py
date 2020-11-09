from datetime import datetime
import logging

from celery import Celery
import pandas as pd

from tadawol.strategies.macd import MACD
from tadawol.strategies.base_strategy import BaseStrategy
from tadawol.strategies.reverse import Reverse
from tadawol.history import get_top_tickers, get_fresh_data
from tadawol.services import email
from tadawol.config import BrokerConfig


app = Celery("tasks", broker=BrokerConfig().url)

logger = logging.getLogger(__name__)


def _send_entry_and_exit(entry_df: pd.DataFrame, exit_df: pd.DataFrame, strategy: BaseStrategy):

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
        """.format(entry_df.to_html(), exit_df[exit_columns].to_html())
    subject = f"{strategy.name} on {datetime.today().date()}"
    email.send_email(html, subject)


@app.task
def execute_macd_reverse_strategies(
        min_top_ticker: int,
        max_top_ticker: int
):
    strategies = [MACD(), Reverse()]
    tickers = get_top_tickers(min_top_ticker, max_top_ticker)
    df = get_fresh_data(tickers)
    for strategy in strategies:
        try:
            today_trades, today_exits = strategy.get_today_trades_and_exits(df.copy(deep=True))
            _send_entry_and_exit(today_trades, today_exits, strategy)
        except KeyboardInterrupt as k_e:
            raise KeyboardInterrupt from k_e
