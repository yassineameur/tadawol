from celery import Celery
import pandas as pd

from tadawol.strategies.macd import MACD
from tadawol.history import get_top_tickers
from tadawol.earnings import get_earnings_data_on_all_dates
from tadawol.services import email


app = Celery("tasks", broker="amqps://elojhrja:OkLbk867cudFaYudiGFv6zHP2TpuRdgz@chinook.rmq.cloudamqp.com/elojhrja")


@app.task
def execute_macd(
        min_top_ticker: int = 0,
        max_top_ticker: int = 300,
        days_to_next_result: int = 20,
        days_since_last_result: int = 0,
        week_previous_entries: int = 1):
    strategy = MACD()

    tickers = get_top_tickers(min_top_ticker, max_top_ticker)
    today_trades, today_exits = strategy.get_today_trades_and_exits(tickers)

    earnings_by_date = get_earnings_data_on_all_dates(today_trades)
    today_trades = pd.merge(today_trades, earnings_by_date, on=["Ticker", "Date"])
    today_trades = today_trades[today_trades["days_to_next_result"] > days_to_next_result]
    today_trades = today_trades[today_trades["days_since_last_result"] > days_since_last_result]
    today_trades = today_trades[today_trades["week_previous_entries"] >= week_previous_entries]
    print(list(today_trades.columns))
    print("****")
    print(today_trades.head())

    entry_columns = ["Date", "Ticker", "company_short_name", "Close", "week_previous_entries", "earnings_surprise", "days_to_next_result"]

    exit_columns = entry_columns.copy()
    exit_columns += ["exit_reason"]

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
    """.format(today_trades[entry_columns].to_html(), today_exits[exit_columns].to_html())

    email.send_email(html)

