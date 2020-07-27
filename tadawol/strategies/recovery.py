# strategy
# EMA 50 > EMA 20
# EMA 10 > EMA 20
# EMA 15 is progressing positively during 10 days
# See whether we can wait many times for the signal
import logging
from datetime import datetime

import pandas as pd

from ..yahoo import get_historical_data
from tadawol import stats


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class Recovery:

    def __init__(
            self,
            long_window: int = 50,
            medium_window: int = 20,
            short_window: int = 10,
            max_lose_percent: int = 10,
            max_win_percent: int = 15,
            max_keep_days: int = 5
    ):
        assert short_window < medium_window < long_window
        self.long_window = long_window
        self.medium_window = medium_window
        self.short_window = short_window

        self.max_lose_percent = max_lose_percent
        self.max_win_percent = max_win_percent
        self.max_keep_days = max_keep_days

    def simulate(self):
        df = get_historical_data()
        data = []

        tickers_number = df["Ticker"].nunique()
        logger.info(f"Simulating strategy for {tickers_number} tickers")

        current_tickers_number = 0
        for ticker, ticker_data in df.groupby(["Ticker"]):
            ticker_data = ticker_data.sort_values(by=["Date"], ascending=True)
            ticker_entries = self.add_entries_for_ticker(ticker_data)
            ticker_exits = self.get_exit_prices_for_ticker(ticker_entries)
            data.append(ticker_exits)

            current_tickers_number += 1
            if current_tickers_number % 100 == 0:
                logger.info(f"Simulation in progress : {round(100 * current_tickers_number /tickers_number)}%")

        if len(data) > 0:
            return pd.concat(data, axis=0)
        return None

    def add_entries_for_ticker(self, ticker_data: pd.DataFrame):

        assert ticker_data["Ticker"].nunique() == 1

        df, long_window_ema_column = stats.add_ema(ticker_data, window=self.long_window)
        df, medium_window_ema_column = stats.add_ema(df, window=self.medium_window)
        df, short_window_ema_column = stats.add_ema(df, window=self.long_window)

        df, sma_column = stats.add_sma(df, window=5, column=short_window_ema_column)
        max_sma_column = "max_sma"
        df.loc[:, max_sma_column] = df[sma_column].rolling(window=10).max()
        df.loc[:, "entry"] = (df[max_sma_column] == df[sma_column]) & (df[long_window_ema_column] > df[medium_window_ema_column]) & (df[short_window_ema_column] > df[medium_window_ema_column])
        return df

    def get_exit_prices_for_ticker(self, df: pd.DataFrame):

        assert "entry" in list(df.columns)
        assert df["Ticker"].nunique() == 1
        for i in range(1, self.max_keep_days + 1):
            df.loc[:, f"Close_{i}"] = df["Close"].shift(-i)

        def get_exit_price(row):
            close = row['Close']
            if not row["entry"]:
                return None

            for i in range(1, self.max_keep_days):
                next_close = row[f"Close_{i}"]
                if next_close > (1 + self.max_win_percent/100.0) * close:
                    return next_close
                if next_close < (1 - self.max_lose_percent/100.0) * close:
                    return next_close
            return next_close

        df.loc[:, "exit_price"] = df.apply(get_exit_price, axis=1)

        return df


def clean_data(df: pd.DataFrame):
    df = df[(df["entry"]) & (df["Close"] > 2)]
    df = df[df["Volume"] > 100000]
    df = df[df["win_percent"] < 50]
    df = df[df["win_percent"] > -50]

    return df


def get_results(df: pd.DataFrame):

    df = df.copy(deep=True)

    assert "exit_price" in list(df.columns) and "entry" in list(df.columns)

    df = df[df["Close"] > 0.5]
    df.loc[:, "win_percent"] = 100 * (df["exit_price"] - df["Close"]) / df["Close"]

    df = clean_data(df)
    logger.info(f"Enter cases number = {df.shape[0]}")
    logger.info(
        "Average winning cases = {}".format(
            100 * df[df["win_percent"] >= 0].shape[0] / df.shape[0])
    )
    logger.info("Average win = {}".format(round(df["win_percent"].mean(), 2)))

    return df


def print_negative_results(df):
    df = df.copy(deep=True)
    df = df[df["Date"] > datetime(2020, 1, 1)]
    df.sort_values(by=["win_percent"], ascending=True, inplace=True)
    print(df.head(20))


if __name__ == "__main__":
    strategy = Recovery()
    data = strategy.simulate()
    df = get_results(data)
    print_negative_results(df)
