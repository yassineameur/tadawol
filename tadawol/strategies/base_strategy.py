from abc import ABC, abstractmethod
from math import inf
from typing import List, Any, Type
import logging
from datetime import datetime

import pandas as pd

from ..history import get_historical_data, get_fresh_data
from ..utils import get_last_week_entries, clean_results, get_search_grid

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class BaseStrategy(ABC):

    def __init__(self, max_lose_percent: int = 15, max_win_percent: int = 20, max_keep_days: int = 10):

        self.max_lose_percent = max_lose_percent
        self.max_win_percent = max_win_percent
        self.max_keep_days = max_keep_days
        self.max_down_days = 4
        self.logger = logger

    @abstractmethod
    def add_entries_for_ticker(self, ticker_data: pd.DataFrame):
        pass

    @staticmethod
    @abstractmethod
    def get_grid() -> List[Any]:
        pass

    def get_exit_prices_for_ticker(self, df: pd.DataFrame):
        assert "entry" in list(df.columns)
        assert df["Ticker"].nunique() == 1
        for i in range(1, self.max_keep_days + 1):
            df.loc[:, f"Close_{i}"] = df["Close"].shift(-i)

        def get_exit_price(row):

            if not row["entry"]:
                return None
            next_close = -1
            close = row['Close']
            last_close = close
            down_days = 0
            for day in range(1, self.max_keep_days):
                next_close = row[f"Close_{day}"]
                if next_close < last_close:
                    down_days += 1
                else:
                    down_days = 0
                last_close = next_close  # IMPORTANT

                if down_days >= self.max_down_days:
                    return next_close
                if next_close > (1 + self.max_win_percent/100.0) * close:
                    return next_close
                if next_close < (1 - self.max_lose_percent/100.0) * close:
                    return next_close
            return next_close

        df.loc[:, "exit_price"] = df.apply(get_exit_price, axis=1)

        return df

    def _get_entries(self, df: pd.DataFrame, samples_by_ticker=inf):

        data = []
        tickers_number = df["Ticker"].nunique()
        logger.info(f"Simulating strategy for {tickers_number} tickers")

        current_tickers_number = 0
        for ticker, ticker_data in df.groupby(["Ticker"]):
            ticker_data = ticker_data.sample(min(samples_by_ticker, ticker_data.shape[0]))
            ticker_data = ticker_data.sort_values(by=["Date"], ascending=True)
            ticker_data.reset_index(drop=True, inplace=True)
            ticker_entries = self.add_entries_for_ticker(ticker_data)
            ticker_exits = self.get_exit_prices_for_ticker(ticker_entries)
            data.append(ticker_exits)

            current_tickers_number += 1
            if current_tickers_number % 200 == 0:
                logger.info(f"Simulation in progress : {round(100 * current_tickers_number / tickers_number)}%")

        if len(data) == 0:
            return None
        df = pd.concat(data, axis=0)
        df.loc[:, "win_percent"] = 100 * (df["exit_price"] - df["Close"]) / df["Close"]
        df = clean_results(df)
        df = get_last_week_entries(df)

        return df[df["week_previous_entries"] >= 1]

    def simulate(self, samples_by_ticker=inf):
        df = get_historical_data()
        return self._get_entries(df, samples_by_ticker)

    def get_today_entries(self):
        df = get_fresh_data()
        df = self._get_entries(df)
        today = datetime.now().date()
        return df[df["Date"] == today]


def get_best_config(strategy: Type[BaseStrategy]):
    grid = strategy.get_grid()
    search_grid = get_search_grid(grid)

    best_win = -inf
    best_combination = None
    for combination in search_grid:
        r = strategy(*combination)
        res = r.simulate()
        current_win = res["win_percent"].mean()
        if current_win > best_win:
            best_win = current_win
            best_combination = combination

        print("Current best combination = ", best_combination)
        print("Current best win = ", best_win)
        print("-----------------------------------------------------")

    print("Best combination = ", best_combination)
    print("Best win = ", best_win)
