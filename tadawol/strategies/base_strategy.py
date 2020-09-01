from abc import ABC, abstractmethod
from math import inf
from typing import List, Any, Type, Optional
import logging
from datetime import datetime, timedelta

import pandas as pd

from ..history import get_historical_data, get_fresh_data
from ..utils import get_last_week_entries, clean_results, get_search_grid

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class BaseStrategy(ABC):

    def __init__(self, max_lose_percent: int, max_win_percent: int, max_keep_days: int):

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

        df.sort_values(by="Date", inplace=True, ascending=True)
        df.reset_index(drop=True, inplace=True)
        for i in range(1, self.max_keep_days + 1):
            df.loc[:, f"Close_{i}"] = df["Close"].shift(-i)
        for i in range(1, self.max_keep_days + 1):
            df.loc[:, f"Open_{i}"] = df["Open"].shift(-i)
        df.reset_index(drop=True, inplace=True)
        def get_exit_data(row):

            if not row["entry"]:
                return None, None
            day_close = -1
            close = row["Close"]

            last_close = close
            down_days = 0
            for day in range(1, self.max_keep_days):
                day_close = row[f"Close_{day}"]
                day_open = row[f"Open_{day}"]

                if day_close < last_close:
                    down_days += 1
                else:
                    down_days = 0
                last_close = day_close  # IMPORTANT
                if day_close > (1 + self.max_win_percent/100.0) * close:
                    return max(day_open, (1 + self.max_win_percent/100.0) * close), day
                if day_close < (1 - self.max_lose_percent/100.0) * close:
                    return min(day_open, (1 - self.max_lose_percent/100.0) * close), day

                if down_days >= self.max_down_days:
                    return day_close, day

            return day_close, self.max_keep_days

        df.loc[:, "exit_data"] = df.apply(get_exit_data, axis=1)
        df.loc[:, "exit_price"] = df.exit_data.map(lambda x: x[0])
        df.loc[:, "exit_date"] = df.exit_data.map(lambda x: x[1])

        return df

    def _get_entries(self, df: pd.DataFrame, ticker_to_simulate: Optional[str] = None):

        df = df.copy(deep=True)
        if ticker_to_simulate is not None:
            df = df[df["Ticker"] == ticker_to_simulate]
        data = []
        tickers_number = df["Ticker"].nunique()
        logger.info(f"Simulating strategy for {tickers_number} tickers")

        current_tickers_number = 0
        for ticker, ticker_data in df.groupby(["Ticker"]):
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

        return df[df["week_previous_entries"] >= 0]

    def simulate(self, ticker_to_simulate: Optional[str] = None):
        df = get_historical_data()
        return self._get_entries(df, ticker_to_simulate)

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
