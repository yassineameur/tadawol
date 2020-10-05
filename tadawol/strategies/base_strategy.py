from abc import ABC, abstractmethod
from math import inf
from typing import List, Any, Type, Optional
import logging
from datetime import datetime, timedelta


import pandas as pd

from ..history import get_historical_data, get_fresh_data, get_top_tickers
from ..utils import get_last_week_entries, clean_results, get_search_grid

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BaseStrategy(ABC):

    def __init__(self, max_lose_percent: int, max_win_percent: int, max_keep_days: int):

        self.max_lose_percent = max_lose_percent
        self.max_win_percent = max_win_percent
        self.max_keep_days = max_keep_days
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
            df.loc[:, f"Open_{i}"] = df["Open"].shift(-i)
            df.loc[:, f"go-on_{i}"] = df["go-on"].shift(-i)
            df.loc[:, f"Date_{i}"] = df["Date"].shift(-i)

        df.reset_index(drop=True, inplace=True)
        def get_exit_data(row):

            if not row["entry"]:
                return None, None, None
            day_close = -1
            close = row["Close"]

            for day in range(1, self.max_keep_days):
                day_close = row[f"Close_{day}"]
                day_open = row[f"Open_{day}"]
                day_date = row[f"Date_{day}"]
                if pd.isna(day_close):
                    return None, None, None, None
                if day_close > (1 + self.max_win_percent/100.0) * close:
                    return max(day_open, (1 + self.max_win_percent/100.0) * close), day, "max win"
                if day_close < (1 - self.max_lose_percent/100.0) * close:
                    return min(day_open, (1 - self.max_lose_percent/100.0) * close), day, "max lose"

                go_on = row[f"go-on_{day}"]
                if not go_on:
                    return day_close, day, "go-on lost", day_date

            return day_close, self.max_keep_days, "end days", day_date

        df.loc[:, "exit_data"] = df.apply(get_exit_data, axis=1)
        df.loc[:, "exit_price"] = df.exit_data.map(lambda x: x[0])
        df.loc[:, "exit_date"] = df.exit_data.map(lambda x: x[1])
        df.loc[:, "exit_reason"] = df.exit_data.map(lambda x: x[2])
        df.loc[:, "exit"] = df.exit_data.map(lambda x: x[3])

        return df

    def _get_trades(self, df: pd.DataFrame, tickers_to_simulate: Optional[List[str]] = None):

        df = df.copy(deep=True)
        if tickers_to_simulate is not None:
            df = df[df["Ticker"].isin(tickers_to_simulate)]
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
        df = df[df["entry"]]

        df.loc[:, "win_percent"] = 100 * (df["exit_price"] - df["Close"]) / df["Close"]
        df = clean_results(df)
        df = get_last_week_entries(df)
        return df[df["week_previous_entries"] >= 0]

    def simulate(self, tickers_to_simulate: Optional[List[str]] = None):
        df = get_historical_data()
        trades = self._get_trades(df, tickers_to_simulate)
        return trades[trades['exit_price'].notna()]

    def get_today_trades_and_exits(self, tickers: Optional[List[str]] = None):
        df = get_fresh_data(tickers)

        trades = self._get_trades(df)
        today = (datetime.now()).date()
        today_date = datetime(today.year, today.month, today.day)
        today_trades = trades[trades["Date"] == today_date]
        today_exits = trades[trades["exit"] == today_date]
        return today_trades, today_exits


def get_best_config(strategy: Type[BaseStrategy]):
    grid = strategy.get_grid()
    search_grid = get_search_grid(grid)

    tickers = get_top_tickers(0, 300)

    best_win = -inf
    best_combination = None
    logger.setLevel(logging.INFO)
    simulations_number = len(search_grid)
    i = 1
    for combination in search_grid:
        r = strategy(*combination)
        res = r.simulate(tickers)
        current_win = round(100 * res[res["win_percent"] > 0].shape[0] / res.shape[0], 2)
        if current_win > best_win:
            best_win = current_win
            best_combination = combination

        print(f" Simulation : {i}/{simulations_number}: Current best combination = ", best_combination)
        print(f" Simulation : {i}/{simulations_number}:Current best win = ", best_win)
        print("-----------------------------------------------------")

        i += 1

    print("Best combination = ", best_combination)
    print("Best win = ", best_win)
