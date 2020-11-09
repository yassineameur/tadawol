from abc import ABC, abstractmethod
from math import inf
from typing import List, Any, Type, Optional
import logging
from datetime import datetime

from click import progressbar

from ..simulator import simulate_trades


import pandas as pd

from ..history import get_historical_data, get_top_tickers
from ..utils import get_last_week_entries, clean_results, get_search_grid

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BaseStrategy(ABC):

    def __init__(self, max_lose_percent: int, max_win_percent: int, max_keep_days: int):

        self.max_lose_percent = max_lose_percent
        self.max_win_percent = max_win_percent
        self.max_keep_days = max_keep_days
        self.logger = logger
        self.name = "abstract"

    @abstractmethod
    def add_entries_for_ticker(self, ticker_data: pd.DataFrame):
        pass

    @staticmethod
    @abstractmethod
    def get_grid() -> List[Any]:
        pass

    @staticmethod
    @abstractmethod
    def get_hint_columns() -> List[str]:
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
                return None, None, None, None
            day_close = -1
            close = row["Close"]

            for day in range(1, self.max_keep_days + 1):
                day_close = row[f"Close_{day}"]
                day_open = row[f"Open_{day}"]
                day_date = row[f"Date_{day}"]
                if pd.isna(day_close):
                    return None, None, None, None
                if day_close > (1 + self.max_win_percent/100.0) * close:
                    return max(day_open, (1 + self.max_win_percent/100.0) * close), day, "max win", day_date
                if day_close < (1 - self.max_lose_percent/100.0) * close:
                    return min(day_open, (1 - self.max_lose_percent/100.0) * close), day, "max lose", day_date

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
            if current_tickers_number % 20 == 0:
                logger.info(f"Simulation in progress : {round(100 * current_tickers_number / tickers_number)}%")

        if len(data) == 0:
            return None
        df = pd.concat(data, axis=0)
        df = df[df["entry"]]

        df.loc[:, "win_percent"] = 100 * (df["exit_price"] - df["Close"]) / df["Close"]
        df = clean_results(df)
        df = get_last_week_entries(df)
        return df

    def simulate(self, tickers_to_simulate: Optional[List[str]] = None):
        df = get_historical_data()
        trades = self._get_trades(df, tickers_to_simulate)
        return trades[trades['exit_price'].notna()]

    def add_entry_hints(self, df: pd.DataFrame):

        if df.empty:
            return df

        df.loc[:, "max_lose"] = df["Close"].map(lambda x: x * (100 - self.max_lose_percent) / 100)
        df.loc[:, "invest"] = df["week_previous_entries"].map(lambda x: 2500 if x >= 1 else 1800)

        def get_shares_number(x):
            return round(x["invest"]/x["Close"])

        df.loc[:, "shares_number"] = df.apply(get_shares_number, axis=1)

        return df

    def get_today_trades_and_exits(self, df: pd.DataFrame):

        assert "Ticker" in list(df.columns)
        assert "Close" in list(df.columns)
        assert "Date" in list(df.columns)
        trades = self._get_trades(df)
        today = (datetime.now()).date()
        today_date = datetime(today.year, today.month, today.day)
        #today_date = datetime(2020, 11, 3)
        today_trades = trades[trades["Date"] == today_date]
        today_exits = trades[trades["exit"] == today_date]
        trades_columns = [
            "Date", "Ticker", "Close", "week_previous_entries", "exit_reason"
        ]

        if not today_trades.empty:
            hint_columns = self.get_hint_columns()
            if hint_columns:
                today_trades.sort_values(by=hint_columns, ascending=False)
                for col in hint_columns:
                    trades_columns.append(col)

            today_trades = self.add_entry_hints(today_trades)
            trades_columns.extend(["max_lose", "invest", "shares_number"])

        return today_trades[trades_columns], today_exits


def get_best_config(strategy: Type[BaseStrategy]):
    grid = strategy.get_grid()
    search_grid = get_search_grid(grid)

    tickers = get_top_tickers(100, 300)

    best_win = -inf
    best_win_percent = 0
    best_combination = None
    logger.setLevel(logging.ERROR)
    simulations_number = len(search_grid)
    i = 1

    with progressbar(search_grid) as combinations:

        for combination in combinations:
            r = strategy(*combination)
            res = r.simulate(tickers)
            current_win, _, _ = simulate_trades(res)
            win_percent = round(100 * res[res["win_percent"] > 0].shape[0] / res.shape[0], 2)
            if current_win > best_win:
                best_win = current_win
                best_win_percent = win_percent
                best_combination = combination

            print(f" Simulation : {i}/{simulations_number}: Best combination = ", best_combination)
            print(f" Simulation : {i}/{simulations_number}:Best win = ", best_win)
            print(f" Simulation : {i}/{simulations_number}:Best win %= ", best_win_percent)
            print("-----------------------------------------------------")

            i += 1

        print("Best combination = ", best_combination)
        print("Best win = ", best_win)
        print("Best win % = ", best_win_percent)
