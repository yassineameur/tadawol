from typing import List
import pandas as pd

from ..strategies import base_strategy
from tadawol import stats
from tadawol import earnings
from math import inf


class Earnings(base_strategy.BaseStrategy):

    def __init__(
            self,
            short_window: int = 5,
            long_window: int = 50,
            max_lose_percent: int = 8,
            max_win_percent: int = 15,
            max_keep_days: int = 15
    ):
        super().__init__(
            max_lose_percent=max_lose_percent,
            max_win_percent=max_win_percent,
            max_keep_days=max_keep_days,
        )

        self.short_window = short_window
        self.long_window = long_window

        earnings.update_data()
        self.earnings_df = earnings.get_earnings_df()
        self.earnings_df.rename(columns={"ticker": "Ticker"}, inplace=True)

        self.name = "Earnings"

    def add_entries_for_ticker(self, ticker_data: pd.DataFrame, **kwargs):
        ticker_data = ticker_data.copy(deep=True)
        ticker_data.sort_values(by="Date", ascending=True, inplace=True)
        ticker_data.reset_index(drop=True, inplace=True)
        assert ticker_data["Ticker"].nunique() == 1

        # get ticker earnings
        df, long_window_ema_column = stats.add_ema(ticker_data, window=self.long_window)
        df, short_window_ema_column = stats.add_ema(df, window=self.short_window)

        df.loc[:, "long_ema_evolution"] = df[long_window_ema_column] - df[long_window_ema_column].shift(1)
        df.loc[:, "short_ema_evolution"] = df[short_window_ema_column] - df[short_window_ema_column].shift(1)

        ticker = ticker_data["Ticker"].unique()[0]
        ticker_earnings = self.earnings_df[self.earnings_df["Ticker"] == ticker]
        df = pd.merge(df, ticker_earnings, on=["Date", "Ticker"], how="left")
        df.sort_values(by="Date", ascending=True, inplace=True)
        for i in range(1, 4):
            df.loc[:, f"earnings_{i}"] = df["epssurprisepct"].shift(i)

        def last_surprise(row):
            for i in range(1, 4):
                last_surprise = row[f"earnings_{i}"]
                if not pd.isna(last_surprise):
                    return last_surprise

            return -inf

        df.loc[:, "last_surprise"] = df.apply(last_surprise, axis=1)

        df.loc[:, "entry"] = (df["short_ema_evolution"] > 0) & (df["last_surprise"] > 0)

        # go-on condition
        df.loc[:, "good_evolution"] = df["short_ema_evolution"] > 0
        df.loc[:, "go-on"] = df["good_evolution"] | df["good_evolution"].shift(1) | df["good_evolution"].shift(2) | df[
            "good_evolution"].shift(3) | df["good_evolution"].shift(4) | df["good_evolution"].shift(5)
        return df

    @staticmethod
    def get_grid():
        return [
            [9, 12, 15],
            [22, 26, 30],
            [9, 6],
            [8],
            [15],
            [7, 10, 15]
        ]

    @staticmethod
    def get_hint_columns() -> List[str]:
        return []
