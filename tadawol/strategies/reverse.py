from typing import List

import pandas as pd

from ..strategies import base_strategy
from tadawol import stats


class Reverse(base_strategy.BaseStrategy):

    def __init__(
            self,
            ema_window: int = 15,
            evolution_window: int = 5,
            max_lose_percent: int = 8,
            max_win_percent: int = 15,
            max_keep_days: int = 15
    ):
        self.ema_window = ema_window
        self.evolution_window = evolution_window
        super().__init__(
            max_lose_percent=max_lose_percent,
            max_win_percent=max_win_percent,
            max_keep_days=max_keep_days,
        )

        self.name = "Reverse"

    def add_entries_for_ticker(self, ticker_data: pd.DataFrame):
        ticker_data = ticker_data.copy(deep=True)
        ticker_data.sort_values(by="Date", ascending=True, inplace=True)
        ticker_data.reset_index(drop=True, inplace=True)
        assert ticker_data["Ticker"].nunique() == 1

        df, ema_column = stats.add_ema(ticker_data, window=self.ema_window)
        # smooth ema
        df, ema_column = stats.add_sma(df, window=3, column=ema_column)

        df, rsi_column = stats.add_rsi(df)
        # smooth rsi twice
        df, rsi_column = stats.add_sma(df, window=3, column=rsi_column)
        df, rsi_column = stats.add_sma(df, window=3, column=rsi_column)

        df.loc[:, "evolution_rsi"] = (df[rsi_column] - df[rsi_column].shift(1)).rolling(window=self.evolution_window).min()
        df.loc[:, "rsi_increasing"] = df["evolution_rsi"] > 0

        df.loc[:, "price_evolution"] = (df[ema_column] - df[ema_column].shift(1)).rolling(window=self.evolution_window).max()
        df.loc[:, "price_decreasing"] = df["price_evolution"] < 0

        df.loc[:, "fake_entry"] = (df["rsi_increasing"]) & (df["price_decreasing"])

        df.loc[:, "ema_increasing"] = df[ema_column] > df[ema_column].shift(1)

        df.loc[:, "entry"] = (df["ema_increasing"]) & (df["fake_entry"] | df["fake_entry"].shift(1) | df["fake_entry"].shift(2) | df["fake_entry"].shift(3) | df["fake_entry"].shift(4))

        df, atr_col = stats.add_atr(df, window=14)
        df, smoothed_atr_col = stats.add_sma(df, column=atr_col, window=5)
        df.loc[:, "evolution_atr"] = (df[smoothed_atr_col] - df[smoothed_atr_col].shift(1)).rolling(window=5).max()
        df.loc[:, "atr_decreasing"] = df["evolution_atr"] < 0

        df, sma = stats.add_sma(df, window=52)
        df.loc[:, "sma_diff"] =(df[sma] - df[sma].shift(1)).rolling(window=10).max()
        df.loc[:, "sma_decreasing"] = df["sma_diff"] < 0

        # go-on condition
        df.loc[:, "ema_good"] = (df[ema_column] - df[ema_column].shift(1)) > 0
        df.loc[:, "go-on"] = True
        return df

    @staticmethod
    def get_grid():
        return [
            [12, 15, 21],
            [5, 7, 10],
            [8],
            [15],
            [7, 10, 15]
        ]

    @staticmethod
    def get_hint_columns() -> List[str]:
        return ["atr_decreasing"]
