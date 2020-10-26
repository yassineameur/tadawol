from typing import List
import pandas as pd

from ..strategies import base_strategy
from tadawol import stats

# 15, 30, 9


class MACD(base_strategy.BaseStrategy):

    def __init__(
            self,
            short_window: int = 12,
            long_window: int = 30,
            macd_window: int = 9,
            ema_window_search: int = 5,
            max_lose_percent: int = 8,
            max_win_percent: int = 15,
            max_keep_days: int = 15
    ):
        assert short_window < long_window

        super().__init__(
            max_lose_percent=max_lose_percent,
            max_win_percent=max_win_percent,
            max_keep_days=max_keep_days,
        )

        self.short_window = short_window
        self.long_window = long_window
        self.macd_window = macd_window
        self.ema_window_search = ema_window_search

        self.name = "MACD"

    def add_entries_for_ticker(self, ticker_data: pd.DataFrame):
        ticker_data = ticker_data.copy(deep=True)
        ticker_data.sort_values(by="Date", ascending=True, inplace=True)
        ticker_data.reset_index(drop=True, inplace=True)
        assert ticker_data["Ticker"].nunique() == 1

        df, long_window_ema_column = stats.add_ema(ticker_data, window=self.long_window)
        df, short_window_ema_column = stats.add_ema(df, window=self.short_window)
        df["macd"] = df[short_window_ema_column] - df[long_window_ema_column]

        df, macd_signal = stats.add_ema(df, window=self.macd_window, column="macd")

        df.loc[:, "emas_diff"] = df[macd_signal] - df["macd"]
        df, ema_diff = stats.add_ema(df, window=3, column="emas_diff")
        df.loc[:, "evolution_emas_diff"] = (df[ema_diff] - df[ema_diff].shift(1)).rolling(window=self.ema_window_search).min()

        df, atr_col = stats.add_atr(df, window=14)
        df, smoothed_atr_col = stats.add_sma(df, column=atr_col, window=3)
        df.loc[:, "evolution_atr"] = (df[smoothed_atr_col] - df[smoothed_atr_col].shift(1)).rolling(window=5).max()
        df.loc[:, "atr_decreasing"] = df["evolution_atr"] < 0

        df, ema_21 = stats.add_ema(ticker_data, window=21)
        df, smoothed_ema_21 = stats.add_ema(ticker_data, window=2, column=ema_21)
        df.loc[:, "ema_evolution"] = (df[smoothed_ema_21] - df[smoothed_ema_21].shift(1)).rolling(window=5).min()
        df.loc[:, "ema_increasing"] = df["ema_evolution"] > 0

        df, rsi_column = stats.add_rsi(df)

        df.loc[:, "entry"] = (df["evolution_emas_diff"] > 0) & (df["emas_diff"] < 0)

        # go-on condition
        df.loc[:, "good_evolution"] = df["evolution_emas_diff"] > 0
        df.loc[:, "go-on"] = df["good_evolution"] | df["good_evolution"].shift(1) | df["good_evolution"].shift(2) | df["good_evolution"].shift(3) | df["good_evolution"].shift(4) | df["good_evolution"].shift(5)
        return df

    @staticmethod
    def get_grid():
        return [
            [9, 12, 15],
            [22, 26, 30],
            [9, 6],
            [5, 7],
            [8],
            [15],
            [7, 10, 15]
        ]

    @staticmethod
    def get_hint_columns() -> List[str]:
        return ["atr_decreasing", "ema_increasing"]
