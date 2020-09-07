import pandas as pd

from ..strategies import base_strategy
from tadawol import stats

# 15, 30, 9

class MACD(base_strategy.BaseStrategy):
    def __init__(
            self,
            short_window: int = 15,
            long_window: int = 30,
            macd_window: int = 9,
            max_lose_percent: int = 8,
            max_win_percent: int = 15,
            max_keep_days: int = 15
    ):
        assert short_window < long_window

        self.short_window = short_window
        self.long_window = long_window
        self.macd_window = macd_window

        super().__init__(
            max_lose_percent=max_lose_percent,
            max_win_percent=max_win_percent,
            max_keep_days=max_keep_days,
        )

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
        df, ema_diff = stats.add_ema(df, window=2, column="emas_diff")
        df.loc[:, "evolution_emas_diff"] = (df[ema_diff] - df[ema_diff].shift(1)).rolling(window=5).min()

        df, ema_column = stats.add_ema(df, window=5)
        df.loc[:, "good_ema"] = df[ema_column] < 1 * df[ema_column].shift(1)

        df.loc[:, "entry"] = (df["evolution_emas_diff"] > 0) & (df["emas_diff"] < 0) #& (df["good_ema"])



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
            [8],
            [15],
            [7, 10, 15]
        ]
