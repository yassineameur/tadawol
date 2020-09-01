# strategy
# EMA 50 > EMA 20
# EMA 10 > EMA 20
# EMA 15 is progressing positively during 10 days
# See whether we can wait many times for the signal


####################
# BEST CONFIG
# 40,15,7,15,20,10 #
####################

# OTHER IDEAS ###
# when we have new 50 day record
# When we get to a new level: 5, 10, 20, 50, 75, 100, 500, 1000, 1500
# when there is a split
# exit strategy: when price is bearish twice, get out immediately
# Coder l'inverse d'une stratégie
###########
import pandas as pd

from ..strategies import base_strategy
from tadawol import stats


class Recovery(base_strategy.BaseStrategy):
    def __init__(
            self,
            long_window: int = 40,
            medium_window: int = 15,
            short_window: int = 7,
            max_lose_percent: int = 8,
            max_win_percent: int = 20,
            max_keep_days: int = 10
    ):
        assert short_window < medium_window < long_window
        self.long_window = long_window
        self.medium_window = medium_window
        self.short_window = short_window

        super().__init__(
            max_lose_percent=max_lose_percent,
            max_win_percent=max_win_percent,
            max_keep_days=max_keep_days)

    def add_entries_for_ticker(self, ticker_data: pd.DataFrame):
        ticker_data.sort_values(by="Date", ascending=True, inplace=True)
        ticker_data.reset_index(drop=True, inplace=True)
        assert ticker_data["Ticker"].nunique() == 1

        df, long_window_ema_column = stats.add_ema(ticker_data, window=self.long_window)
        df, medium_window_ema_column = stats.add_ema(df, window=self.medium_window)
        df, short_window_ema_column = stats.add_ema(df, window=self.short_window)

        positive_slope_days = 10
        df, positive_slope_column = stats.add_ema(df, window=positive_slope_days, column=short_window_ema_column)
        df.loc[:, "slope_diff"] = df[positive_slope_column] - df[positive_slope_column].shift(1)
        df.loc[:, "min_diff"] = df["slope_diff"].rolling(positive_slope_days).min()

        df.loc[:, "entry"] = (df["min_diff"] > 0) & (df[long_window_ema_column] > df[medium_window_ema_column]) & (df[short_window_ema_column] > df[medium_window_ema_column])
        return df

    @staticmethod
    def get_grid():
        return [
            [40, 50],
            [15, 20],
            [7, 10],
            [10, 15],
            [15, 20],
            [7, 10]
        ]

