###
# This strategy is based on the fact that if stock is reaching a new record, you have to follow it
###
import pandas as pd

from ..strategies import base_strategy
from .. import stats


class Record(base_strategy.BaseStrategy):
    def __init__(
            self,
            smoothing_window: int = 10,
            record_window: int = 50,
            max_lose_percent: int = 15,
            max_win_percent: int = 20,
            max_keep_days: int = 10
    ):
        self.smoothing_window = smoothing_window
        self.record_window = record_window

        super().__init__(
            max_lose_percent=max_lose_percent,
            max_win_percent=max_win_percent,
            max_keep_days=max_keep_days)

    def add_entries_for_ticker(self, ticker_data: pd.DataFrame):
        assert ticker_data["Ticker"].nunique() == 1
        df, ema_column = stats.add_ema(ticker_data, window=self.smoothing_window)
        df, max_column = stats.add_max(df, window=self.record_window, column=ema_column)
        df.loc[:, "entry"] = df[max_column] == df[ema_column]
        return df

    @staticmethod
    def get_grid():
        return [
            [5, 10],
            [30, 40, 50, 60],
            [10, 15],
            [15, 20],
            [7, 10]
        ]
