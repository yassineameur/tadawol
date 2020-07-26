from typing import Tuple
import pandas as pd


def add_ema(df: pd.DataFrame, window:int, column:str="Close") -> Tuple[pd.DataFrame, str]:
    column_name = f"{column}_ema_{window}"
    df[column_name] = df[column].ewm(span=window).mean()

    return df, column_name


def add_sma(df: pd.DataFrame, window:int, column:str="Close") -> Tuple[pd.DataFrame, str]:
    column_name = f"{column}_sma_{window}"
    df[column_name] = df[column].rolling(window=window).mean()

    return df, column_name


def add_macd(
        df: pd.DataFrame,
        fast_length: int = 12,
        slow_length: int = 26,
        signal_smoothing: int = 9,
        column: str = "Close"
) -> Tuple[pd.DataFrame, str]:
    fast_ema = df[column].ewm(span=fast_length).mean()
    slow_ema = df[column].ewm(span=slow_length).mean()

    diff = fast_ema - slow_ema
    diff_ema = (fast_ema - slow_ema).ewm(span=signal_smoothing).mean()
    column_name = f"{column}_MACD_{fast_length}_{slow_length}_diff"
    df[column_name] = diff - diff_ema
    return df, column_name
