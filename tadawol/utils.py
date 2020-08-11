from typing import List, Type, Any
import pandas as pd


def clean_results(df: pd.DataFrame):
    df = df[(df["entry"]) & (df["Close"] > 2)]
    df = df[df["Volume"] > 100000]
    df = df[df["win_percent"] < 50]
    df = df[df["win_percent"] > -50]

    return df


def get_search_grid(grid: List[List[Any]]):
    search_grid = [[v] for v in grid[0]]
    grid.pop(0)

    while len(grid) > 0:
        values = grid[0]
        intermediate_search_grid = []
        for v in values:
            for current_list in search_grid:
                new_list = current_list.copy()
                new_list.append(v)
                intermediate_search_grid.append(new_list)
        search_grid = intermediate_search_grid
        grid.pop(0)
    return search_grid


def get_last_week_entries(df) -> pd.DataFrame:

    assert df[df["entry"]].shape[0] == df.shape[0]

    df = df.copy(deep=True)
    tickers_data = []
    for ticker, ticker_data in df.groupby(["Ticker"]):
        ticker_data = ticker_data.sort_values(by="Date", ascending=True)
        for i in range(1, 5):
            ticker_data.loc[:, f"Date_{i}"] = ticker_data["Date"].shift(i)

        def compute_last_week_entries(row):
            entries = 0
            entry_date = row["Date"]
            for i in range(1, 5):
                last_date = row[f"Date_{i}"]
                if not pd.isna(last_date):
                    if (entry_date - last_date).days < 7:
                        entries += 1
            return entries

        ticker_data.loc[:, "week_previous_entries"] = ticker_data.apply(compute_last_week_entries, axis=1)
        tickers_data.append(ticker_data)

    return pd.concat(tickers_data, axis=0)


def print_bad_cases(df: pd.DataFrame, cases_number: int):

    df = df.sort_values(by=["win_percent"], ascending=True)
    print(df.head(cases_number))


