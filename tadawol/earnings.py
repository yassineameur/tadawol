import os
from datetime import datetime, timedelta
import time
import logging

from yahoo_earnings_calendar import YahooEarningsCalendar
import pandas as pd

from tadawol.history import get_tickers

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


DEFAULT_START_DATE = datetime(2010, 1, 15)
DATA_PATH = os.path.join(os.getcwd(), 'tadawol/data')
CRUDE_EARNINGS_DATA_PATH = os.path.join(DATA_PATH, "earnings_history.csv")
TRANSFORMED_EARNINGS_DATA_PATH = os.path.join(DATA_PATH, "earnings_data.csv")

BIG_NUMBER = 10000


def get_latest_data():
    data = pd.read_csv(CRUDE_EARNINGS_DATA_PATH)
    data.loc[:, "Date"] = pd.to_datetime(data['startdatetime'], format="%Y-%m-%d")

    return data["Date"].max()


def update_data():
    latest_date = get_latest_data()
    print(latest_date)
    on_date = latest_date + timedelta(days=1)
    on_date = datetime(on_date.year, on_date.month, on_date.day)
    #on_date = datetime(2014, 2, 26)
    yec = YahooEarningsCalendar(delay=0.5)
    while on_date <= datetime.utcnow() - timedelta(days=2):
        logger.info(f"[Earnings] Add data on {on_date}")
        data = yec.earnings_on(on_date)
        df = pd.DataFrame(data)
        df.to_csv(CRUDE_EARNINGS_DATA_PATH, mode='a', header=False)

        on_date += timedelta(days=1)
        time.sleep(1)


def get_earnings_df():
    df = pd.read_csv(CRUDE_EARNINGS_DATA_PATH)
    df.loc[:, "Date"] = pd.to_datetime(df['startdatetime'], format="%Y-%m-%d").map(lambda x: datetime(x.year, x.month, x.day))
    return df


def get_earnings_data():
    df = get_earnings_df()
    all_tickers = get_tickers()
    data = []
    tickers_number = df["ticker"].nunique()
    logger.info(f"Tickers number = {tickers_number}")
    treated_tickers_number = 0
    for ticker, ticker_earnings in df.groupby(["ticker"]):
        if not ticker in all_tickers:
            continue
        ticker_earnings = ticker_earnings.sort_values(by="Date", ascending=True)
        ticker_earnings.reset_index(drop=True, inplace=True)
        ticker_earnings.loc[:, "last_date"] = ticker_earnings["Date"].shift(1)
        ticker_earnings.loc[:, "next_date"] = ticker_earnings["Date"].shift(-1)

        for row_number, row_data in ticker_earnings.iterrows():
            next_earnings_date = row_data["next_date"]
            earnings_date = row_data["Date"]
            company_short_name = row_data["companyshortname"]
            earnings_estimate = row_data["epsestimate"]
            real_earnings = row_data["epsactual"]
            earnings_surprise = row_data["epssurprisepct"]
            if pd.isna(next_earnings_date):
                next_earnings_date = datetime(2021, 1, 1)
            days_number = (next_earnings_date - earnings_date).days
            for i in range(days_number):
                date = earnings_date + timedelta(days=i)
                data.append(
                    [ticker, company_short_name, date, earnings_date, next_earnings_date, earnings_estimate, real_earnings, earnings_surprise]
                )

        treated_tickers_number += 1
        if treated_tickers_number % 100 == 0:
            logger.info(f"[Earnings] Treated tickers number = {round(100.0 * treated_tickers_number / tickers_number)}%")

    df = pd.DataFrame(
        data=data,
        columns=[
            "Ticker", "company_short_name", "Date",
            "earnings_date", "next_earnings_date",
            "earnings_estimate", "real_earnings", "earnings_surprise"
        ]
    )

    df.to_csv("earnings_data.csv")


if __name__ == "__main__":
    """
    df = pd.read_csv("earnings_data.csv")
    print(df.shape)
    df.sort_values(by="Date", ascending=False)
    df = df[df["Ticker"] == "MSFT"]
    print(df.tail())
    """
    df = pd.read_csv(CRUDE_EARNINGS_DATA_PATH)
    date_column = pd.to_datetime(df['startdatetime'], format="%Y-%m-%d")
    from datetime import timezone
    df = df[date_column < datetime(2020, 1, 1, tzinfo=timezone.utc)]
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df.to_csv(CRUDE_EARNINGS_DATA_PATH)


