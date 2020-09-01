import os
from datetime import datetime, timedelta
import time
import logging
from typing import Optional
from yahoo_earnings_calendar import YahooEarningsCalendar
import pandas as pd

from tadawol.history import get_tickers

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


DEFAULT_START_DATE = datetime(2010, 1, 15)
DATA_PATH = os.path.join(os.getcwd(), 'tadawol/data')
CRUDE_EARNINGS_DATA_PATH = os.path.join(DATA_PATH, "earnings_history.csv")
TRANSFORMED_EARNINGS_DATA_PATH = os.path.join(DATA_PATH, "earnings_data.csv")

BIG_NUMBER = 10000


def get_latest_data():
    data = get_earnings_df()
    return data["Date"].max()


def update_data():
    latest_date = get_latest_data()
    on_date = latest_date + timedelta(days=1)
    on_date = datetime(on_date.year, on_date.month, on_date.day)
    yec = YahooEarningsCalendar(delay=1)
    while on_date <= datetime.utcnow() - timedelta(days=2):
        logger.info(f"[Earnings] Add data on {on_date}")
        data = yec.earnings_on(on_date)
        df = pd.DataFrame(data)
        df.to_csv(CRUDE_EARNINGS_DATA_PATH, mode='a', header=False)

        on_date += timedelta(days=1)
        time.sleep(1)


def get_earnings_df():
    df = pd.read_csv(CRUDE_EARNINGS_DATA_PATH)
    df.loc[:, "Date"] = pd.to_datetime(df['startdatetime'], format="%Y-%m-%d").\
        map(lambda x: datetime(x.year, x.month, x.day))
    df.dropna(how="any", inplace=True)
    df.drop_duplicates(subset=["ticker", "Date", "epsestimate", "epsactual", "epssurprisepct"], keep="first", inplace=True)
    return df


def get_earnings_data_on_all_dates(reference_df: pd.DataFrame):
    reference_df = reference_df.copy(deep=True)
    reference_df.loc[:, "Date"] = pd.to_datetime(reference_df['Date'])
    assert "Ticker" in reference_df.columns
    assert "Date" in reference_df.columns

    earnings_df = get_earnings_df()
    data = []
    tickers_number = reference_df["Ticker"].nunique()
    logger.info(f"Tickers number = {tickers_number}")
    treated_tickers_number = 0
    for ticker, ticker_earnings in earnings_df.groupby(["ticker"]):
        ticker_reference_data = reference_df[reference_df["Ticker"] == ticker]
        if ticker_reference_data.shape[0] == 0:
            continue

        ticker_dates = [pd.to_datetime(d) for d in ticker_reference_data["Date"].unique()]
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
                ticker_format_date = datetime(date.year, date.month, date.day)
                if ticker_format_date in ticker_dates:
                    data.append(
                        [ticker, company_short_name, ticker_format_date, earnings_date, next_earnings_date, earnings_estimate, real_earnings, earnings_surprise]
                    )

        treated_tickers_number += 1
        if treated_tickers_number % 100 == 0:
            logger.info(f"[Earnings] Treated tickers number = {round(100.0 * treated_tickers_number / tickers_number)}%")

    logger.info(f"Putting data into dataframe ...: rows number = {len(data)}")
    res_df = pd.DataFrame(
        data=data,
        columns=[
            "Ticker", "company_short_name", "Date",
            "earnings_date", "next_earnings_date",
            "earnings_estimate", "real_earnings", "earnings_surprise"
        ]
    )

    def get_days_diff(x, y):
        return round((x - y).total_seconds() / (60 * 60 * 24))

    logger.info(f"Adding features ...")
    res_df.loc[:, "days_since_last_result"] = res_df.apply(lambda x: get_days_diff(x["Date"], x["earnings_date"]), axis=1)
    res_df.loc[:, "days_to_next_result"] = res_df.apply(lambda x: get_days_diff(x["next_earnings_date"], x["Date"]), axis=1)
    return res_df


def check_data(ticker: Optional[str] = None):

    df = get_earnings_df()
    if ticker is not None:
        df = df[df["ticker"] == ticker]

    if df.shape[0] == 0:
        logger.info("No data")
        return

    i = 0
    for ticker, ticker_data in df.groupby(["ticker"]):

        dates_number = ticker_data["Date"].nunique()

        if dates_number != ticker_data.shape[0]:
            logger.info(ticker_data.groupby("Date").size())
            logger.info("******")
            logger.info(ticker_data)
            raise Exception(f"Data is not coherent for {ticker}: dates_number = {dates_number}, data_shape = {ticker_data.shape[0]}")

        if i % 100 == 0:
            logging.info(f"Checking info for {ticker}")
        i += 1
    logger.info("Data is good")


if __name__ == "__main__":
    df = pd.read_csv(CRUDE_EARNINGS_DATA_PATH)
    date_column = pd.to_datetime(df['startdatetime'], format="%Y-%m-%d")
    from datetime import timezone
    df = df[date_column < datetime(2020, 1, 1, tzinfo=timezone.utc)]
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df.to_csv(CRUDE_EARNINGS_DATA_PATH)


