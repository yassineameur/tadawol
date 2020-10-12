from typing import Set, Dict, Optional, List
import os
import time
from datetime import datetime, timedelta
import logging

from yahoo_historical.fetch import Fetcher
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


DEFAULT_START_DATE = datetime(2015, 1, 1)
DATA_PATH = os.path.join(os.getcwd(), 'tadawol/data')

TICKERS_LIST_PATH = os.path.join(DATA_PATH, "tickers_list.csv")
STOCKS_HISTORY_PATH = os.path.join(DATA_PATH, "history.csv")


def get_ticker_data(ticker: str, start_date: datetime, end_date: Optional[datetime] = None) -> pd.DataFrame:
    if end_date is None:
        end_date = datetime.utcnow() - timedelta(days=1)

    assert start_date < end_date

    fetcher = Fetcher(
        ticker=ticker,
        start=[start_date.year, start_date.month, start_date.day],
        end=[end_date.year, end_date.month, end_date.day],
    )
    data = fetcher.get_historical()
    data['Ticker'] = ticker
    data.drop_duplicates(subset=["Date"], inplace=True)
    return data


def get_tickers() -> Set[str]:
    df = pd.read_csv(TICKERS_LIST_PATH)
    return set(df['Ticker'])


def get_historical_data() -> pd.DataFrame:
    df = pd.read_csv(STOCKS_HISTORY_PATH)
    df.loc[:, 'Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d")
    df = df[df["Date"] > DEFAULT_START_DATE]
    logger.info("Historical data is extracted, rows_umber = {}".format(df.shape[0]))
    return df


def get_last_update_date_per_ticker() -> Dict[str, datetime]:
    tickers = get_tickers()
    historical_data = get_historical_data()

    start_date_per_ticker = {}
    for ticker, ticker_data in historical_data.groupby(['Ticker']):
        if ticker in tickers:
            last_date = max(ticker_data['Date'])
            start_date_per_ticker[ticker] = last_date + timedelta(days=1)
    for ticker in tickers:
        if ticker not in start_date_per_ticker:
            start_date_per_ticker[ticker] = DEFAULT_START_DATE

    return start_date_per_ticker


def _insert_data(data):
    if len(data) > 0:
        stocks_df = pd.concat(data, axis=0).reset_index(drop=True)
        stocks_df.to_csv(STOCKS_HISTORY_PATH, mode='a', header=False)
        logger.info(
            "{} tickers data is inserted".format(
                len(data)
            )
        )
    else:
        logger.info("No data is inserted")
    return data


def update_data(tickers_to_update: Optional[List[str]] = None, save_data: bool = True):
    start_date_per_ticker = get_last_update_date_per_ticker()
    if tickers_to_update is not None:
        start_date_per_ticker = {ticker: start_date for ticker, start_date in start_date_per_ticker.items() if ticker in tickers_to_update}

    logger.info("Fetching data for {} tickers".format(len(start_date_per_ticker)))
    failed_tickers = []
    data = []
    added_data = []

    current_tickers_number = 0
    for ticker, start_date in start_date_per_ticker.items():
        try:
            current_tickers_number += 1
            end_date = None
            if not save_data:
                end_date = (datetime.now() + timedelta(days=1)).date()
            ticker_data = get_ticker_data(ticker, start_date, end_date)
        except KeyboardInterrupt as e:
            logging.info('Interrupted by user')
            raise e
        except:
            logging.error("Failed to fetch data for {}".format(ticker))
            failed_tickers.append(ticker)
        else:
            if ticker_data.shape[0] > 0:
                data.append(ticker_data)
                added_data.append(ticker_data)
        finally:
            if current_tickers_number % 10 == 0:
                logger.info(
                    'Treated {}% of tickers'.format(
                        round(100 * current_tickers_number/len(start_date_per_ticker))
                    )
                )
                if len(data) > 0:
                    if save_data:
                        _insert_data(data)
                    data = []

    if len(failed_tickers) > 0:
        logging.error("Failed to fetch data for {} ticker(s): {}".format(len(failed_tickers), failed_tickers))
    if save_data:
        _insert_data(data)

    return pd.concat(added_data, axis=0)


def update_tickers_from_scratch():
    tickers_number = 750
    tickers_list = get_top_tickers(0, tickers_number)
    i = 0
    tickers_number = len(tickers_list)
    for ticker in tickers_list:
        try:
            ticker_data = get_ticker_data(ticker=ticker, start_date=DEFAULT_START_DATE)
            ticker_data.to_csv(STOCKS_HISTORY_PATH, mode='a', header=False)
        except KeyboardInterrupt:
            raise KeyboardInterrupt()
        except:
            logger.error("Fail for ticker {}".format(ticker))

        i += 1
        if i % 20 == 0:
            print("{}/{}".format(i, tickers_number))
            time.sleep(2)


def get_fresh_data(tickers_to_update: List[str], past_days: int = 90):

    logger.info("Fetching data for {} tickers".format(len(tickers_to_update)))
    failed_tickers = []
    data = []

    start_date = (datetime.now() - timedelta(days=past_days)).date()
    end_date = (datetime.now() + timedelta(days=1)).date()

    current_tickers_number = 0
    for ticker in tickers_to_update:
        try:
            current_tickers_number += 1
            ticker_data = get_ticker_data(ticker, start_date, end_date)
        except KeyboardInterrupt as e:
            logging.info('Interrupted by user')
            raise e
        except:
            logging.error("Failed to fetch data for {}".format(ticker))
            failed_tickers.append(ticker)
        else:
            if ticker_data.shape[0] > 0:
                data.append(ticker_data)
        finally:
            if current_tickers_number % 10 == 0:
                logger.info(
                    'Treated {}% of tickers'.format(
                        round(100 * current_tickers_number/len(tickers_to_update))
                    )
                )

    df = pd.concat(data, axis=0)
    df.loc[:, "Date"] = pd.to_datetime(df['Date'])

    return df


def check_data(ticker: Optional[str]):
    df = get_historical_data()
    if ticker is not None:
        df = df[df["Ticker"] == ticker]

    logger.info(f"Min date : {df.Date.min()}")
    logger.info(f"Max date : {df.Date.max()}")

    tickers_number = df['Ticker'].nunique()

    logger.info(f'Tickers number = {tickers_number}')
    for ticker, ticker_data in df.groupby(["Ticker"]):
        logger.info(f"Checking {ticker} ...")
        rows_number = ticker_data.shape[0]
        dates_number = ticker_data['Date'].nunique()
        if rows_number != dates_number:
            raise Exception(f"{ticker}: rows_number = {rows_number}, dates_number = {dates_number}")

        ticker_data.sort_values(by=["Date"], inplace=True)
        ticker_data.reset_index(drop=True, inplace=True)
        ticker_data.loc[:, "last_date"] = df['Date'].shift(1)
        ticker_data['date_diff'] = ticker_data['Date'] - ticker_data['last_date']
        ticker_data = ticker_data[1:]
        if ticker_data['date_diff'].max().days > 5:
            raise Exception(f"{ticker}: Max difference more than 5 days")
        if ticker_data['date_diff'].min().days < 1:

            raise Exception(f"{ticker}: Min difference  less than 1 day")

    logger.info("Data is good !")


def delete_date():

    historical_data = get_historical_data()
    before = historical_data.shape[0]
    historical_data = historical_data[historical_data["Date"] != datetime(2020, 7, 24)]
    after = historical_data.shape[0]
    historical_data.to_csv(STOCKS_HISTORY_PATH)


def get_fresh_data_v2(tickers: Optional[List[str]] = None, days_number=100):

    added_data = update_data(tickers, save_data=False)
    old_data = pd.read_csv(STOCKS_HISTORY_PATH)

    df = pd.concat([added_data, old_data], axis=0).reset_index(drop=True)

    if tickers is not None:
        df = df[df["Ticker"].isin(tickers)]

    limit_date = datetime.utcnow() - timedelta(days=days_number)
    df.loc[:, "Date"] = pd.to_datetime(df['Date'])
    df = df[df["Date"] > limit_date]
    return df


def get_top_tickers(start, end):

    df = pd.read_csv(TICKERS_LIST_PATH)
    df = df[["Ticker", "Market Capitalization"]]
    df.drop_duplicates(inplace=True)
    df.sort_values(by="Market Capitalization", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df.loc[start: end, ]
    return list(df["Ticker"])

