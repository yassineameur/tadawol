from typing import Set, Dict
import os
from datetime import datetime, timedelta
import logging

from yahoo_historical.fetch import Fetcher
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


DEFAULT_START_DATE = datetime(2010, 1, 15)
DATA_PATH = os.path.join(os.getcwd(), 'tadawol/data')

TICKERS_LIST_PATH = os.path.join(DATA_PATH, "tickers_list.csv")
STOCKS_HISTORY_PATH = os.path.join(DATA_PATH, "history.csv")


def get_stock_data(ticker: str, start_date: datetime) -> pd.DataFrame:
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
    df = df[df["Date"] > datetime(2018, 7, 20)]
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


def update_data():
    start_date_per_ticker = get_last_update_date_per_ticker()
    logger.info("Fetching data for {} tickers".format(len(start_date_per_ticker)))

    failed_tickers = []
    data = []

    def insert_data(data):
        if len(data) > 0:
            stocks_df = pd.concat(data, axis=0).reset_index(drop=True)
            stocks_df.to_csv(STOCKS_HISTORY_PATH, mode='a', header=False)
            logger.info(
                "{} tickers data is inserted".format(
                    len(data)
                )
            )
            data = []
        else:
            logger.info("No data is inserted")
        return data

    current_tickers_number = 0
    for ticker, start_date in start_date_per_ticker.items():
        try:
            current_tickers_number += 1
            ticker_data = get_stock_data(ticker, start_date)
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
            if current_tickers_number % 50 == 0:
                logger.info(
                    'Treated {}% of tickers'.format(
                        round(100 * current_tickers_number/len(start_date_per_ticker))
                    )
                )
                if len(data) > 0:
                    data = insert_data(data)

    if len(failed_tickers) > 0:
        logging.error("Failed to fetch data for {} ticker(s): {}".format(len(failed_tickers), failed_tickers))

    insert_data(data)


def check_data():
    df = get_historical_data()

    tickers_number = df['Ticker'].nunique()
    logger.info(f'Tickers number = {tickers_number}')
    for ticker, ticker_data in df.groupby(["Ticker"]):
        logger.info(f"Checking {ticker} ...")
        rows_number = ticker_data.shape[0]
        dates_number = ticker_data['Date'].nunique()
        if rows_number != dates_number:
            raise Exception(f"{ticker}: rows_number = {rows_number}, dates_number = {dates_number}")

        ticker_data.sort_values(by=["Date"], inplace=True)
        ticker_data['date_diff'] = df['Date'] - df['Date'].shift(1)
        ticker_data = ticker_data[1:]
        if ticker_data['date_diff'].max().days > 5:
            raise Exception(f"{ticker}: Max difference more than 5 days")
        if ticker_data['date_diff'].min().days != 1:
            raise Exception(f"{ticker}: Min difference  less than 1 day")

    logger.info("Data is good !")


def delete_date():

    historical_data = get_historical_data()
    before = historical_data.shape[0]
    historical_data = historical_data[historical_data["Date"] != datetime(2020, 7, 24)]
    after = historical_data.shape[0]
    historical_data.to_csv(STOCKS_HISTORY_PATH)


if __name__ == '__main__':
    data = get_historical_data()
    data = data[data["Ticker"] == "ACI"]
    data = data[data["Date"] > datetime(2017, 5, 1)]
    print(data.head(30))


# hasura migrations
# tests manuels de ma mr: avec la partie update
# j'ai commencé la partie partage des fichiers

