from datetime import datetime, timedelta
import pandas as pd


def simulate_trades(
        df: pd.DataFrame,
        total_amount: int = 30000,
        transaction_min: float = 1800,
        transaction_max: float = 2500,
        max_trades_by_day: int = 3
):

    current_amount = total_amount
    df = df.sort_values(by="Date", ascending=True)
    date = df["Date"].min()

    returned_money_by_date = dict()
    trade_fees = 0
    realized_trades = []
    while date < datetime.now():
        # recuperate money
        money_to_recupere = returned_money_by_date.get(date, 0)
        current_amount += money_to_recupere
        returned_money_by_date[date] = 0

        # see available money
        day_trades = df[df["Date"] == date]
        day_trades_number = min(day_trades.shape[0], max_trades_by_day)

        if day_trades_number == 0 or current_amount / day_trades_number < transaction_min:
            date += timedelta(days=1)
            continue

        money_by_trade = current_amount / day_trades_number
        money_by_trade = min(money_by_trade, transaction_max)

        trade_number = 0
        for _, trade_row in day_trades.iterrows():

            current_traded_tickers = [tr[0] for tr in realized_trades if tr[1] < date and tr[2] > date]
            ticker_to_trade = trade_row["Ticker"]
            if current_traded_tickers.count(ticker_to_trade) >= 2:
                continue

            trade_number += 1

            current_amount -= money_by_trade
            trade_fees += 2
            # add money to exit

            money_to_be_returned = (1 + trade_row["win_percent"] / 100.0) * money_by_trade

            exit_date = date + timedelta(days=trade_row["exit_date"])
            returned_money_on_exit = returned_money_by_date.get(exit_date, 0)
            returned_money_by_date[exit_date] = returned_money_on_exit + money_to_be_returned

            realized_trades.append(
                [
                    ticker_to_trade,
                    date,
                    trade_row["exit"],
                    trade_row["Close"],
                    trade_row["exit_price"],
                    trade_row["win_percent"]
                ]
            )

            if trade_number == day_trades_number:
                break

        date += timedelta(days=1)

    rest_money = 0
    for m in returned_money_by_date.values():
        rest_money += m

    win = rest_money + current_amount
    realized_trades_df = pd.DataFrame(
        data=realized_trades,
        columns=["Ticker", "Date", "Exit date", "Enter price", "Exit price", "win_percent"])
    return win, trade_fees, realized_trades_df
