from fastapi import FastAPI
from tasks import execute_macd

from pydantic import BaseModel



app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello world !"}


class MacdStrategyParameters(BaseModel):

    min_top_ticker: int = 0
    max_top_ticker: int = 300
    days_to_next_result: int = 20
    days_since_last_result: int = 0
    week_previous_entries: int = 1


@app.get("/macd")
async def macd(
        min_top_ticker: int = 0,
        max_top_ticker: int = 300,
        days_to_next_result: int = 20,
        days_since_last_result: int = 0,
        week_previous_entries: int = 1
):
    execute_macd.delay(
        min_top_ticker=min_top_ticker,
        max_top_ticker=max_top_ticker,
        days_to_next_result=days_to_next_result,
        days_since_last_result=days_since_last_result,
        week_previous_entries=week_previous_entries
    )
    return {"message": "Strategy will be executed, results will be sent by email !"}
