from fastapi import FastAPI
from tasks import execute_macd, execute_earnings

from pydantic import BaseModel


app = FastAPI()


@app.get("/")
async def root():
    return {"status": "up"}


@app.get("/macd")
async def macd(
        min_top_ticker: int = 0,
        max_top_ticker: int = 300,
        week_previous_entries: int = 0
):
    execute_macd.delay(
        min_top_ticker=min_top_ticker,
        max_top_ticker=max_top_ticker,
        week_previous_entries=week_previous_entries
    )
    return {"message": "Strategy will be executed, results will be sent by email !"}


@app.get("/earnings")
async def earnings(
        min_top_ticker: int = 0,
        max_top_ticker: int = 300,
):
    execute_earnings.delay(
        min_top_ticker=min_top_ticker,
        max_top_ticker=max_top_ticker,
    )
    return {"message": "Strategy will be executed, results will be sent by email !"}
