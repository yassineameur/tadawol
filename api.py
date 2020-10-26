from fastapi import FastAPI
from tasks import execute_macd, execute_earnings, execute_reverse


app = FastAPI()


@app.get("/")
async def root():
    return {"status": "up"}


@app.get("/macd")
async def macd(
        min_top_ticker: int = 0,
        max_top_ticker: int = 500
):
    execute_macd.delay(
        min_top_ticker=min_top_ticker,
        max_top_ticker=max_top_ticker,
    )
    return {"message": "Strategy will be executed, results will be sent by email !"}


@app.get("/reverse")
async def reverse(
        min_top_ticker: int = 0,
        max_top_ticker: int = 500
):
    execute_reverse.delay(
        min_top_ticker=min_top_ticker,
        max_top_ticker=max_top_ticker,
    )
    return {"message": "Strategy will be executed, results will be sent by email !"}


@app.get("/earnings")
async def earnings(
        min_top_ticker: int = 0,
        max_top_ticker: int = 500,
):
    execute_earnings.delay(
        min_top_ticker=min_top_ticker,
        max_top_ticker=max_top_ticker,
    )
    return {"message": "Strategy will be executed, results will be sent by email !"}
