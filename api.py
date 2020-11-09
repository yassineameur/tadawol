from fastapi import FastAPI
from tasks import execute_macd_reverse_strategies


app = FastAPI()


@app.get("/")
async def root():
    return {"status": "up"}


@app.get("/macd_and_reverse")
async def macd_reverse(
        min_top_ticker: int = 0,
        max_top_ticker: int = 500
):
    execute_macd_reverse_strategies.delay(
        min_top_ticker=min_top_ticker,
        max_top_ticker=max_top_ticker,
    )
    return {"message": "Strategies will be executed, results will be sent by email !"}
