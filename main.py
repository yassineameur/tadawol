from fastapi import FastAPI
from tasks import execute_macd

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello world !"}


@app.get("/macd")
async def macd():
    execute_macd.delay()
    return {"message": "Strategy will be executed, results will be sent by email !"}
