from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from forex import Forex
import asyncio

app = FastAPI()
forex = Forex()
from fastapi.responses import HTMLResponse


@app.get("/")
async def serve_index():
    with open("static/index.html", "r") as file:
        content = file.read()
    return HTMLResponse(content=content)

@app.get("/api/rates/{currency}")
async def get_currency_rates(currency: str):
    rates, last_update = await forex.get_currency_rates(currency, use_cache=True)
    # replace bank codes with bank names
    for rate in rates:
        if rate['Bank'] in forex.bank_names:
            rate['Bank'] = forex.bank_names[rate['Bank']]
    return {
        "rates": rates,
        "last_update": last_update
    }

@app.get("/api/reload")
async def reload_rates():
    
    await forex.reload_rates()
    return {"message": "Rates reloaded"}

if __name__ == "__main__":
    # uvicorn forex_server:app --host 0.0.0.0 --port 80
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=80)