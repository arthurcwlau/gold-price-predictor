import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os

def get_market_data():
    # 1. Fetch Gold Futures and Dollar Index from Yahoo Finance
    # GC=F is Gold, DX-Y.NYB is the US Dollar Index
    tickers = yf.Tickers('GC=F DX-Y.NYB')
    gold_price = tickers.tickers['GC=F'].fast_info['last_price']
    dxy_index = tickers.tickers['DX-Y.NYB'].fast_info['last_price']

    # 2. Fetch Polymarket Probability (Gold Up/Down Market)
    url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
    poly_data = requests.get(url).json()
    
    try:
        # Find the specific market for "Gold Up or Down"
        market = poly_data[0]['markets'][0]
        poly_prob = float(market['outcomePrices'][0]) * 100 # Convert to %
        market_name = market['question']
    except:
        poly_prob = "N/A"
        market_name = "Market Not Found"

    return {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": round(gold_price, 2),
        "dxy_index": round(dxy_index, 2),
        "poly_up_prob": poly_prob,
        "market_context": market_name
    }

# Update the CSV
data = get_market_data()
df_new = pd.DataFrame([data])

file_name = "gold_rolling_data.csv"
if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_combined = pd.concat([df_old, df_new]).tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"Logged: Gold ${data['gold_price']} | DXY {data['dxy_index']} | Poly {data['poly_up_prob']}%")
