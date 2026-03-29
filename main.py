import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os

def get_data():
    data_point = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0,
        "dxy_index": 0,
        "poly_up_prob": 0,
        "market_name": "N/A"
    }

    # 1. Fetch Finance Data (with error handling)
    try:
        print("Fetching Yahoo Finance data...")
        # Get Gold (GC=F) and Dollar Index (DX-Y.NYB)
        gold = yf.Ticker("GC=F").history(period="1d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="1d")['Close'].iloc[-1]
        data_point["gold_price"] = round(gold, 2)
        data_point["dxy_index"] = round(dxy, 2)
    except Exception as e:
        print(f"Finance Error: {e}")

    # 2. Fetch Polymarket Data (with error handling)
    try:
        print("Fetching Polymarket data...")
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
        response = requests.get(url).json()
        if response and len(response) > 0:
            market = response[0]['markets'][0]
            data_point["poly_up_prob"] = float(market['outcomePrices'][0]) * 100
            data_point["market_name"] = market['question']
        else:
            print("No active Gold markets found on Polymarket right now.")
    except Exception as e:
        print(f"Polymarket Error: {e}")

    return data_point

# --- Main Logic ---
new_entry = get_data()
df_new = pd.DataFrame([new_entry])

file_name = "gold_rolling_data.csv"
if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_combined = pd.concat([df_old, df_new]).tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"Successfully saved data for {new_entry['date']}")
