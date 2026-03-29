import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_market_data():
    print("--- Robot Starting: Surgical Fetch ---")
    entry = {"date": datetime.now().strftime("%Y-%m-%d"), "gold_price": 0, "dxy": 0, "poly_prob": 0, "market": "Search Failed"}

    # 1. Fetch Friday's Close (Handles Sunday perfectly)
    try:
        gold = yf.Ticker("GC=F").history(period="5d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="5d")['Close'].iloc[-1]
        entry["gold_price"], entry["dxy"] = round(gold, 2), round(dxy, 2)
        print(f"Success: Gold ${entry['gold_price']} | DXY {entry['dxy']}")
    except:
        print("!! Finance Data Error")

    # 2. Fetch Polymarket Sentiment (The Surgical Decoder)
    try:
        # We search specifically for the Gold daily market
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold%20XAUUSD"
        resp = requests.get(url).json()
        
        for event in resp:
            title = event.get('title', '').upper()
            if "XAUUSD" in title or "GOLD (GC)" in title:
                market = event['markets'][0]
                
                # THE DECODER: This handles strings, lists, and brackets
                raw_prices = market.get('outcomePrices')
                if isinstance(raw_prices, str):
                    prices = json.loads(raw_prices)
                else:
                    prices = raw_prices
                
                entry["poly_prob"] = round(float(prices[0]) * 100, 2)
                entry["market"] = market.get('question', 'Gold Daily')
                print(f"Polymarket Decoded: {entry['poly_prob']}%")
                break
    except Exception as e:
        print(f"!! Polymarket Decode Error: {e}")
        entry["market"] = "Parse Error"

    return entry

# --- Save & Rolling logic ---
new_row = get_market_data()
file = "gold_data.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file):
    df_old = pd.read_csv(file)
    # This prevents duplicate dates and keeps the last 30 entries (Rolling Window)
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print("--- Robot Task Complete ---")
