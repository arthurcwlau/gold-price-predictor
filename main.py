import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import sys
import json

def get_market_data():
    print("--- 🎯 Surgical Strike: Fetching Specific Market ---")
    
    # 1. THE TARGET: This is the 'Slug' from the link you sent
    EVENT_SLUG = "xauusd-up-or-down-on-march-30-2026"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0,
        "dxy_index": 0.0,
        "poly_prob": 0.0,
        "market_name": "Fetch Failed"
    }

    # 2. Fetch Finance Data (Standard Weekend-Safe Logic)
    try:
        gold = yf.Ticker("GC=F").history(period="5d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="5d")['Close'].iloc[-1]
        entry["gold_price"], entry["dxy_index"] = round(gold, 2), round(dxy, 2)
        print(f"✅ Finance: Gold ${entry['gold_price']} | DXY {entry['dxy_index']}")
    except:
        print("!! Finance Data Failed")

    # 3. Fetch Polymarket (Direct Slug Method)
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={EVENT_SLUG}"
        resp = requests.get(url).json()
        
        # Polymarket returns a list, we take the first item
        if resp and len(resp) > 0:
            event = resp[0]
            market = event['markets'][0]
            
            # The prices are hidden in a string like '["0.68", "0.32"]'
            # We use json.loads to turn that text into real numbers
            raw_prices = market.get('outcomePrices')
            if isinstance(raw_prices, str):
                prices = json.loads(raw_prices)
            else:
                prices = raw_prices
            
            # Index 0 is usually 'Up' / 'Yes'
            entry["poly_prob"] = round(float(prices[0]) * 100, 2)
            entry["market_name"] = event.get('title', 'Gold Daily Trend')
            print(f"🎯 TARGET REACHED: {entry['market_name']} at {entry['poly_prob']}%")
        else:
            print(f"⚠️ Could not find event with slug: {EVENT_SLUG}")

    except Exception as e:
        print(f"!! Polymarket Error: {e}")

    return entry

# --- Save Logic ---
new_row = get_market_data()
file = "gold_data.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file):
    df_old = pd.read_csv(file)
    # Merges data, cleans duplicates, and keeps 30 days of history
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print("--- 🏁 Robot Task Finished Successfully ---")
