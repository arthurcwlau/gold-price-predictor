import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import sys

def get_market_data():
    print("--- 🎯 Precision Search: Starting ---")
    # Clean column names for a clean CSV
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0,
        "dxy_index": 0.0,
        "poly_prob": 0.0,
        "market_name": "No Market Found"
    }

    # 1. Fetch Finance Data
    try:
        gold = yf.Ticker("GC=F").history(period="5d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="5d")['Close'].iloc[-1]
        entry["gold_price"], entry["dxy_index"] = round(gold, 2), round(dxy, 2)
        print(f"Finance: Gold ${entry['gold_price']} | DXY {entry['dxy_index']}")
    except:
        print("!! Finance Data Failed")

    # 2. Fetch Polymarket (The Fuzzy Hunter)
    try:
        # We search for 'Gold' generally and filter manually in Python
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
        resp = requests.get(url).json()
        
        for event in resp:
            title = event.get('title', '').upper()
            # We look for the "Up or Down" style market specifically
            if "UP OR DOWN" in title and ("XAU" in title or "GOLD" in title):
                market = event['markets'][0]
                prices = market.get('outcomePrices', ["0.5"])
                entry["poly_prob"] = round(float(prices[0]) * 100, 2)
                entry["market_name"] = title
                print(f"✅ Found Match: {title} at {entry['poly_prob']}%")
                break
    except:
        print("!! Polymarket Search Failed")

    return entry

# --- Save Logic ---
new_row = get_market_data()
file = "gold_data.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file):
    df_old = pd.read_csv(file)
    # Keep it clean: Merge and remove duplicate dates
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print("--- 🏁 Robot Task Finished ---")
