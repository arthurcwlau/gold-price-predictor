import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
import sys
import json

def get_market_data():
    print("--- 🤖 Robot Starting: Precision Hunt 🤖 ---")
    
    # We'll use tomorrow's date for the Polymarket search because that's usually the target
    target_date = (datetime.now() + timedelta(days=1)).strftime("%B-%d-%Y").lower()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    entry = {
        "date": today_str,
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
        print(f"✅ Finance: Gold ${entry['gold_price']} | DXY {entry['dxy_index']}")
    except Exception as e:
        print(f"❌ Finance Error: {e}")

    # 2. Fetch Polymarket (The Precision Hunt)
    try:
        # Search for XAUUSD specifically (The gold code)
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=XAUUSD"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers).json()
        
        found = False
        if resp:
            for event in resp:
                title = event.get('title', '').upper()
                print(f"Checking: {title}")
                
                # Filter: Must be the 'Up or Down' market and NOT Bitcoin
                if "UP OR DOWN" in title and "XAUUSD" in title:
                    market = event['markets'][0]
                    # Polymarket prices are often a list like ["0.68", "0.32"]
                    prices = market.get('outcomePrices', ["0.5"])
                    
                    # Convert to number: 0.68 -> 68.0
                    entry["poly_prob"] = round(float(prices[0]) * 100, 2)
                    entry["market_name"] = title
                    found = True
                    print(f"🎯 MATCH FOUND: {title} at {entry['poly_prob']}%")
                    break
        
        if not found:
            print("⚠️ Fuzzy search failed. Market might be hidden. Recording 50% default.")
            entry["poly_prob"] = 50.0

    except Exception as e:
        print(f"❌ Polymarket API Error: {e}")

    return entry

# --- Save & Append Logic ---
new_row = get_market_data()
file = "gold_data.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file):
    df_old = pd.read_csv(file)
    # Merge, remove duplicates, and keep the latest 30 days
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print(f"--- 🏁 Task Finished: Data saved for {new_row['date']} ---")
