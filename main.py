import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import sys
import json

def get_market_data():
    print("--- 🎯 Surgical Strike: Target XAUUSD 🎯 ---")
    
    # FIX: This must be the hyphenated version from the URL
    TARGET_SLUG = "xauusd-up-or-down-on-march-30-2026"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0,
        "dxy_index": 0.0,
        "poly_prob": 0.0,
        "market_name": "No Data Found"
    }

    # 1. Fetch Finance Data
    try:
        gold = yf.Ticker("GC=F").history(period="5d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="5d")['Close'].iloc[-1]
        entry["gold_price"], entry["dxy_index"] = round(gold, 2), round(dxy, 2)
        print(f"✅ Finance: Gold ${entry['gold_price']} | DXY {entry['dxy_index']}")
    except:
        print("!! Finance Data Failed")

    # 2. Fetch Polymarket (Direct Slug Method)
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={TARGET_SLUG}"
        resp = requests.get(url).json()
        
        if resp and len(resp) > 0:
            event = resp[0]
            market = event['markets'][0]
            
            # THE FIX: Safely turn the string '["0.49", "0.51"]' into a real number
            raw_prices = market.get('outcomePrices')
            if isinstance(raw_prices, str):
                prices = json.loads(raw_prices)
            else:
                prices = raw_prices
            
            # prices[0] is 'Up' / 'Yes'
            entry["poly_prob"] = round(float(prices[0]) * 100, 2)
            entry["market_name"] = event.get('title')
            print(f"🚀 SUCCESS: {entry['market_name']} is {entry['poly_prob']}%")
        else:
            print(f"⚠️ Slug '{TARGET_SLUG}' not found. Checking search backup...")
            # Backup Search
            backup = requests.get("https://gamma-api.polymarket.com/events?active=true&q=XAUUSD").json()
            for e in backup:
                if "XAUUSD" in e['title'].upper():
                    entry["poly_prob"] = round(float(json.loads(e['markets'][0]['outcomePrices'])[0]) * 100, 2)
                    entry["market_name"] = e['title']
                    break

    except Exception as e:
        print(f"!! Polymarket Error: {e}")

    return entry

# --- Execution & Saving ---
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
print("--- 🏁 Robot Task Complete ---")
