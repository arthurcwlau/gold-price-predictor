import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import sys

def get_market_data():
    print("--- 🎯 Surgical Strike: Target XAUUSD 🎯 ---")
    
    # This is the exact identifier for the market in your link
    TARGET_SLUG = "xauusd-up-or-down-on-march-30-2026"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0,
        "dxy_index": 0.0,
        "poly_prob": 0.0,
        "market_name": "Target Found"
    }

    # 1. Fetch Finance Data (Standard Weekend-Safe)
    try:
        gold = yf.Ticker("GC=F").history(period="5d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="5d")['Close'].iloc[-1]
        entry["gold_price"], entry["dxy_index"] = round(gold, 2), round(dxy, 2)
        print(f"✅ Finance: Gold ${entry['gold_price']} | DXY {entry['dxy_index']}")
    except:
        print("!! Finance Data Failed")

    # 2. Fetch Polymarket (Direct Slug Method)
    try:
        # We query for the specific slug you shared
        url = f"https://gamma-api.polymarket.com/events?slug={TARGET_SLUG}"
        resp = requests.get(url).json()
        
        if resp and len(resp) > 0:
            event = resp[0]
            market = event['markets'][0]
            
            # Polymarket prices can be list or string in 2026
            prices = market.get('outcomePrices', ["0.5"])
            
            # The prices are usually strings, we turn them into numbers
            entry["poly_prob"] = round(float(prices[0]) * 100, 2)
            entry["market_name"] = event.get('title', 'Gold Daily Trend')
            print(f"🚀 SUCCESS: {entry['market_name']} at {entry['poly_prob']}%")
        else:
            print(f"⚠️ Slug {TARGET_SLUG} not found. Checking active markets...")
            # Backup: search specifically for 'XAUUSD'
            backup_url = "https://gamma-api.polymarket.com/events?active=true&q=XAUUSD"
            backup_resp = requests.get(backup_url).json()
            for e in backup_resp:
                if "XAUUSD" in e['title'].upper():
                    entry["poly_prob"] = round(float(e['markets'][0]['outcomePrices'][0]) * 100, 2)
                    entry["market_name"] = e['title']
                    break

    except Exception as e:
        print(f"!! Polymarket Error: {e}")

    return entry

# --- Execution ---
new_row = get_market_data()
file = "gold_data.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file):
    df_old = pd.read_csv(file)
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print("--- 🏁 Robot Task Complete ---")
