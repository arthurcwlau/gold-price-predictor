import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import sys

def get_data():
    print("--- DEBUG: Starting Data Fetch ---")
    # We prepare a default entry in case of errors
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"), 
        "gold_price": 0, 
        "dxy": 0, 
        "poly_prob": 0, 
        "market": "Error"
    }

    # 1. Yahoo Finance (Handling Sunday/Weekends)
    try:
        print("Fetching Yahoo Finance...")
        # We fetch 1 month to ensure we have valid Friday data on a Sunday
        gold_data = yf.Ticker("GC=F").history(period="1mo")
        dxy_data = yf.Ticker("DX-Y.NYB").history(period="1mo")
        
        if not gold_data.empty:
            entry["gold_price"] = round(gold_data['Close'].iloc[-1], 2)
            entry["dxy"] = round(dxy_data['Close'].iloc[-1], 2)
            print(f"Finance Success: Gold ${entry['gold_price']}")
    except Exception as e:
        print(f"!! Finance Error: {e}")

    # 2. Polymarket (Flexible Search for Gold/XAU)
    try:
        print("Searching Polymarket...")
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
        resp = requests.get(url).json()
        
        for event in resp:
            title = event.get('title', '').upper()
            # Logic: Must contain GOLD or XAU, but NOT BITCOIN
            if ("GOLD" in title or "XAU" in title) and "BITCOIN" not in title:
                market = event['markets'][0]
                # Safely get prices (outcomePrices is usually a list)
                prices = market.get('outcomePrices', [0.5, 0.5])
                entry["poly_prob"] = round(float(prices[0]) * 100, 2)
                entry["market"] = market.get('question', 'Gold Trend')
                print(f"Polymarket Success: {entry['poly_prob']}%")
                break
    except Exception as e:
        print(f"!! Polymarket Error: {e}")

    return entry

# --- Execution & Saving ---
data_point = get_data()
df_new = pd.DataFrame([data_point])
file_path = "gold_data.csv"

if os.path.exists(file_path):
    df_old = pd.read_csv(file_path)
    # This prevents duplicate rows for the same date
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date']).tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_path, index=False)
print("--- SCRIPT FINISHED SUCCESSFULLY ---")
