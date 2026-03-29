import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import sys

def get_market_data():
    print("--- 🤖 Robot Starting: Gold Data Fetch 🤖 ---")
    
    # We create a template for the data row
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0,
        "dxy_index": 0.0,
        "poly_prob": 0.0,
        "market_name": "Search Failed"
    }

    # 1. Fetch Finance Data (Looking back 5 days to handle Sunday/Weekends)
    try:
        print("📡 Connecting to Yahoo Finance...")
        # GC=F is Gold, DX-Y.NYB is the US Dollar Index
        gold_data = yf.Ticker("GC=F").history(period="5d")
        dxy_data = yf.Ticker("DX-Y.NYB").history(period="5d")

        if not gold_data.empty:
            entry["gold_price"] = round(gold_data['Close'].iloc[-1], 2)
            entry["dxy_index"] = round(dxy_data['Close'].iloc[-1], 2)
            print(f"✅ Finance Success: Gold ${entry['gold_price']} | DXY {entry['dxy_index']}")
    except Exception as e:
        print(f"❌ Finance Error: {e}")

    # 2. Fetch Polymarket Sentiment (Surgical Search for XAUUSD)
    try:
        print("📡 Connecting to Polymarket API...")
        # We search specifically for the 'XAUUSD' keyword from your link
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=XAUUSD"
        response = requests.get(url).json()
        
        found_market = False
        for event in response:
            title = event.get('title', '').upper()
            
            # We filter for the specific Gold market, ignoring Bitcoin
            if "XAUUSD" in title or "GOLD" in title:
                market = event['markets'][0]
                
                # Get the "Up" price (probability)
                # Polymarket returns a list like [0.68, 0.32]
                prices = market.get('outcomePrices', [0.5, 0.5])
                
                # We turn '0.68' into 68.0
                entry["poly_prob"] = round(float(prices[0]) * 100, 2)
                entry["market_name"] = market.get('question', 'Gold Daily Trend')
                found_market = True
                print(f"✅ Polymarket Success: '{entry['market_name']}' is at {entry['poly_prob']}%")
                break
        
        if not found_market:
            print("⚠️ No matching XAUUSD market found. Using 50% neutral default.")
            entry["poly_prob"] = 50.0
            entry["market_name"] = "No Active Market"

    except Exception as e:
        print(f"❌ Polymarket Error: {e}")

    return entry

# --- Execution & Saving ---
data_point = get_market_data()

if data_point:
    file_name = "gold_data.csv"
    df_new = pd.DataFrame([data_point])
    
    # If the file already exists, we append to it
    if os.path.exists(file_name):
        df_old = pd.read_csv(file_name)
        # Combine old and new, and remove any duplicate dates
        df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
    else:
        # If it's the first time, just use the new data
        df_combined = df_new
        
    # Save back to CSV
    df_combined.to_csv(file_name, index=False)
    print(f"🎉 SUCCESS: {file_name} has been updated and saved.")
else:
    print("❌ FATAL: Robot could not generate data.")
    sys.exit(1)
