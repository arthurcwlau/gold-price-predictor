import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import sys

def get_market_data():
    print("--- Starting Data Collection ---")
    data_point = {}

    # 1. Fetch Finance Data
    try:
        print("Fetching Yahoo Finance (Gold & DXY)...")
        gold = yf.Ticker("GC=F").history(period="1d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="1d")['Close'].iloc[-1]
        data_point["gold_price"] = round(gold, 2)
        data_point["dxy_index"] = round(dxy, 2)
        print(f"Found Gold: ${gold}, DXY: {dxy}")
    except Exception as e:
        print(f"!! Finance Error: {e}")
        return None

    # 2. Fetch Polymarket
    try:
        print("Searching Polymarket for Gold events...")
        # Using a broader search to ensure we find SOMETHING
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
        resp = requests.get(url).json()
        
        found_market = False
        if resp:
            for event in resp:
                title = event.get('title', '').upper()
                print(f"Checking event: {title}")
                
                # Filter for real gold, exclude Bitcoin/MicroStrategy
                if "GOLD" in title and "BITCOIN" not in title:
                    market = event['markets'][0]
                    # Polymarket prices can be a list or a string; we handle both
                    raw_prices = market.get('outcomePrices', [0.5, 0.5])
                    
                    # Convert to number safely
                    prob = float(raw_prices[0]) * 100
                    
                    data_point["date"] = datetime.now().strftime("%Y-%m-%d")
                    data_point["poly_up_prob"] = round(prob, 2)
                    data_point["market_name"] = market.get('question', 'Gold Market')
                    found_market = True
                    print(f"Matched Gold Market: {data_point['market_name']} at {prob}%")
                    break
        
        if not found_market:
            print("!! No matching Gold market found on Polymarket today.")
            return None

    except Exception as e:
        print(f"!! Polymarket Error: {e}")
        return None

    return data_point

# --- Save Logic ---
new_row = get_market_data()

if new_row:
    file = "gold_data.csv"
    df_new = pd.DataFrame([new_row])
    
    if os.path.exists(file):
        df_old = pd.read_csv(file)
        df_combined = pd.concat([df_old, df_new]).tail(30)
    else:
        df_combined = df_new
        
    df_combined.to_csv(file, index=False)
    print(f"--- SUCCESS: {file} updated ---")
else:
    print("--- FAILED: No data was saved ---")
    sys.exit(1) # This forces a Red X so you know it failed
