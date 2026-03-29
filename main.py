import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import sys

def get_market_data():
    print("--- Starting Gold Data Fetch ---")
    data_point = {"date": datetime.now().strftime("%Y-%m-%d")}

    # 1. Fetch Finance Data (Look back 5 days to handle weekends)
    try:
        # We fetch 5 days of history to be safe on weekends/holidays
        gold_hist = yf.Ticker("GC=F").history(period="5d")
        dxy_hist = yf.Ticker("DX-Y.NYB").history(period="5d")
        
        # Take the very last valid row (Friday's close if today is Sunday)
        data_point["gold_price"] = round(gold_hist['Close'].iloc[-1], 2)
        data_point["dxy_index"] = round(dxy_hist['Close'].iloc[-1], 2)
        print(f"Success: Gold ${data_point['gold_price']} | DXY {data_point['dxy_index']}")
    except Exception as e:
        print(f"Finance Error: {e}")
        return None

    # 2. Fetch Polymarket (The "Sentiment" Oracle)
    try:
        # Searching for 'Gold (GC)' specifically to avoid Bitcoin news
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
        resp = requests.get(url).json()
        
        found = False
        for event in resp:
            title = event.get('title', '').upper()
            # We want 'Gold (GC)' or 'XAUUSD' but NOT 'Bitcoin'
            if ("GOLD" in title or "XAU" in title) and "BITCOIN" not in title:
                market = event['markets'][0]
                # Safely get the 'Yes' price (probability)
                prob_raw = market.get('outcomePrices', [0.5, 0.5])[0]
                data_point["poly_up_prob"] = round(float(prob_raw) * 100, 2)
                data_point["market_name"] = market.get('question', 'Gold Trend')
                found = True
                print(f"Found Polymarket: {data_point['market_name']} at {data_point['poly_up_prob']}%")
                break
        
        if not found:
            data_point["poly_up_prob"] = 50.0
            data_point["market_name"] = "No active gold market found"
            
    except Exception as e:
        print(f"Polymarket Error: {e}")
        return None

    return data_point

# --- Save to CSV ---
new_row = get_market_data()
if new_row:
    file = "gold_data.csv"
    df_new = pd.DataFrame([new_row])
    
    if os.path.exists(file):
        df_old = pd.read_csv(file)
        # Keep only the last 30 days to keep the AI focused
        df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date']).tail(30)
    else:
        df_combined = df_new
        
    df_combined.to_csv(file, index=False)
    print("--- SUCCESS: Data saved to gold_data.csv ---")
else:
    print("--- FAILED: Critical data missing ---")
    sys.exit(1)
