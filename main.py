import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_market_data():
    try:
        # 1. Fetch Gold & Dollar Index (Ground Truth)
        gold = yf.Ticker("GC=F").history(period="1d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="1d")['Close'].iloc[-1]
        
        # 2. Fetch Polymarket (The "Oracle")
        # We use a very specific query to find Gold markets
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold%20(GC)"
        resp = requests.get(url).json()
        
        prob = 0.0
        q_text = "No active Gold market"
        
        if resp:
            for event in resp:
                # Extra safety: Make sure it's about Gold, not Bitcoin
                if "GOLD (GC)" in event['title'].upper():
                    market = event['markets'][0]
                    # FIX: Safely parse the probability string
                    prices = json.loads(market['outcomePrices'])
                    prob = float(prices[0]) * 100
                    q_text = market['question']
                    break
                    
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "gold_price": round(gold, 2),
            "dxy_index": round(dxy, 2),
            "poly_up_prob": round(prob, 2),
            "market_name": q_text
        }
    except Exception as e:
        print(f"Robot Error: {e}")
        return None

# --- Save to Spreadsheet ---
new_row = get_market_data()
if new_row:
    file = "gold_data.csv"
    df_new = pd.DataFrame([new_row])
    
    # If the file exists, add to it; if not, create it
    if os.path.exists(file):
        df_old = pd.read_csv(file)
        df_combined = pd.concat([df_old, df_new]).tail(30)
    else:
        df_combined = df_new
        
    df_combined.to_csv(file, index=False)
    print(f"Data saved! Gold: ${new_row['gold_price']}")
