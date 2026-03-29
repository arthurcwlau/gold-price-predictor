import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os

def get_market_data():
    try:
        # 1. Fetch Finance Data
        # Using the latest 2026 ticker for Gold and DXY
        gold = yf.Ticker("GC=F").history(period="1d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="1d")['Close'].iloc[-1]
        
        # 2. Fetch Polymarket
        # March 2026 specific search for Gold (GC) markets
        url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
        resp = requests.get(url).json()
        
        prob = 50.0 # Default if market not found
        q_text = "No active gold market"
        
        if resp:
            for event in resp:
                if "GOLD" in event['title'].upper():
                    market = event['markets'][0]
                    prob = float(market['outcomePrices'][0]) * 100
                    q_text = market['question']
                    break
                    
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "gold_price": round(gold, 2),
            "dxy": round(dxy, 2),
            "poly_prob": round(prob, 2),
            "market": q_text
        }
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# --- Main Logic ---
new_data = get_market_data()
if new_data:
    df_new = pd.DataFrame([new_data])
    file = "gold_data.csv"
    
    if os.path.exists(file):
        df_old = pd.read_csv(file)
        df_combined = pd.concat([df_old, df_new]).tail(30)
    else:
        df_combined = df_new
        
    df_combined.to_csv(file, index=False)
    print(f"Success! Recorded {new_data['date']}")
