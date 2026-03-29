import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os

def get_gold_data():
    # 1. Fetch Actual Gold Price & Dollar Index
    gold_price = yf.Ticker("GC=F").history(period="1d")['Close'].iloc[-1]
    dxy_index = yf.Ticker("DX-Y.NYB").history(period="1d")['Close'].iloc[-1]

    # 2. Fetch Polymarket Odds (Filtering for GC Gold)
    # We use a very specific query for Gold Commodities
    url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold%20Price"
    resp = requests.get(url).json()
    
    prob = 0
    q_text = "No Gold Market Found"
    
    for event in resp:
        # We only want markets about the "GC" (Gold) or "Gold Price"
        if "GOLD" in event['title'].upper() and "BITCOIN" not in event['title'].upper():
            market = event['markets'][0]
            prob = float(market['outcomePrices'][0]) * 100
            q_text = market['question']
            break

    return {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "actual_gold": round(gold_price, 2),
        "dxy": round(dxy_index, 2),
        "poly_prob": round(prob, 2),
        "market": q_text
    }

# Run and Save
row = get_gold_data()
df_new = pd.DataFrame([row])
file = "gold_data.csv"

if os.path.exists(file):
    df_old = pd.read_csv(file)
    df_combined = pd.concat([df_old, df_new]).tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print(f"Success! Recorded {row['market']} at {row['poly_prob']}%")
