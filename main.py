import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os

def get_market_data():
    print("Fetching data...")
    # 1. Handle Weekends: Look back 5 days to find the last 'Friday' price
    gold_hist = yf.Ticker("GC=F").history(period="5d")
    dxy_hist = yf.Ticker("DX-Y.NYB").history(period="5d")

    if gold_hist.empty or dxy_hist.empty:
        print("Could not find any price data.")
        return None

    # Grab the last known valid price
    last_gold = round(gold_hist['Close'].iloc[-1], 2)
    last_dxy = round(dxy_hist['Close'].iloc[-1], 2)

    # 2. Fetch Polymarket (The "Sentiment" Oracle)
    url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
    resp = requests.get(url).json()
    
    prob = 50.0 # Default if market not found
    q_text = "No active market"
    
    if resp:
        for event in resp:
            title = event.get('title', '').upper()
            if "GOLD" in title and "BITCOIN" not in title:
                market = event['markets'][0]
                # Prices are returned as a list like [0.65, 0.35]
                raw_prices = market.get('outcomePrices', ["0.5"])
                prob = round(float(raw_prices[0]) * 100, 2)
                q_text = market.get('question', 'Gold Trend')
                break

    return {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": last_gold,
        "dxy_index": last_dxy,
        "poly_prob": prob,
        "market": q_text
    }

# Save Logic
data = get_market_data()
if data:
    df_new = pd.DataFrame([data])
    file = "gold_data.csv"
    if os.path.exists(file):
        df_old = pd.read_csv(file)
        df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date']).tail(30)
    else:
        df_combined = df_new
    df_combined.to_csv(file, index=False)
    print(f"Recorded Gold at ${data['gold_price']}")
