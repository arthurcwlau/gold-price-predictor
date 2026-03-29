import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os

def get_data():
    print("Fetching data...")
    # 1. Handle Weekends: Look back 5 days for the last valid price
    gold = yf.Ticker("GC=F").history(period="5d")['Close'].iloc[-1]
    dxy = yf.Ticker("DX-Y.NYB").history(period="5d")['Close'].iloc[-1]

    # 2. Fetch Polymarket (Search for 'Gold XAU' to be specific)
    url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold%20XAU"
    resp = requests.get(url).json()
    
    prob = 50.0
    market_name = "No active gold market"
    
    if resp:
        for event in resp:
            # Explicitly ignore 'Bitcoin' or 'MicroStrategy'
            title = event.get('title', '').upper()
            if "GOLD" in title and "BITCOIN" not in title:
                market = event['markets'][0]
                # Price is usually the first number in the list
                prices = market.get('outcomePrices', ["0.5"])
                prob = float(prices[0]) * 100
                market_name = market.get('question', 'Gold Trend')
                break

    return {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": round(gold, 2),
        "dxy_index": round(dxy, 2),
        "poly_prob": round(prob, 2),
        "market": market_name
    }

# Run and Append
new_row = get_data()
df_new = pd.DataFrame([new_row])
file = "gold_data.csv"

if os.path.exists(file):
    df_old = pd.read_csv(file)
    # We use 'drop_duplicates' so you don't get 10 rows for the same Sunday
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date']).tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print(f"Success! Logged Gold at ${new_row['gold_price']}")
