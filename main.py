import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_pro_metrics():
    print("--- 🧠 AI-Predictor: Pro Data Collection ---")
    
    # Targeting the specific slug from your link
    SLUG = "xauusd-up-or-down-on-march-30-2026"
    entry = {"date": datetime.now().strftime("%Y-%m-%d"), "gold": 0, "dxy": 0, "prob": 0, "spread": 0, "fed_cut": 0}

    # 1. Macro Data
    try:
        gold = yf.Ticker("GC=F").history(period="5d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="5d")['Close'].iloc[-1]
        entry["gold"], entry["dxy"] = round(gold, 2), round(dxy, 2)
    except: print("!! Macro Fetch Failed")

    # 2. Polymarket Gamma + CLOB (Conviction Logic)
    try:
        # Get Market Details
        market_resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={SLUG}").json()
        if market_resp:
            market = market_resp[0]['markets'][0]
            yes_token = market['clobTokenIds'][0] # The "UP" Token
            
            # Get Current Prob from Gamma
            prices = json.loads(market['outcomePrices'])
            entry["prob"] = round(float(prices[0]) * 100, 2)

            # Get CLOB Spread (The "Conviction" Metric)
            # This is a public endpoint discussed in the article
            book_url = f"https://clob.polymarket.com/book?token_id={yes_token}"
            book = requests.get(book_url).json()
            
            if book.get('bids') and book.get('asks'):
                best_bid = float(book['bids'][0]['price'])
                best_ask = float(book['asks'][0]['price'])
                entry["spread"] = round(best_ask - best_bid, 4)
                print(f"✅ Pro Success: Spread is {entry['spread']}")

        # 3. Fed Sentiment (Bonus Predictor for AI)
        fed_url = "https://gamma-api.polymarket.com/events?active=true&q=Fed%20Rate%20Cut"
        fed_resp = requests.get(fed_url).json()
        if fed_resp:
            entry["fed_cut"] = round(float(json.loads(fed_resp[0]['markets'][0]['outcomePrices'])[0]) * 100, 2)

    except Exception as e:
        print(f"!! Pro Fetch Error: {e}")

    return entry

# --- Save Routine ---
row = get_pro_metrics()
file = "gold_pro_data.csv"
df_new = pd.DataFrame([row])

if os.path.exists(file):
    df_old = pd.read_csv(file)
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print("--- 🏁 Robot Task Complete ---")
