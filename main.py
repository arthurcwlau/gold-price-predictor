import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_pro_investment_data():
    print("--- 🏦 2026 Gold Investment Pipeline: Active ---")
    
    # Direct Target Slugs from your links
    GOLD_SLUG = "xauusd-up-or-down-on-march-30-2026"
    FED_SLUG = "fed-rate-cut-by-629" # Event ID from your snippet
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0,
        "dxy_index": 0.0,
        "oil_wti": 0.0,
        "gold_up_prob": 0.0,
        "gold_spread": 0.0,
        "fed_april_cut_prob": 0.0
    }

    # 1. Macro Data: Gold, Dollar, and Oil (Weekend-Safe)
    try:
        gold_h = yf.Ticker("GC=F").history(period="7d")
        dxy_h = yf.Ticker("DX-Y.NYB").history(period="7d")
        oil_h = yf.Ticker("CL=F").history(period="7d")
        
        entry["gold_price"] = round(gold_h['Close'].iloc[-1], 2)
        entry["dxy_index"] = round(dxy_h['Close'].iloc[-1], 2)
        entry["oil_wti"] = round(oil_h['Close'].iloc[-1], 2)
        print(f"📈 Macro: Gold ${entry['gold_price']} | Oil ${entry['oil_wti']} | DXY {entry['dxy_index']}")
    except Exception as e:
        print(f"!! Macro Error: {e}")

    # 2. Polymarket: Gold Sentiment & Conviction
    try:
        g_resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={GOLD_SLUG}").json()
        if g_resp:
            m = g_resp[0]['markets'][0]
            g_prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            entry["gold_up_prob"] = round(float(g_prices[0]) * 100, 2)
            
            # Conviction Check (Order Book Spread)
            token_id = m['clobTokenIds'][0]
            book = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}").json()
            if book.get('bids') and book.get('asks'):
                entry["gold_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
            else:
                entry["gold_spread"] = -1.0 # Market is sleeping/closed
    except: print("!! Gold Sentiment Fetch Failed")

    # 3. Polymarket: Fed April Meeting Sentiment
    try:
        f_resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={FED_SLUG}").json()
        if f_resp:
            # We look through the list of meetings for 'April'
            for market in f_resp[0]['markets']:
                if "APRIL" in market.get('groupItemTitle', '').upper() or "APRIL" in market.get('question', '').upper():
                    f_prices = json.loads(market['outcomePrices']) if isinstance(market['outcomePrices'], str) else market['outcomePrices']
                    entry["fed_april_cut_prob"] = round(float(f_prices[0]) * 100, 2)
                    print(f"🏦 Fed Sentiment: April Cut Probability is {entry['fed_april_cut_prob']}%")
                    break
    except: print("!! Fed Sentiment Fetch Failed")

    return entry

# --- Save & Rolling Log ---
new_row = get_pro_investment_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # Merges, cleans duplicates, keeps last 30 days
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"✅ Success: 2026 Strategic Data Saved.")
