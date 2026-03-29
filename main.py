import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_pro_metrics():
    print("--- 🧠 2026 AI-Predictor: Strategic Fetch ---")
    
    # Target Slug from your link
    SLUG = "xauusd-up-or-down-on-march-30-2026"
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold": 0.0, "dxy": 0.0, "oil_wti": 0.0, 
        "poly_prob": 0.0, "spread": 0.0, "fed_cut_prob": 0.0
    }

    # 1. Macro Data (Gold, DXY, and WTI Oil)
    try:
        # We look back 7 days to handle the Sunday market close
        gold = yf.Ticker("GC=F").history(period="7d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1]
        oil = yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1]
        
        entry["gold"], entry["dxy"], entry["oil_wti"] = round(gold, 2), round(dxy, 2), round(oil, 2)
        print(f"✅ Macro: Gold ${entry['gold']} | Oil ${entry['oil_wti']} | DXY {entry['dxy']}")
    except: print("!! Macro Data Fetch Failed")

    # 2. Polymarket: Gold Conviction & Fed Sentiment
    try:
        # A. Gold Sentiment & Spread
        m_resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={SLUG}").json()
        if m_resp:
            m = m_resp[0]['markets'][0]
            entry["poly_prob"] = round(float(json.loads(m['outcomePrices'])[0]) * 100, 2)
            
            # Get Order Book Depth (Conviction)
            token_id = m['clobTokenIds'][0]
            book = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}").json()
            if book.get('bids') and book.get('asks'):
                entry["spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)

        # B. Fed Rate Cut Probability (Critical for Gold)
        fed_resp = requests.get("https://gamma-api.polymarket.com/events?active=true&q=Fed%20Rate%20Cut").json()
        if fed_resp:
            entry["fed_cut_prob"] = round(float(json.loads(fed_resp[0]['markets'][0]['outcomePrices'])[0]) * 100, 2)
            print(f"✅ Sentiment: Gold Prob {entry['poly_prob']}% | Fed Cut {entry['fed_cut_prob']}%")

    except Exception as e:
        print(f"!! Polymarket Error: {e}")

    return entry

# --- Save Logic ---
new_row = get_pro_metrics()
file = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file):
    df_old = pd.read_csv(file)
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file, index=False)
print(f"--- 🏁 Data saved for March 29, 2026 ---")
