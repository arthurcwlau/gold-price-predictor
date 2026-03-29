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
        # Weekend-safe logic
        gold_h = yf.Ticker("GC=F").history(period="7d")
        dxy_h = yf.Ticker("DX-Y.NYB").history(period="7d")
        oil_h = yf.Ticker("CL=F").history(period="7d")
        
        entry["gold"] = round(gold_h['Close'].iloc[-1], 2)
        entry["dxy"] = round(dxy_h['Close'].iloc[-1], 2)
        entry["oil_wti"] = round(oil_h['Close'].iloc[-1], 2)
        print(f"✅ Macro: Gold ${entry['gold']} | Oil ${entry['oil_wti']}")
    except Exception as e: 
        print(f"!! Macro Data Error: {e}")

    # 2. Polymarket: Gold Conviction & Fed Sentiment
    try:
        m_resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={SLUG}").json()
        if m_resp and len(m_resp) > 0:
            m = m_resp[0]['markets'][0]
            
            # Outcome Prices can be a list or a string-list
            prices = m.get('outcomePrices')
            if isinstance(prices, str):
                prices = json.loads(prices)
            
            entry["poly_prob"] = round(float(prices[0]) * 100, 2)
            
            # Get Order Book Depth (Conviction)
            token_id = m['clobTokenIds'][0]
            book_resp = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}")
            if book_resp.status_code == 200:
                book = book_resp.json()
                if book.get('bids') and book.get('asks'):
                    entry["spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)

        # Fed Rate Cut Sentiment
        fed_resp = requests.get("https://gamma-api.polymarket.com/events?active=true&q=Fed%20Rate%20Cut").json()
        if fed_resp and len(fed_resp) > 0:
            f_prices = fed_resp[0]['markets'][0].get('outcomePrices')
            if isinstance(f_prices, str): f_prices = json.loads(f_prices)
            entry["fed_cut_prob"] = round(float(f_prices[0]) * 100, 2)
            print(f"✅ Sentiment: Gold {entry['poly_prob']}% | Fed Cut {entry['fed_cut_prob']}%")

    except Exception as e:
        print(f"!! Polymarket Error: {e}")

    return entry

# --- Save Logic (Matching the YAML) ---
new_row = get_pro_metrics()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"--- 🏁 Success: Data saved to {file_name} ---")
