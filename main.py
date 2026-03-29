import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_pro_investment_data():
    print("--- 🏦 2026 Gold Investment Dashboard: Online ---")
    
    # Direct Target Slugs from your links
    GOLD_DAILY_SLUG = "xauusd-up-or-down-on-march-30-2026"
    FED_SLUG = "fed-rate-cut-by-629"
    JUNE_BEAR_SLUG = "gc-settle-below-3800-jun-2026"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0,
        "dxy_index": 0.0,
        "oil_wti": 0.0,
        "daily_up_prob": 0.0,
        "fed_april_cut_prob": 0.0,
        "june_crash_prob": 0.0, # Target < $3,800
        "market_spread": 0.0
    }

    # 1. Macro Data: Gold, Dollar, and Oil
    try:
        gold_h = yf.Ticker("GC=F").history(period="7d")
        dxy_h = yf.Ticker("DX-Y.NYB").history(period="7d")
        oil_h = yf.Ticker("CL=F").history(period="7d")
        
        entry["gold_price"] = round(gold_h['Close'].iloc[-1], 2)
        entry["dxy_index"] = round(dxy_h['Close'].iloc[-1], 2)
        entry["oil_wti"] = round(oil_h['Close'].iloc[-1], 2)
        print(f"📈 Macro: Gold ${entry['gold_price']} | Oil ${entry['oil_wti']}")
    except Exception as e:
        print(f"!! Macro Error: {e}")

    # 2. Polymarket Data Fetching
    def fetch_poly_prob(slug):
        try:
            resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            if resp:
                # Find the 'Yes' or 'Up' price
                # If multiple markets (like Fed), we look for April
                market = resp[0]['markets'][0]
                if "fed" in slug: # Handle specific Fed meeting selection
                    for m in resp[0]['markets']:
                        if "APRIL" in m.get('groupItemTitle', '').upper():
                            market = m
                            break
                
                prices = market.get('outcomePrices')
                if isinstance(prices, str): prices = json.loads(prices)
                return round(float(prices[0]) * 100, 2), market.get('clobTokenIds', [None])[0]
        except: return 0.0, None
        return 0.0, None

    # Execute Poly Fetches
    entry["daily_up_prob"], daily_token = fetch_poly_prob(GOLD_DAILY_SLUG)
    entry["fed_april_cut_prob"], _ = fetch_poly_prob(FED_SLUG)
    entry["june_crash_prob"], _ = fetch_poly_prob(JUNE_BEAR_SLUG)

    # 3. Conviction Check (Spread on the Daily Market)
    if daily_token:
        try:
            book = requests.get(f"https://clob.polymarket.com/book?token_id={daily_token}").json()
            if book.get('bids') and book.get('asks'):
                entry["market_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
        except: entry["market_spread"] = -1.0

    print(f"🎯 Sentiment: Daily Up {entry['daily_up_prob']}% | June Tail Risk {entry['june_crash_prob']}%")
    return entry

# --- Save & Append Logic ---
new_row = get_pro_investment_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"✅ Success: Deep-Sentiment Data Saved for {new_row['date']}")
