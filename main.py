import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_gold_alpha_data():
    print("--- 🏦 2026 Gold Alpha Pipeline: Starting ---")
    
    # SLUGS - Targeted for high predictive value
    SLUGS = {
        "gold_daily": "xauusd-up-or-down-on-march-30-2026",
        "fed_risk": "fed-rate-cut-by-629",
        "gold_june": "gc-settle-jun-2026",
        "oil_shock": "cl-hit-jun-2026"
    }
    
    # Baseline Entry
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0,
        "market_conviction": 0.0 # Derived from spread
    }

    # 1. Macro Pulse (Leading Indicators)
    try:
        # Weekend-safe fetching
        gold = yf.Ticker("GC=F").history(period="7d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1]
        oil = yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1]
        entry.update({"gold_price": round(gold, 2), "dxy_index": round(dxy, 2), "oil_wti": round(oil, 2)})
    except: print("!! Macro Fetch Failed")

    # 2. Polymarket "Alpha" Fetcher
    def fetch_sentiment(slug):
        try:
            r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            return r[0]['markets'] if r else []
        except: return []

    # A. Gold Sentiment (Daily & Tail Risk)
    gold_daily = fetch_sentiment(SLUGS["gold_daily"])
    if gold_daily:
        prices = json.loads(gold_daily[0]['outcomePrices']) if isinstance(gold_daily[0]['outcomePrices'], str) else gold_daily[0]['outcomePrices']
        entry["poly_gold_daily_up"] = round(float(prices[0]) * 100, 2)
        # Conviction Check: Low spread = Smart Money is present
        try:
            tid = gold_daily[0]['clobTokenIds'][0]
            book = requests.get(f"https://clob.polymarket.com/book?token_id={tid}").json()
            if book.get('bids') and book.get('asks'):
                entry["market_conviction"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
        except: entry["market_conviction"] = -1.0

    # B. The June Crash Check (Tail Risk)
    gold_june = fetch_sentiment(SLUGS["gold_june"])
    for m in gold_june:
        if "3800" in m.get('groupItemTitle', ''):
            p = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            entry["gold_crash_june_prob"] = round(float(p[0]) * 100, 2)

    # C. The Fed Pivot (Monetary Lead)
    fed_markets = fetch_sentiment(SLUGS["fed_risk"])
    for m in fed_markets:
        if "APRIL" in m.get('groupItemTitle', '').upper():
            p = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            entry["fed_pivot_prob"] = round(float(p[0]) * 100, 2)

    # D. Oil Shock Curve (Geopolitical Lead)
    oil_markets = fetch_sentiment(SLUGS["oil_shock"])
    # We only keep the most predictive "Breakout" levels
    targets = ["$120", "$150", "$200"]
    for m in oil_markets:
        title = m.get('groupItemTitle', '')
        if any(t in title for t in targets):
            clean_name = f"oil_shock_{title.replace('$', '').strip()}_prob"
            p = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            entry[clean_name] = round(float(p[0]) * 100, 2)

    print(f"✅ Success: Captured {len(entry)} High-Alpha data points.")
    return entry

# --- Save & Rolling Strategy ---
new_row = get_gold_alpha_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # Ensure new columns don't break old data
    df_combined = pd.concat([df_old, df_new], sort=False).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Alpha Dashboard Updated: {file_name}")
