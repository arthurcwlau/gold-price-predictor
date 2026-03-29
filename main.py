import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re
import time

def get_institutional_data():
    print("--- 🧠 2026 Gold Intelligence: Institutional Pipeline Active ---")
    
    # STRATEGIC EVENT SLUGS
    GOLD_SETTLE_SLUG = "gc-settle-jun-2026"
    OIL_HIT_SLUG = "cl-hit-jun-2026"
    FED_JUNE_SLUG = "fed-decision-in-june-825"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0
    }

    # 1. Macro Data
    try:
        gold = yf.Ticker("GC=F").history(period="7d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1]
        oil = yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1]
        entry.update({"gold_price": round(gold, 2), "dxy_index": round(dxy, 2), "oil_wti": round(oil, 2)})
    except Exception as e:
        print(f"!! Macro Data Error: {e}")

    # 2. Polymarket Data Fetchers
    def fetch_event_markets(slug):
        try:
            r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            return r[0]['markets'] if r else []
        except: return []

    def get_clob_metrics(token_id):
        # CLOB API: Order Book, Spread, and Depth
        try:
            if not token_id: return 0.0, 0.0
            # Small sleep to prevent rate limiting (1500 req / 10s is the limit)
            time.sleep(0.1) 
            r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}").json()
            bids = r.get('bids', [])
            asks = r.get('asks', [])
            
            spread = 0.0
            if bids and asks:
                spread = round(float(asks[0]['price']) - float(bids[0]['price']), 4)
            
            depth = sum([float(b['size']) for b in bids[:10]]) + sum([float(a['size']) for a in asks[:10]])
            return spread, round(depth, 2)
        except: return 0.0, 0.0

    def get_reliable_oi(market_id):
        # Data API: The only 100% reliable source for Open Interest
        try:
            r = requests.get(f"https://data-api.polymarket.com/oi?market_id={market_id}").json()
            return round(float(r.get('openInterest', 0)), 2)
        except: return 0.0

    def process_institutional_curve(slug, prefix, entry_dict):
        markets = fetch_event_markets(slug)
        for m in markets:
            title = m.get('groupItemTitle') or m.get('question') or ""
            clean_name = title.replace('$', '').replace('+', '').replace(',', '').replace('<', 'under_').replace('>', 'over_').strip().lower()
            clean_name = re.sub(r'[^a-z0-9]', '_', clean_name).replace('__', '_')
            
            # Probability & Volume (Gamma)
            prices = m.get('outcomePrices')
            if isinstance(prices, str): prices = json.loads(prices)
            entry_dict[f"{prefix}_{clean_name}_prob"] = round(float(prices[0]) * 100, 2)
            entry_dict[f"{prefix}_{clean_name}_vol"] = round(float(m.get('volume', 0)), 2)
            
            # Open Interest (Data API)
            entry_dict[f"{prefix}_{clean_name}_oi"] = get_reliable_oi(m['id'])
            
            # Spread & Depth (CLOB API)
            token_id = m.get('clobTokenIds', [None])[0]
            if token_id:
                spread, depth = get_clob_metrics(token_id)
                entry_dict[f"{prefix}_{clean_name}_spread"] = spread
                entry_dict[f"{prefix}_{clean_name}_depth"] = depth

    # Execute Triple-API Scrape
    process_institutional_curve(GOLD_SETTLE_SLUG, "gold", entry)
    process_institutional_curve(OIL_HIT_SLUG, "oil", entry)
    process_institutional_curve(FED_JUNE_SLUG, "fed", entry)

    return entry

# --- Save Logic ---
new_row = get_institutional_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_combined = pd.concat([df_old, df_new], sort=False)
    df_combined = df_combined[df_new.columns]
    df_combined = df_combined.drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Tidy Institutional Data Saved.")
