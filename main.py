import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re
import time

def get_pro_data():
    print("--- 🏦 2026 Gold Intelligence: Surgical Reset ---")
    
    # TARGET EVENTS
    TARGETS = {
        "gold": "gc-settle-jun-2026",
        "oil": "cl-hit-jun-2026",
        "fed": "fed-decision-in-june-825"
    }
    
    entry = {"date": datetime.now().strftime("%Y-%m-%d"), "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0}

    # 1. Macro Data
    try:
        gold_h = yf.Ticker("GC=F").history(period="7d")
        entry["gold_price"] = round(gold_h['Close'].iloc[-1], 2)
        entry["oil_wti"] = round(yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1], 2)
        entry["dxy_index"] = round(yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1], 2)
    except: print("!! Macro Data Offline")

    # 2. Polymarket API Helpers
    def get_clob(token_id):
        try:
            if not token_id: return 0.0, 0.0
            r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}").json()
            bids, asks = r.get('bids', []), r.get('asks', [])
            if not bids or not asks: return 0.0, 0.0
            spread = round(float(asks[0]['price']) - float(bids[0]['price']), 4)
            depth = sum([float(b['size']) for b in bids[:5]]) + sum([float(a['size']) for a in asks[:5]])
            return spread, round(depth, 2)
        except: return 0.0, 0.0

    def get_oi(market_id):
        try:
            r = requests.get(f"https://data-api.polymarket.com/oi?market_id={market_id}").json()
            return round(float(r.get('openInterest', 0)), 2)
        except: return 0.0

    # 3. Processing the Curves
    for prefix, slug in TARGETS.items():
        try:
            r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            if not r: continue
            for m in r[0]['markets']:
                # Clean header names: e.g. "Gold <$3,800" -> "gold_under_3800"
                title = m.get('groupItemTitle') or m.get('question') or ""
                clean = re.sub(r'[^a-z0-9]', '_', title.replace('$', '').replace('<', 'under_').replace('>', 'over_').lower()).strip('_')
                clean = re.sub(r'_+', '_', clean) # Remove double underscores
                
                # Prob & Vol
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                entry[f"{prefix}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
                entry[f"{prefix}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
                
                # OI, Spread, Depth
                entry[f"{prefix}_{clean}_oi"] = get_oi(m['id'])
                tokens = m.get('clobTokenIds')
                if tokens:
                    s, d = get_clob(tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0])
                    entry[f"{prefix}_{clean}_spread"], entry[f"{prefix}_{clean}_depth"] = s, d
        except: print(f"!! Failed {slug}")

    return entry

# --- Save Logic (Strict Column Alignment) ---
data = get_pro_data()
df_new = pd.DataFrame([data])
file_name = "gold_investment_pro.csv"

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # CRITICAL: This line forces the old file to drop any columns not in our new clean script
    df_old = df_old[df_old.columns.intersection(df_new.columns)]
    df_combined = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=['date'], keep='last')
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print("🏁 Clean Data Saved.")
