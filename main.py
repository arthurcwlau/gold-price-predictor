import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re
import time

def get_institutional_data():
    print("--- 🏥 2026 Diagnostic Mode: Polymarket Triple-Check ---")
    
    SLUGS = {
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

    # 2. Polymarket Data Fetchers
    def get_clob_metrics(token_id):
        try:
            if not token_id: return 0.0, 0.0
            r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}").json()
            bids, asks = r.get('bids', []), r.get('asks', [])
            if not bids or not asks:
                return 0.0, 0.0 # Market is likely closed for Sunday
            spread = round(float(asks[0]['price']) - float(bids[0]['price']), 4)
            depth = sum([float(b['size']) for b in bids[:5]]) + sum([float(a['size']) for a in asks[:5]])
            return spread, round(depth, 2)
        except: return 0.0, 0.0

    def get_reliable_oi(condition_id):
        try:
            # 2026 Standard: Data API prefers condition_id over market_id
            url = f"https://data-api.polymarket.com/oi?condition_id={condition_id}"
            r = requests.get(url).json()
            return round(float(r.get('openInterest', 0)), 2)
        except: return 0.0

    def process_curve(slug, prefix):
        print(f"🔍 Scanning Event: {slug}...")
        try:
            resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            if not resp: return
            markets = resp[0]['markets']
            
            for m in markets:
                title = m.get('groupItemTitle') or m.get('question') or ""
                clean_name = re.sub(r'[^a-z0-9]', '_', title.replace('$', '').replace('<', 'under_').lower()).strip('_')
                
                # Probability & Vol
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                entry[f"{prefix}_{clean_name}_prob"] = round(float(prices[0]) * 100, 2)
                entry[f"{prefix}_{clean_name}_vol"] = round(float(m.get('volume', 0)), 2)
                
                # OI - Using conditionId
                entry[f"{prefix}_{clean_name}_oi"] = get_reliable_oi(m.get('conditionId'))
                
                # Spread & Depth
                tokens = m.get('clobTokenIds')
                if isinstance(tokens, str): tokens = json.loads(tokens)
                if tokens:
                    s, d = get_clob_metrics(tokens[0])
                    entry[f"{prefix}_{clean_name}_spread"] = s
                    entry[f"{prefix}_{clean_name}_depth"] = d
        except Exception as e:
            print(f"!! Error on {slug}: {e}")

    process_curve(SLUGS["gold"], "gold")
    process_curve(SLUGS["oil"], "oil")
    process_curve(SLUGS["fed"], "fed")

    return entry

# --- Save Logic ---
new_row = get_institutional_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_combined = pd.concat([df_old, df_new], sort=False).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Diagnostic Run Complete. Check logs for scan results.")
