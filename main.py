import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re
import time

def get_master_institutional_data():
    print("--- 🛰️ 2026 Master Intelligence: Hourly Velocity MA Enabled ---")
    
    SLUGS = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825"}
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"), # Added timestamp for hourly precision
        "gold_price": 0.0, 
        "dxy_index": 0.0, 
        "oil_wti": 0.0,
        "treasury_10y": 0.0,
        "vix_index": 0.0
    }

    # 1. Macro Pulse (Yahoo Finance)
    tickers = {"gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", "treasury_10y": "^TNX", "vix_index": "^VIX"}
    
    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="5d")
            if not h.empty: entry[key] = round(h['Close'].iloc[-1], 2)
        except: pass

    # 2. Institutional Helpers
    def get_hourly_velocity(token_id):
        try:
            # Fetches the last 1 hour of price action for high-frequency detection
            r = requests.get(f"https://clob.polymarket.com/prices-history?token_id={token_id}&interval=1h").json()
            h = r.get('history', [])
            if len(h) > 1: return round((float(h[-1]['p']) - float(h[0]['p'])) * 100, 2)
        except: pass
        return 0.0

    def get_whale_pct(market_id):
        try:
            r = requests.get(f"https://data-api.polymarket.com/holders?market_id={market_id}").json()
            h = r.get('holders', [])
            if h: return round(sum([float(x['weight']) for x in h[:3]]) * 100, 2)
        except: pass
        return 0.0

    def get_clob_data(token_id):
        try:
            r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}").json()
            b, a = r.get('bids', []), r.get('asks', [])
            if b and a:
                spread = round(float(a[0]['price']) - float(b[0]['price']), 4)
                depth = round(sum([float(x['size']) for x in b[:5] + a[:5]]), 2)
                return spread, depth
        except: pass
        return 0.0, 0.0

    def process_curve(slug, prefix):
        try:
            resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            if not resp: return
            for m in resp[0]['markets']:
                raw = m.get('groupItemTitle') or m.get('question') or ""
                clean = re.sub(r'[^a-z0-9]', '_', raw.replace('$', '').replace('<', 'under_').replace('>', 'over_').lower()).strip('_')
                clean = re.sub(r'_+', '_', clean)
                
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                entry[f"{prefix}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
                entry[f"{prefix}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
                entry[f"{prefix}_{clean}_oi"] = round(float(m.get('openInterest', 0)), 2)
                entry[f"{prefix}_{clean}_whale_pct"] = get_whale_pct(m['id'])
                
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    entry[f"{prefix}_{clean}_spread"], entry[f"{prefix}_{clean}_depth"] = get_clob_data(tid)
                    entry[f"{prefix}_{clean}_velocity"] = get_hourly_velocity(tid)
        except: pass

    for p, s in SLUGS.items(): process_curve(s, p)
    return entry

# --- Save & Alpha Calculation Routine ---
new_data = get_master_institutional_data()
df_new = pd.DataFrame([new_data])
file_name = "gold_investment_pro.csv"

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_old = df_old[df_old.columns.intersection(df_new.columns)]
    df_combined = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=['date'], keep='last')
    
    # --- MOMENTUM CROSSOVER LOGIC ---
    # We calculate the 6-hour Moving Average for all Velocity columns
    velocity_cols = [c for c in df_combined.columns if c.endswith('_velocity')]
    for col in velocity_cols:
        ma_col = col + "_ma6"
        # Rolling average of the last 6 hours
        df_combined[ma_col] = df_combined[col].rolling(window=6).mean().round(2)
        
        # Signal: 1 if Current Velocity > 6-hour Average (Acceleration)
        signal_col = col + "_signal"
        df_combined[signal_col] = (df_combined[col] > df_combined[ma_col]).astype(int)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print("🏁 Hourly Alpha Data with Momentum Crossovers Saved.")
