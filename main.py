import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re

def safe_get_json(url):
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 404: return None
        response.raise_for_status()
        return response.json()
    except: return None

def get_market_data():
    print("--- 🛰️ 2026 Master Intel: Non-Destructive Build ---")
    SLUGS = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825"}
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    entry = {"date": now_ts, "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0, "treasury_10y": 0.0, "vix_index": 0.0}
    
    # 1. Live Macro (Yahoo Finance)
    tickers = {"gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", "treasury_10y": "^TNX", "vix_index": "^VIX"}
    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="1d")
            if not h.empty: entry[key] = round(h['Close'].iloc[-1], 2)
        except: pass

    # 2. Live Polymarket (Depth & Probability)
    for p, slug in SLUGS.items():
        data = safe_get_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
        if not data or not data[0].get('markets'): continue
        for m in data[0]['markets']:
            clean = re.sub(r'[^a-z0-9]', '_', (m.get('groupItemTitle') or m.get('question')).lower()).strip('_')
            clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
            
            prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            if prices: entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
            
            tokens = m.get('clobTokenIds')
            if tokens:
                tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                book = safe_get_json(f"https://clob.polymarket.com/book?token_id={tid}")
                if book and book.get('bids'):
                    entry[f"{p}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
    return entry

# --- PERSISTENCE ENGINE ---
file_name = "gold_investment_pro.csv"
live_row = get_market_data()
df_live = pd.DataFrame([live_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name, low_memory=False)
    # Standardize old dates to avoid mismatch
    df_old['date'] = pd.to_datetime(df_old['date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
    # Use sort=False to preserve all historical columns
    df_final = pd.concat([df_old, df_live], ignore_index=True, sort=False)
else:
    df_final = df_live

# Clean, Sort, and Re-calculate Velocity
df_final = df_final.dropna(subset=['date']).groupby('date').first().reset_index().sort_values('date')

prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

df_final.to_csv(file_name, index=False)
print(f"🏁 Data Saved. Current Rows: {len(df_final)}")
