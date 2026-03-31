import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re

def safe_get_json(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 404: return None
        response.raise_for_status()
        return response.json()
    except: return None

def get_live_market_data():
    print("--- 🛰️ 2026 Pulse: Institutional Intelligence Mode ---")
    # Expanded Slugs: Added Recession and Geopolitical Risk
    SLUGS = {
        "gold": "gc-settle-jun-2026", 
        "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825",
        "macro": "us-recession-2026"
    }
    
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    entry = {"date": now_ts}
    
    # 1. Institutional Macro Tickers
    tickers = {
        "gold_price": "GC=F", 
        "oil_wti": "CL=F", 
        "dxy_index": "DX-Y.NYB", 
        "vix_index": "^VIX",
        "gold_vix": "^GVZ",          # Gold-specific fear
        "real_yield_proxy": "TIP",   # Real Interest Rates
        "silver_price": "SI=F"       # Silver (Gold's lead indicator)
    }
    
    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="1d")
            if not h.empty: 
                entry[key] = round(h['Close'].iloc[-1], 2)
                if key == "gold_price": # Capture ETF volume for Gold
                    gld_h = yf.Ticker("GLD").history(period="1d")
                    entry["gld_etf_vol"] = int(gld_h['Volume'].iloc[-1])
        except: pass

    # 2. Advanced Prediction Pulse (Adding Volume & Open Interest)
    for p, slug in SLUGS.items():
        data = safe_get_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
        if not data or not data[0].get('markets'): continue
        for m in data[0]['markets']:
            raw_title = (m.get('groupItemTitle') or m.get('question')).lower()
            clean = re.sub(r'[^a-z0-9]', '_', raw_title).strip('_')
            clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
            
            # Probability
            prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            if prices: entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
            
            # Conviction Metrics
            entry[f"{p}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
            entry[f"{p}_{clean}_oi"] = round(float(m.get('openInterest', 0)), 2)
            
            # Whale Depth
            tokens = m.get('clobTokenIds')
            if tokens:
                tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                book = safe_get_json(f"https://clob.polymarket.com/book?token_id={tid}")
                if book and book.get('bids'):
                    entry[f"{p}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
    return entry

# --- EXECUTION ENGINE ---
file_name = "gold_investment_pro.csv"
live_row = get_live_market_data()
df_new = pd.DataFrame([live_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name, low_memory=False)
    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
else:
    df_final = df_new

# --- CLEAN & SYNC ---
df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
df_final = df_final.dropna(subset=['date']).groupby('date').first().reset_index().sort_values('date')
df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%d %H:%M')

# --- MOMENTUM SIGNALS (Applied to all Probabilities) ---
prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

df_final.to_csv(file_name, index=False)
print(f"🏁 Institutional Update Successful. Columns tracked: {len(df_final.columns)}")
