import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re

def safe_get_json(url):
    # Added User-Agent to prevent 403 Forbidden errors from Polymarket/Yahoo
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 404: return None
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️ API Warning: {e}")
        return None

def get_live_market_data():
    print("--- 🛰️ 2026 Pulse: Hourly Scraper Active ---")
    SLUGS = {
        "gold": "gc-settle-jun-2026", 
        "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825"
    }
    
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    entry = {"date": now_ts, "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0, "treasury_10y": 0.0, "vix_index": 0.0}
    
    # 1. Macro Pulse (Yahoo Finance)
    tickers = {"gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", "treasury_10y": "^TNX", "vix_index": "^VIX"}
    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="1d")
            if not h.empty: 
                entry[key] = round(h['Close'].iloc[-1], 2)
        except: 
            pass

    # 2. Prediction Pulse (Polymarket)
    for p, slug in SLUGS.items():
        data = safe_get_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
        if not data or not data[0].get('markets'): continue
        
        for m in data[0]['markets']:
            raw_title = (m.get('groupItemTitle') or m.get('question')).lower()
            # Clean column names for CSV compatibility
            clean = re.sub(r'[^a-z0-9]', '_', raw_title).strip('_')
            clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
            
            prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            if prices: 
                entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
    
    return entry

# --- EXECUTION ENGINE ---
file_name = "gold_investment_pro.csv"
live_row = get_live_market_data()
df_new = pd.DataFrame([live_row])

if os.path.exists(file_name):
    # load existing data
    df_old = pd.read_csv(file_name, low_memory=False)
    # Combine and deduplicate
    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
else:
    df_final = df_new

# --- RESILIENT DATA CLEANING ---
df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
df_final = df_final.dropna(subset=['date'])
df_final = df_final.sort_values('date')

# Remove duplicate entries for the same minute
df_final = df_final.groupby('date').first().reset_index()

# Re-format date back to string for CSV
df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%d %H:%M')

# --- MOMENTUM SIGNALS ---
prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    # Velocity: Difference from last hour
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    # MA6: 6-hour Moving Average of that velocity
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
    # Signal: 1 if current velocity is picking up faster than the average
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).fillna(0).astype(int)

# --- FINAL SAVE ---
df_final.to_csv(file_name, index=False)
print(f"🏁 Update Complete. Total database size: {len(df_final)} rows.")
