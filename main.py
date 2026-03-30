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
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️ API Alert: {url} failed. Error: {e}")
        return None

def get_market_data():
    print("--- 🛰️ 2026 Robust Intelligence: Protection Active ---")
    SLUGS = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825"}
    now_ts = datetime.now().strftime("%Y-%m-%d %H:00")
    
    entry = {"date": now_ts, "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0, "treasury_10y": 0.0, "vix_index": 0.0}
    
    # 1. Macro Data
    tickers = {"gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", "treasury_10y": "^TNX", "vix_index": "^VIX"}
    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="5d")
            if not h.empty: entry[key] = round(h['Close'].iloc[-1], 2)
        except: pass

    # 2. Polymarket Data
    market_list = []
    for p, slug in SLUGS.items():
        data = safe_get_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
        if not data or not data[0].get('markets'): continue
        
        for m in data[0]['markets']:
            clean = re.sub(r'[^a-z0-9]', '_', m.get('groupItemTitle', '').lower() or m.get('question', '').lower()).strip('_')
            clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
            
            # Probability
            prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            if prices: entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
            
            # Depth/Spread
            tokens = m.get('clobTokenIds')
            if tokens:
                tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                book = safe_get_json(f"https://clob.polymarket.com/book?token_id={tid}")
                if book and book.get('bids') and book.get('asks'):
                    entry[f"{p}_{clean}_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
                    entry[f"{p}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5] + book['asks'][:5]]), 2)
                market_list.append({"prefix": p, "clean": clean, "token_id": tid})
    
    return entry, market_list

def backfill_history(market_list):
    print("⏳ Running Emergency Backfill...")
    history_rows = []
    for m in market_list:
        h_data = safe_get_json(f"https://clob.polymarket.com/prices-history?market={m['token_id']}&interval=1h")
        if h_data and h_data.get('history'):
            for p in h_data['history'][-24:]: # Grab last 24h
                dt = datetime.fromtimestamp(p['t']).strftime("%Y-%m-%d %H:00")
                history_rows.append({"date": dt, f"{m['prefix']}_{m['clean']}_prob": round(float(p['p']) * 100, 2)})
    return pd.DataFrame(history_rows)

# --- EXECUTION ---
file_name = "gold_investment_pro.csv"
live_row, markets = get_market_data()
df_live = pd.DataFrame([live_row])

if os.path.exists(file_name) and os.path.getsize(file_name) > 1000:
    df_old = pd.read_csv(file_name)
    df_final = pd.concat([df_old, df_live], ignore_index=True, sort=False)
else:
    df_hist = backfill_history(markets)
    df_final = pd.concat([df_hist, df_live], ignore_index=True, sort=False)

# Data Cleaning
df_final['date'] = pd.to_datetime(df_final['date'])
df_final = df_final.groupby('date').first().reset_index().sort_values('date')

# Indicators
for col in [c for c in df_final.columns if c.endswith('_prob')]:
    base = col.replace('_prob', '')
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(6).mean().round(2)
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

df_final.to_csv(file_name, index=False)
print(f"🏁 Update Complete. Rows: {len(df_final)}")
