import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
import json
import re

def get_market_data():
    print("--- 🛰️ 2026 Ultimate Alpha: Backfill & Merge Active ---")
    
    SLUGS = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825"}
    # Force top-of-hour format for perfect merging
    now_ts = datetime.now().strftime("%Y-%m-%d %H:00")
    
    entry = {"date": now_ts, "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0, "treasury_10y": 0.0, "vix_index": 0.0}
    
    # 1. Live Macro
    tickers = {"gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", "treasury_10y": "^TNX", "vix_index": "^VIX"}
    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="1d")
            if not h.empty: entry[key] = round(h['Close'].iloc[-1], 2)
        except: pass

    # 2. Live Polymarket
    market_list = []
    for p, slug in SLUGS.items():
        try:
            resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            for m in resp[0]['markets']:
                clean = re.sub(r'[^a-z0-9]', '_', m.get('groupItemTitle', '').lower() or m.get('question', '').lower()).strip('_')
                clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
                
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
                
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    # Depth/Spread
                    r = requests.get(f"https://clob.polymarket.com/book?token_id={tid}").json()
                    b, a = r.get('bids', []), r.get('asks', [])
                    if b and a:
                        entry[f"{p}_{clean}_spread"] = round(float(a[0]['price']) - float(b[0]['price']), 4)
                        entry[f"{p}_{clean}_depth"] = round(sum([float(x['size']) for x in b[:5] + a[:5]]), 2)
                    market_list.append({"prefix": p, "clean": clean, "token_id": tid})
        except: pass

    return entry, market_list

def backfill_missing_hours(market_list):
    print("⏳ Auto-Backfilling 48h History...")
    history_data = []
    # Macro History (7d)
    macro_h = {}
    tickers = {"gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", "treasury_10y": "^TNX", "vix_index": "^VIX"}
    for key, ticker in tickers.items():
        macro_h[key] = yf.Ticker(ticker).history(period="7d", interval="1h")['Close']

    # Poly History (48h)
    for m in market_list:
        try:
            prices = requests.get(f"https://clob.polymarket.com/prices-history?market={m['token_id']}&interval=1h").json().get('history', [])
            for p in prices[-48:]:
                dt = datetime.fromtimestamp(p['t']).strftime("%Y-%m-%d %H:00")
                row = {"date": dt, f"{m['prefix']}_{m['clean']}_prob": round(float(p['p']) * 100, 2)}
                ts = pd.to_datetime(dt).tz_localize(None)
                for k in tickers.keys():
                    idx = macro_h[k].index.tz_localize(None)
                    if ts in idx: row[k] = round(macro_h[k].loc[ts], 2)
                history_data.append(row)
        except: pass
    return pd.DataFrame(history_data)

# --- EXECUTION ---
live_row, markets = get_market_data()
df_live = pd.DataFrame([live_row])
file_name = "gold_investment_pro.csv"

if os.path.exists(file_name) and os.path.getsize(file_name) > 1000:
    df_final = pd.read_csv(file_name)
    # RESILIENT CONCAT: Keep all columns, no intersections
    df_final = pd.concat([df_final, df_live], ignore_index=True, sort=False)
else:
    print("Starting fresh with Backfill...")
    df_final = pd.concat([backfill_missing_hours(markets), df_live], ignore_index=True, sort=False)

# Clean and Sort
df_final['date'] = pd.to_datetime(df_final['date'])
df_final = df_final.groupby('date').first().reset_index()
df_final = df_final.sort_values('date')

# Calculate Indicators (Works instantly on backfill)
prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(6).mean().round(2)
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

df_final.to_csv(file_name, index=False)
print(f"🏁 Done. Total Rows: {len(df_final)}")
