import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone # Use timezone.utc
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
    print("--- 🛰️ 2026 Pulse: Explicit UTC Mode ---")
    SLUGS = {
        "gold": "gc-settle-jun-2026", 
        "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825",
        "recession": "us-recession-by-end-of-2026"
    }
    
    # Generate timestamp with 'Z' for explicit UTC recognition
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")
    entry = {"date": now_ts}
    
    tickers = {
        "gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", 
        "vix_index": "^VIX", "gold_vix": "^GVZ", "real_yield_proxy": "TIP",
        "silver_price": "SI=F", "gold_miners": "GDX", "copper_price": "HG=F"
    }
    
    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="5d")
            if not h.empty: 
                entry[key] = round(h['Close'].iloc[-1], 2)
                if key == "gold_price":
                    gld_h = yf.Ticker("GLD").history(period="5d")
                    if not gld_h.empty: entry["gld_etf_vol"] = int(gld_h['Volume'].iloc[-1])
                if key == "dxy_index":
                    entry["dxy_vol"] = int(h['Volume'].iloc[-1])
        except: pass

    for p, slug in SLUGS.items():
        data = safe_get_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
        if not data or not data[0].get('markets'): continue
        for m in data[0]['markets']:
            raw_title = (m.get('groupItemTitle') or m.get('question')).lower()
            clean = re.sub(r'[^a-z0-9]', '_', raw_title).strip('_')
            clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
            
            prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            if prices: entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
            entry[f"{p}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
            entry[f"{p}_{clean}_oi"] = round(float(m.get('openInterest', 0)), 2)
            entry[f"{p}_{clean}_liq"] = round(float(m.get('liquidity', 0)), 2)
            
            tokens = m.get('clobTokenIds')
            if tokens:
                tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                book = safe_get_json(f"https://clob.polymarket.com/book?token_id={tid}")
                if book and book.get('bids') and book.get('asks'):
                    entry[f"{p}_{clean}_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
                    entry[f"{p}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
                last_p = safe_get_json(f"https://clob.polymarket.com/price?token_id={tid}")
                if last_p and last_p.get('price'):
                    entry[f"{p}_{clean}_last_price"] = round(float(last_p['price']) * 100, 2)
    return entry

# --- PERSISTENCE & AUTO-SORT ENGINE ---
file_name = "gold_investment_pro.csv"
live_row = get_live_market_data()
df_new = pd.DataFrame([live_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name, low_memory=False)
    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
else:
    df_final = df_new

df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
df_final = df_final.dropna(subset=['date']).drop_duplicates(subset=['date']).sort_values('date')

# Format date with 'Z' for the final CSV
df_final['date'] = df_final['date'].dt.strftime("%Y-%m-%d %H:%M Z")

prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

# Organize Columns
yf_order = ["gold_price", "oil_wti", "dxy_index", "vix_index", "gold_vix", "real_yield_proxy", "silver_price", "gold_miners", "copper_price", "gld_etf_vol", "dxy_vol", "treasury_10y"]
priority_cols = ['date'] + [c for c in yf_order if c in df_final.columns]
other_cols = [c for c in df_final.columns if c not in priority_cols]
df_final = df_final[priority_cols + sorted(other_cols)]

df_final.to_csv(file_name, index=False)
print(f"🏁 Update Successful (UTC). Date: {live_row['date']}")
