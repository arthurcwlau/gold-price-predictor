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
    print("--- 🛰️ 2026 Pulse: Reverting to Original Stable Mode ---")
    SLUGS = {
        "gold": "gc-settle-jun-2026", 
        "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825",
        "recession": "us-recession-by-end-of-2026"
    }
    
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    entry = {"date": now_ts}
    
    # EXACT ORIGINAL TICKERS
    tickers = {
        "gold_price": "GC=F", 
        "oil_wti": "CL=F", 
        "dxy_index": "DX-Y.NYB", 
        "vix_index": "^VIX",
        "gold_vix": "^GVZ",
        "real_yield_proxy": "TIP",
        "silver_price": "SI=F",
        "gold_miners": "GDX",
        "copper_price": "HG=F"
    }
    
    for key, ticker in tickers.items():
        try:
            # Using 5d tail(1) is more secure than 1d if the market is just opening/closing
            h = yf.Ticker(ticker).history(period="5d")
            if not h.empty: 
                entry[key] = round(h['Close'].iloc[-1], 2)
                if key == "gold_price":
                    gld_h = yf.Ticker("GLD").history(period="5d")
                    if not gld_h.empty: entry["gld_etf_vol"] = int(gld_h['Volume'].iloc[-1])
                if key == "dxy_index":
                    entry["dxy_vol"] = int(h['Volume'].iloc[-1])
        except: pass

    # ORIGINAL POLYMARKET LOGIC
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

# --- PERSISTENCE & SIGNAL ENGINE ---
file_name = "gold_investment_pro.csv"
live_row = get_live_market_data()
df_new = pd.DataFrame([live_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name, low_memory=False)
    
    # REPAIR LOGIC: If 'gold' exists, move it back to 'gold_price' to fix the "Lean" version gaps
    mapping = {'gold': 'gold_price', 'dxy': 'dxy_index', 'vix': 'vix_index', 'copper': 'copper_price'}
    for lean_col, old_col in mapping.items():
        if lean_col in df_old.columns:
            df_old[old_col] = df_old[old_col].fillna(df_old[lean_col])
    
    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
else:
    df_final = df_new

# Deduplicate and calculate indicators
df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
df_final = df_final.dropna(subset=['date']).drop_duplicates(subset=['date']).sort_values('date')

prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

# Clean up: Drop the "Lean" columns if they were created
df_final = df_final.drop(columns=[c for c in ['gold', 'dxy', 'vix', 'copper', 'au_cu_ratio', 'recession_prob', 'z_gold', 'z_fear', 'divergence', 'signal'] if c in df_final.columns])

df_final.to_csv(file_name, index=False)
print(f"🏁 Update Successful. Reverted to {len(df_final.columns)} original indicators.")
