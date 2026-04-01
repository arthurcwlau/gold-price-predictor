import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re

# --- 🏦 BANK Macro Anchor: FRED Integration ---
def get_fred_data(api_key):
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE":
        return {}
    fred_series = {
        "inflation_expectation": "T10YIE",
        "yield_curve_spread": "T10Y2Y",
        "real_yield_10y": "DFII10",
        "fed_balance_sheet": "WALCL",
        "credit_stress_spread": "BAMLH0A0HYM2"
    }
    fred_data = {}
    for key, series_id in fred_series.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&sort_order=desc&limit=1"
            response = requests.get(url, timeout=10).json()
            if 'observations' in response:
                val = response['observations'][0]['value']
                fred_data[key] = float(val) if val != "." else None
        except: pass
    return fred_data

def safe_get_json(url):
    try:
        response = requests.get(url, timeout=10)
        return response.json() if response.status_code == 200 else None
    except: return None

def get_live_market_data(fred_key=None):
    print("--- 🛰️ 2026 Pulse: Master Intelligence Mode ---")
    SLUGS = {
        "gold": "gc-settle-jun-2026", 
        "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825",
        "recession": "us-recession-by-end-of-2026"
    }
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")
    entry = {"date": now_ts}
    
    if fred_key: entry.update(get_fred_data(fred_key))

    tickers = {
        "gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", 
        "vix_index": "^VIX", "gold_vix": "^GVZ", "real_yield_proxy": "TIP",
        "silver_price": "SI=F", "gold_miners": "GDX", "copper_price": "HG=F",
        "treasury_10y": "^TNX" # RESTORED
    }
    

    for key, ticker in tickers.items():
        try:
            h = yf.Ticker(ticker).history(period="5d")
            if not h.empty: 
                entry[key] = round(h['Close'].iloc[-1], 2)
                
                # RESTORED: Catch GLD volume as a proxy for Gold interest
                if key == "gold_price":
                    gld_h = yf.Ticker("GLD").history(period="5d")
                    if not gld_h.empty: entry["gld_etf_vol"] = int(gld_h['Volume'].iloc[-1])
                
                # RESTORED: Catch DXY volume (Note: This may often be 0 for this specific ticker)
                if key == "dxy_index":
                    entry["dxy_vol"] = int(h['Volume'].iloc[-1]) if 'Volume' in h.columns else 0
                    
        except Exception as e:
            print(f"⚠️ Error fetching {ticker}: {e}")
            pass


    
    for p, slug in SLUGS.items():
        data = safe_get_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
        if not data or not data[0].get('markets'): continue
        for m in data[0]['markets']:
            raw_title = (m.get('groupItemTitle') or m.get('question')).lower()
            clean = re.sub(r'[^a-z0-9]', '_', raw_title).strip('_')
            clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
            
            prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            if prices: entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
            
            # --- RESTORED POLYMARKET METRICS ---
            entry[f"{p}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
            entry[f"{p}_{clean}_liq"] = round(float(m.get('liquidity', 0)), 2)
            entry[f"{p}_{clean}_oi"] = round(float(m.get('openInterest', 0)), 2)
            
            tokens = m.get('clobTokenIds')
            if tokens:
                tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                book = safe_get_json(f"https://clob.polymarket.com/book?token_id={tid}")
                if book and book.get('bids') and book.get('asks'):
                    # RESTORED SPREAD
                    entry[f"{p}_{clean}_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
                    entry[f"{p}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
    return entry

# --- PERSISTENCE ENGINE ---
file_name = "gold_investment_pro.csv"
fred_api_key = os.getenv("FRED_API_KEY", "YOUR_ACTUAL_KEY_HERE")
live_row = get_live_market_data(fred_api_key)
df_new = pd.DataFrame([live_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name, low_memory=False)
    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
else:
    df_final = df_new

# 1. Standardize and Calculate Signals
df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
df_final = df_final.dropna(subset=['date']).drop_duplicates(subset=['date']).sort_values('date')

prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

# 2. --- NEW CLEANUP LOGIC: Mapping long names to short names ---
# This fixes the empty "recession_velocity" issue
mapping = {
    'recession_us_recession_by_end_of_2026_prob': 'recession_prob',
    'recession_us_recession_by_end_of_2026_velocity': 'recession_velocity',
    'recession_us_recession_by_end_of_2026_velocity_ma6': 'recession_velocity_ma6',
    'recession_us_recession_by_end_of_2026_signal': 'recession_signal'
}

for long_col, short_col in mapping.items():
    if long_col in df_final.columns:
        # Move data from long column to short column
        df_final[short_col] = df_final[long_col]

# 3. --- FINAL COLUMN ORGANIZER & JUNK REMOVAL ---
df_final['date'] = df_final['date'].dt.strftime("%Y-%m-%d %H:%M Z")

# Define priority columns
priority_cols = [
    'date', 'gold_price', 'oil_wti', 'recession_prob', 
    'recession_velocity', 'recession_velocity_ma6', 'recession_signal',
    'inflation_expectation', 'credit_stress_spread'
]

# List of junk/duplicate columns to delete
junk_to_delete = [c for c in df_final.columns if 'recession_us_recession' in c]

actual_priority = [c for c in priority_cols if c in df_final.columns]
other_cols = sorted([c for c in df_final.columns if c not in actual_priority and c not in junk_to_delete])

df_final = df_final[actual_priority + other_cols]

df_final.to_csv(file_name, index=False)
print(f"🏁 MASTER Update Successful with Mapping. Date: {live_row['date']}")
