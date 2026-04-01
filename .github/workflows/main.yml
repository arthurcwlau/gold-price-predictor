import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re

# --- 🏦 BANK Macro Anchor: FRED Integration ---
def get_fred_data(api_key):
    # Skip if the key is the placeholder or missing
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE":
        print("⚠️ FRED API Key missing or placeholder. Skipping.")
        return {}
        
    print("--- 🏦 Macro Anchor: FRED Integration ---")
    # Adding High Yield Spread (Credit Stress) for better prediction
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
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'observations' in data and data['observations']:
                    val = data['observations'][0]['value']
                    fred_data[key] = float(val) if val != "." else None
                    print(f"✅ Fetched {key}: {val}")
            else:
                print(f"❌ FRED Error for {series_id}: {response.status_code}")
        except Exception as e:
            print(f"⚠️ Error fetching {series_id}: {e}")
    return fred_data

def safe_get_json(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 404: return None
        response.raise_for_status()
        return response.json()
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
    
    # 1. Fetch FRED Data
    if fred_key:
        entry.update(get_fred_data(fred_key))

    # 2. Institutional Yahoo Tickers
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

    # 3. Polymarket Pulse
    for p, slug in SLUGS.items():
        data = safe_get_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
        if not data or not data[0].get('markets'): continue
        for m in data[0]['markets']:
            raw_title = (m.get('groupItemTitle') or m.get('question')).lower()
            clean = re.sub(r'[^a-z0-9]', '_', raw_title).strip('_')
            clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
            
            prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            if prices: entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
            entry[f"{p}_{clean}_oi"] = round(float(m.get('openInterest', 0)), 2)
            
            tokens = m.get('clobTokenIds')
            if tokens:
                tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                book = safe_get_json(f"https://clob.polymarket.com/book?token_id={tid}")
                if book and book.get('bids'):
                    entry[f"{p}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
    return entry

# --- PERSISTENCE ENGINE ---
file_name = "gold_investment_pro.csv"
# SECRETS LOGIC: Prioritize os.getenv from GitHub Actions
fred_api_key = os.getenv("FRED_API_KEY", "YOUR_ACTUAL_KEY_HERE")
live_row = get_live_market_data(fred_api_key)
df_new = pd.DataFrame([live_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name, low_memory=False)
    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
else:
    df_final = df_new

df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
df_final = df_final.dropna(subset=['date']).drop_duplicates(subset=['date']).sort_values('date')

# Predictor logic
prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

# --- THE COLUMN ORGANIZER ---
df_final['date'] = df_final['date'].dt.strftime("%Y-%m-%d %H:%M Z")

priority_cols = [
    'date', 'gold_price', 'oil_wti', 'dxy_index', 'vix_index', 
    'inflation_expectation', 'yield_curve_spread', 'real_yield_10y', 
    'fed_balance_sheet', 'credit_stress_spread'
]
actual_priority = [c for c in priority_cols if c in df_final.columns]
other_cols = [c for c in df_final.columns if c not in actual_priority]

df_final = df_final[actual_priority + sorted(other_cols)]
df_final.to_csv(file_name, index=False)
print(f"🏁 MASTER Update Successful. Date: {live_row['date']}")
