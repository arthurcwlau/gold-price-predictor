import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re

# --- NEW: FRED Integration ---
def get_fred_data(api_key):
    print("--- 🏦 Macro Anchor: FRED Integration ---")
    fred_series = {
        "inflation_expectation": "T10YIE",
        "yield_curve_spread": "T10Y2Y",
        "financial_stress_idx": "STLFSI4",
        "m2_money_supply": "M2SL"
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

def get_live_market_data(fred_key=None):
    print("--- 🛰️ 2026 Pulse: Multi-Source Intelligence Mode ---")
    SLUGS = {
        "gold": "gc-settle-jun-2026", 
        "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825",
        "recession": "us-recession-by-end-of-2026"
    }
    
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")
    entry = {"date": now_ts}
    
    # 1. Macro & FRED Data
    if fred_key:
        fred_metrics = get_fred_data(fred_key)
        entry.update(fred_metrics)

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
                    gld_h = yf.Ticker("GLD").history(period="1d")
                    if not gld_h.empty: entry["gld_etf_vol"] = int(gld_h['Volume'].iloc[-1])
        except: pass

    # 3. Polymarket Deep Pulse
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

# --- EXECUTION ---
# Usage: Set your API key in your environment or replace 'YOUR_FRED_KEY_HERE'
fred_api_key = os.getenv("FRED_API_KEY", "YOUR_FRED_KEY_HERE")
live_row = get_live_market_data(fred_api_key)
# ... (rest of your auto-sort and persistence engine logic)
