import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re
import numpy as np
import logging

# --- 🛰️ SYSTEM CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_fred_data(session, api_key):
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE": return {}
    series = {
        "inflation_expectation": "T10YIE", "yield_curve_spread": "T10Y2Y",
        "real_yield_10y": "DFII10", "fed_balance_sheet": "WALCL",
        "credit_stress_spread": "BAMLH0A0HYM2", "usd_global_confidence": "DTWEXBGS", 
        "usd_sentiment_index": "UMCSENT"
    }
    data = {}
    for key, series_id in series.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&sort_order=desc&limit=3"
            resp = session.get(url, timeout=10).json()
            if 'observations' in resp:
                for obs in resp['observations']:
                    if obs['value'] != ".":
                        data[key] = float(obs['value'])
                        break
        except: pass
    return data

def fetch_yfinance_data():
    """Captures OHLCV using your preferred reference names."""
    tickers = {
        "gold_price": "GC=F", "oil_wti": "CL=F", "silver": "SI=F", "copper_price": "HG=F",
        "usd_etf": "UUP", "vix_index": "^VIX", "gold_vix": "^GVZ", "real_yield_proxy": "TIP",
        "gold_miners": "GDX", "treasury_10y": "^TNX", "btc_sentiment": "BTC-USD", 
        "geopol_ita": "ITA", "skew_index": "^SKEW", "eastern_bid": "XAUCNY=X"
    }
    data = {}
    for key, symbol in tickers.items():
        try:
            t = yf.Ticker(symbol)
            h = t.history(period="7d", interval="1h")
            if not h.empty:
                last = h.iloc[-1]
                data[key] = round(last['Close'], 2)
                data[f"{key}_high"] = round(last['High'], 2)
                data[f"{key}_low"] = round(last['Low'], 2)
                if 'Volume' in h.columns: data[f"{key}_volume"] = int(last['Volume'])
                
                # Internal Volume Logic
                if key == "gold_price":
                    gld = yf.Ticker("GLD").history(period="5d")
                    if not gld.empty: data["gld_etf_vol"] = int(gld['Volume'].iloc[-1])
        except: pass
    return data

def fetch_polymarket_data(session):
    slugs = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825", "recession": "us-recession-by-end-of-2026"}
    res = {}
    for p, s in slugs.items():
        try:
            d = session.get(f"https://gamma-api.polymarket.com/events?slug={s}", timeout=10).json()
            for m in d[0]['markets']:
                t = (m.get('groupItemTitle') or m.get('question')).lower()
                c = re.sub(r'_+', '_', re.sub(r'[^a-z0-9]', '_', t).strip('_'))
                
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: res[f"{p}_{c}_prob"] = round(float(prices[0]) * 100, 2)
                res[f"{p}_{c}_vol"] = round(float(m.get('volume', 0)), 2)
                res[f"{p}_{c}_oi"] = round(float(m.get('openInterest', 0)), 2)
                
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    bk = session.get(f"https://clob.polymarket.com/book?token_id={tid}", timeout=10).json()
                    if bk.get('bids') and bk.get('asks'):
                        res[f"{p}_{c}_spread"] = round(float(bk['asks'][0]['price']) - float(bk['bids'][0]['price']), 4)
                        res[f"{p}_{c}_depth"] = round(sum([float(x['size']) for x in bk['bids'][:5]]), 2)
                        res[f"{p}_{c}_liq"] = round(float(m.get('liquidity', 0)), 2)
        except: pass
    return res

def main():
    fn = "gold_investment_pro.csv"
    with requests.Session() as s:
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        row = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")}
        row.update(fetch_yfinance_data())
        row.update(fetch_fred_data(s, os.getenv("FRED_API_KEY")))
        row.update(fetch_polymarket_data(s))

    # 1. LOAD & DEDUPLICATE COLUMNS (Fixes the .1, .2, .3 issues)
    if os.path.exists(fn):
        df = pd.read_csv(fn)
        # Remove any columns ending in .1, .2, .3 caused by previous sync errors
        df = df.loc[:, ~df.columns.str.contains(r'\.\d+$')]
    else:
        df = pd.DataFrame()

    df_new = pd.DataFrame([row])
    df = pd.concat([df, df_new], ignore_index=True, sort=False)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # 2. THE REPAIR BRIDGE (Fills the empty gaps from naming changes)
    bridge = {
        'silver_price': 'silver', 'oil_price': 'oil_wti', 
        'btc_digital_gold_price': 'btc_sentiment', 'usd_etf_price': 'usd_etf',
        'recession_us_recession_by_end_of_2026_prob': 'recession_prob'
    }
    for old, target in bridge.items():
        if old in df.columns:
            df[target] = df[target].fillna(df[old])

    # 3. SIGNALS (Velocity/MA)
    prob_cols = [c for c in df.columns if c.endswith('_prob')]
    for col in prob_cols:
        base = col.replace('_prob', '')
        df[f"{base}_velocity"] = df[col].diff().round(2)
        df[f"{base}_velocity_ma6"] = df[f"{base}_velocity"].rolling(6, min_periods=1).mean().round(2)
        df[f"{base}_signal"] = (df[f"{base}_velocity"] > df[f"{base}_velocity_ma6"]).astype(int)

    # 4. NEAT GROUPING (As Requested)
    yfinance_core = ['gold_price', 'oil_wti', 'silver', 'copper_price', 'usd_etf', 'vix_index', 'gold_vix', 'real_yield_proxy', 'gold_miners', 'gld_etf_vol', 'treasury_10y']
    yfinance_ext = [c for c in df.columns if any(x in c for x in ['_high', '_low', '_volume']) and not any(p in c for p in ['gold_', 'oil_', 'fed_', 'recession_'])]
    
    poly_list = sorted([c for c in df.columns if any(c.startswith(p) for p in ['gold_', 'oil_', 'fed_', 'recession_']) and c not in yfinance_core + yfinance_ext])
    
    macro_sent = ['btc_sentiment', 'geopol_ita', 'skew_index', 'eastern_bid', 'inflation_expectation', 'yield_curve_spread', 'real_yield_10y', 'fed_balance_sheet', 'credit_stress_spread', 'usd_global_confidence', 'usd_sentiment_index']
    
    # Define Final Order
    final_cols = ['date'] + yfinance_core + yfinance_ext + poly_list + macro_sent
    # Filter to only existing columns and maintain order
    df = df[[c for c in final_cols if c in df.columns]]

    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 Update Complete. Gaps filled and columns neatly sorted.")

if __name__ == "__main__": main()
