import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re
import logging

# --- 🛰️ SYSTEM CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_fred_data(session, api_key):
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE": return {}
    series = {
        "inflation_expectation": "T10YIE",
        "yield_curve_spread": "T10Y2Y",
        "real_yield_10y": "DFII10",
        "fed_balance_sheet": "WALCL",
        "credit_stress_spread": "BAMLH0A0HYM2"
    }
    data = {}
    for key, series_id in series.items():
        try:
            # We fetch 2 observations to ensure if the latest is '.' we have a fallback
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&sort_order=desc&limit=2"
            resp = session.get(url, timeout=10).json()
            if 'observations' in resp:
                # Try latest, then previous if latest is null
                for obs in resp['observations']:
                    val = obs['value']
                    if val != ".":
                        data[key] = float(val)
                        break
        except: pass
    return data

def fetch_yfinance_data():
    tickers = {
        "gold_price": "GC=F", "oil_wti": "CL=F", "silver": "SI=F", 
        "copper_price": "HG=F", "dxy_index": "DX-Y.NYB", "vix_index": "^VIX", 
        "gold_vix": "^GVZ", "real_yield_proxy": "TIP", "gold_miners": "GDX", "treasury_10y": "^TNX"
    }
    data = {}
    for key, symbol in tickers.items():
        try:
            h = yf.Ticker(symbol).history(period="5d")
            if not h.empty:
                data[key] = round(h['Close'].iloc[-1], 2)
                if key == "gold_price":
                    g = yf.Ticker("GLD").history(period="5d")
                    if not g.empty: data["gld_etf_vol"] = int(g['Volume'].iloc[-1])
                if key == "dxy_index":
                    data["dxy_vol"] = int(h['Volume'].iloc[-1]) if 'Volume' in h.columns else 0
        except: pass
    return data

def fetch_polymarket_data(session):
    slugs = {
        "gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825", "recession": "us-recession-by-end-of-2026"
    }
    res = {}
    for p, s in slugs.items():
        try:
            d = session.get(f"https://gamma-api.polymarket.com/events?slug={s}", timeout=10).json()
            if not d or not d[0].get('markets'): continue
            for m in d[0]['markets']:
                t = (m.get('groupItemTitle') or m.get('question')).lower()
                c = re.sub(r'_+', '_', re.sub(r'[^a-z0-9]', '_', t).strip('_'))
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: res[f"{p}_{c}_prob"] = round(float(prices[0]) * 100, 2)
                res[f"{p}_{c}_vol"] = round(float(m.get('volume', 0)), 2)
                res[f"{p}_{c}_liq"] = round(float(m.get('liquidity', 0)), 2)
                res[f"{p}_{c}_oi"] = round(float(m.get('openInterest', 0)), 2)
                tks = m.get('clobTokenIds')
                if tks:
                    tid = tks[0] if isinstance(tks, list) else json.loads(tks)[0]
                    bk = session.get(f"https://clob.polymarket.com/book?token_id={tid}", timeout=10).json()
                    if bk.get('bids') and bk.get('asks'):
                        res[f"{p}_{c}_spread"] = round(float(bk['asks'][0]['price']) - float(bk['bids'][0]['price']), 4)
                        res[f"{p}_{c}_depth"] = round(sum([float(x['size']) for x in bk['bids'][:5]]), 2)
        except: pass
    return res

def main():
    fn = "gold_investment_pro.csv"
    with requests.Session() as s:
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        row = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")}
        row.update(fetch_fred_data(s, os.getenv("FRED_API_KEY")))
        row.update(fetch_yfinance_data())
        row.update(fetch_polymarket_data(s))

    df = pd.concat([pd.read_csv(fn) if os.path.exists(fn) else pd.DataFrame(), pd.DataFrame([row])], ignore_index=True)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # --- 🏗️ CRITICAL FIX: MAP BEFORE CALCULATING ---
    mapping = {
        'recession_us_recession_by_end_of_2026_prob': 'recession_prob',
        'recession_us_recession_by_end_of_2026_vol': 'recession_vol',
        'recession_us_recession_by_end_of_2026_oi': 'recession_oi',
        'recession_us_recession_by_end_of_2026_liq': 'recession_liq'
    }
    for l, sh in mapping.items():
        if l in df.columns:
            df[sh] = df[sh].fillna(df[l])

    # 🧬 CALCULATE VELOCITY & SIGNALS
    prob_cols = [c for c in df.columns if c.endswith('_prob')]
    for col in prob_cols:
        base = col.replace('_prob', '')
        df[f"{base}_velocity"] = df[col].diff().round(2)
        df[f"{base}_velocity_ma6"] = df[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
        df[f"{base}_signal"] = (df[f"{base}_velocity"] > df[f"{base}_velocity_ma6"]).astype(int)

    # 🧹 FINAL CLEANUP & GROUPING
    y_cols = ['gold_price', 'oil_wti', 'silver', 'copper_price', 'dxy_index', 'vix_index', 'gold_vix', 'real_yield_proxy', 'gold_miners', 'gld_etf_vol', 'dxy_vol', 'treasury_10y']
    f_cols = ['inflation_expectation', 'yield_curve_spread', 'real_yield_10y', 'fed_balance_sheet', 'credit_stress_spread']
    junk = [c for c in df.columns if 'recession_us_recession' in c]
    p_cols = sorted([c for c in df.columns if c not in ['date'] + y_cols + f_cols + junk])
    
    df = df[['date'] + y_cols + f_cols + p_cols]
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 Pulse Successful. Recession Math Fixed.")

if __name__ == "__main__": main()
