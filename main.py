import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_fred_data(session, api_key):
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE": return {}
    series = {"real_yield_10y": "DFII10", "yield_curve_spread": "T10Y2Y", "usd_sentiment": "UMCSENT", "inflation_expect": "T10YIE"}
    data = {}
    for key, series_id in series.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&sort_order=desc&limit=1"
            resp = session.get(url, timeout=10).json()
            if 'observations' in resp: data[key] = float(resp['observations'][0]['value'])
        except: pass
    return data

def fetch_yfinance_data():
    tickers = {"gold_price": "GC=F", "oil_wti": "CL=F", "silver": "SI=F", "vix_index": "^VIX", "treasury_10y": "^TNX", "usd_index": "DX-Y.NYB"}
    data = {}
    for key, symbol in tickers.items():
        try:
            h = yf.Ticker(symbol).history(period="2d", interval="1h")
            if not h.empty: data[key] = round(h['Close'].iloc[-1], 2)
        except: pass
    return data

def fetch_polymarket_data(session):
    slugs = {"gold": "gc-settle-jun-2026", "fed": "fed-decision-in-june-825", "recession": "us-recession-by-end-of-2026"}
    res = {}
    for p, s in slugs.items():
        try:
            d = session.get(f"https://gamma-api.polymarket.com/events?slug={s}", timeout=10).json()
            for m in d[0]['markets']:
                t = (m.get('groupItemTitle') or m.get('question')).lower()
                c = re.sub(r'[^a-z0-9]', '_', t).strip('_')
                prefix = f"{p}_{c}"
                if "recession" in prefix: prefix = "recession"
                
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: res[f"{prefix}_prob"] = round(float(prices[0]) * 100, 2)
                res[f"{prefix}_vol"] = round(float(m.get('volume', 0)), 2)
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

    df = pd.read_csv(fn) if os.path.exists(fn) else pd.DataFrame()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True, sort=False)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # --- ⚖️ CORE FAIR VALUE CALCULATION ---
    midpoints = {"under_3_800": 3600, "3_800_4_200": 4000, "4_200_4_600": 4400, "4_600_5_000": 4800, "5_000_5_400": 5200, "5_400_5_800": 5600, "5_800_6_200": 6000, "over_6_200": 6400}
    w_sum, t_prob = 0, 0
    for k, v in midpoints.items():
        col = f"gold_{k}_prob"
        if col in df.columns:
            w_sum += df[col].fillna(0) * v
            t_prob += df[col].fillna(0)
    df['fair_value'] = (w_sum / t_prob).replace([np.inf, -np.inf], np.nan).ffill()

    # --- 🏗️ CLEAN SORTING ---
    cols = ['date', 'gold_price', 'fair_value']
    probs = sorted([c for c in df.columns if c.endswith('_prob')])
    macros = sorted([c for c in df.columns if c not in cols + probs])
    
    df = df[cols + probs + macros]
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 CORE DATA UNIFIED: {len(df.columns)} columns.")

if __name__ == "__main__": main()
