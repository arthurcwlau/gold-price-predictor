# DYNAMIC 

import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re
import logging
import time

# --- 🛰️ LOGGING & CONFIG ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 🏦 MODULE 1: FRED (Hardened) ---
def fetch_fred_data(session, api_key):
    if not api_key: return {}
    # Use the same series mapping
    series = {"inflation_expectation":"T10YIE","yield_curve_spread":"T10Y2Y","real_yield_10y":"DFII10","fed_balance_sheet":"WALCL","credit_stress_spread":"BAMLH0A0HYM2"}
    data = {}
    for k, s in series.items():
        try:
            r = session.get(f"https://api.stlouisfed.org/fred/series/observations?series_id={s}&api_key={api_key}&file_type=json&sort_order=desc&limit=1", timeout=15).json()
            if 'observations' in r and r['observations']:
                val = r['observations'][0]['value']
                data[k] = float(val) if val != "." else None
        except Exception as e:
            logging.warning(f"FRED failure for {k}: {e}")
    return data

# --- 📈 MODULE 2: YFINANCE ---
def fetch_yfinance_data():
    tickers = {"gold_price":"GC=F","oil_wti":"CL=F","silver":"SI=F","copper_price":"HG=F","dxy_index":"DX-Y.NYB","vix_index":"^VIX","gold_vix":"^GVZ","real_yield_proxy":"TIP","gold_miners":"GDX","treasury_10y":"^TNX"}
    data = {}
    for k, s in tickers.items():
        try:
            h = yf.Ticker(s).history(period="5d")
            if not h.empty:
                data[k] = round(h['Close'].iloc[-1], 2)
                if k == "gold_price":
                    g = yf.Ticker("GLD").history(period="5d")
                    if not g.empty: data["gld_etf_vol"] = int(g['Volume'].iloc[-1])
        except: pass
    return data

# --- 📰 MODULE 3: NEWS ---
def fetch_news_narrative(session, av_key, nyt_key):
    data = {}
    if av_key:
        try:
            url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=GLD&apikey={av_key}"
            resp = session.get(url, timeout=15).json()
            if "feed" in resp:
                scores = [float(item['overall_sentiment_score']) for item in resp['feed'][:5]]
                if scores: data['news_sentiment_score'] = round(sum(scores) / len(scores), 3)
        except: pass
    if nyt_key:
        for word in ["gold", "recession"]:
            try:
                url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?q={word}&api-key={nyt_key}"
                resp = session.get(url, timeout=15).json()
                data[f'nyt_{word}_hits'] = resp.get('response', {}).get('meta', {}).get('hits', 0)
                time.sleep(6)
            except: pass
    return data

# --- 🛰️ MODULE 4: SMART POLYMARKET DISCOVERY ---
def fetch_polymarket_data(session):
    url = "https://clob.polymarket.com/markets?active=true"
    results = {}
    try:
        markets = session.get(url, timeout=20).json()
        # Deduplication Strategy: Group similar questions together
        for m in markets:
            q = m.get('question', '').lower()
            prefix = ""
            if "gold" in q: prefix = "gold"
            elif "recession" in q: prefix = "recession"
            elif "fed" in q or "interest rate" in q: prefix = "fed"
            
            if prefix:
                # 1. CLEANING: Remove dates and symbols to consolidate columns
                clean = re.sub(r'[^a-z0-9]', '_', q).strip('_')
                # Only keep first 30 chars to force grouping of similar questions
                clean = f"{prefix}_{clean[:30]}"
                
                # 2. FETCH DATA
                token_id = m['tokens'][0]['token_id']
                p_data = session.get(f"https://clob.polymarket.com/price?token_id={token_id}", timeout=10).json()
                if 'price' in p_data:
                    results[f"{clean}_prob"] = round(float(p_data['price']) * 100, 2)
                    
                # 3. DEPTH (Optional: only for high liq)
                if float(m.get('liquidity', 0)) > 1000:
                    b_data = session.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=10).json()
                    if b_data.get('bids'):
                        results[f"{clean}_depth"] = round(sum([float(x['size']) for x in b_data['bids'][:5]]), 2)
    except Exception as e:
        logging.error(f"Polymarket Discovery error: {e}")
    return results

# --- 🏗️ ORCHESTRATION ---
def main():
    fn = "gold_investment_pro.csv"
    with requests.Session() as s:
        s.headers.update({"User-Agent": "GoldProRobust/1.0"})
        row = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")}
        row.update(fetch_fred_data(s, os.getenv("FRED_API_KEY")))
        row.update(fetch_yfinance_data())
        row.update(fetch_news_narrative(s, os.getenv("ALPHA_VANTAGE_API_KEY"), os.getenv("NYT_API_KEY")))
        row.update(fetch_polymarket_data(s))

    # Load existing and Forward-Fill (Ensures core data isn't missing if API blips)
    if os.path.exists(fn):
        df_old = pd.read_csv(fn)
        df_new = pd.DataFrame([row])
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = pd.DataFrame([row])

    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')
    
    # FORWARD FILL Core Columns (Fixes the blip issue)
    core_cols = ['recession_prob', 'inflation_expectation', 'credit_stress_spread', 'real_yield_10y']
    for c in [cc for cc in core_cols if cc in df.columns]:
        df[c] = df[c].ffill(limit=3) # Carry over last known value for up to 3 hours

    # Signal Logic
    for c in [c for c in df.columns if c.endswith('_prob')]:
        b = c.replace('_prob', '')
        df[f"{b}_velocity"] = df[c].diff().round(2)
        df[f"{b}_velocity_ma6"] = df[f"{b}_velocity"].rolling(6, min_periods=1).mean().round(2)
        df[f"{b}_signal"] = (df[f"{b}_velocity"] > df[f"{b}_velocity_ma6"]).astype(int)

    # Save
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 Robust Update Complete. Rows: {len(df)}")

if __name__ == "__main__": main()
