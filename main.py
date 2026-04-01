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

# --- 🛰️ SYSTEM CONFIGURATION & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 🏦 MODULE 1: FRED (Macro Anchors) ---
def fetch_fred_data(session, api_key):
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE": return {}
    series = {"inflation_expectation":"T10YIE","yield_curve_spread":"T10Y2Y","real_yield_10y":"DFII10","fed_balance_sheet":"WALCL","credit_stress_spread":"BAMLH0A0HYM2"}
    data = {}
    for k, s in series.items():
        try:
            r = session.get(f"https://api.stlouisfed.org/fred/series/observations?series_id={s}&api_key={api_key}&file_type=json&sort_order=desc&limit=1", timeout=10).json()
            if 'observations' in r and r['observations']: 
                data[k] = float(r['observations'][0]['value']) if r['observations'][0]['value'] != "." else None
        except: pass
    return data

# --- 📈 MODULE 2: YFINANCE (Institutional Anchors) ---
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

# --- 📰 MODULE 3: NEWS & NARRATIVE (AV & NYT) ---
def fetch_news_narrative(session, av_key, nyt_key):
    data = {}
    if av_key:
        try:
            url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=GLD&apikey={av_key}"
            resp = session.get(url, timeout=15).json()
            if "feed" in resp and resp["feed"]:
                scores = [float(item['overall_sentiment_score']) for item in resp['feed'][:5]]
                data['news_sentiment_score'] = round(sum(scores) / len(scores), 3)
        except: pass
    if nyt_key:
        for word in ["gold", "recession"]:
            try:
                url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?q={word}&api-key={nyt_key}"
                resp = session.get(url, timeout=10).json()
                data[f'nyt_{word}_hits'] = resp.get('response', {}).get('meta', {}).get('hits', 0)
                time.sleep(6) # Rate Limit
            except: pass
    return data

# --- 🛰️ MODULE 4: DYNAMIC POLYMARKET DISCOVERY ---

def discover_active_markets(session):
    """Scans the Polymarket CLOB for new active markets matching our keywords."""
    url = "https://clob.polymarket.com/markets?active=true"
    try:
        markets = session.get(url, timeout=15).json()
        keywords = {"gold": ["gold", "xau"], "recession": ["recession"], "fed": ["fed ", "fomc"]}
        targets = []
        for m in markets:
            q = m.get('question', '').lower()
            for cat, words in keywords.items():
                if any(w in q for w in words):
                    # We usually want the 'Yes' outcome or the specific bracket token
                    token_id = m['tokens'][0]['token_id']
                    targets.append({"prefix": cat, "title": q, "token_id": token_id})
        return targets
    except: return []

def fetch_discovered_data(session, targets):
    """Fetches price and depth for the discovered tokens."""
    results = {}
    for t in targets:
        try:
            # Clean name for CSV
            clean_name = re.sub(r'[^a-z0-9]', '_', t['title'].lower()).strip('_')
            clean_name = re.sub(r'_+', '_', clean_name)[:40]
            
            # Price
            p_data = session.get(f"https://clob.polymarket.com/price?token_id={t['token_id']}", timeout=10).json()
            if 'price' in p_data:
                results[f"{t['prefix']}_{clean_name}_prob"] = round(float(p_data['price']) * 100, 2)
            
            # Book Depth
            b_data = session.get(f"https://clob.polymarket.com/book?token_id={t['token_id']}", timeout=10).json()
            if b_data.get('bids') and b_data.get('asks'):
                results[f"{t['prefix']}_{clean_name}_spread"] = round(float(b_data['asks'][0]['price']) - float(b_data['bids'][0]['price']), 4)
                results[f"{t['prefix']}_{clean_name}_depth"] = round(sum([float(x['size']) for x in b_data['bids'][:5]]), 2)
        except: pass
    return results

# --- 🏗️ ORCHESTRATION ---
def main():
    fn = "gold_investment_pro.csv"
    with requests.Session() as s:
        s.headers.update({"User-Agent": "GoldProDynamic/1.0"})
        row = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")}
        row.update(fetch_fred_data(s, os.getenv("FRED_API_KEY")))
        row.update(fetch_yfinance_data())
        row.update(fetch_news_narrative(s, os.getenv("ALPHA_VANTAGE_API_KEY"), os.getenv("NYT_API_KEY")))
        
        # Dynamic Discovery Phase
        active_list = discover_active_markets(s)
        row.update(fetch_discovered_data(s, active_list))

    df = pd.concat([pd.read_csv(fn) if os.path.exists(fn) else pd.DataFrame(), pd.DataFrame([row])], ignore_index=True)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # Self-Healing Velocity logic for any '_prob' column found
    for c in [c for c in df.columns if c.endswith('_prob')]:
        b = c.replace('_prob', '')
        df[f"{b}_velocity"] = df[c].diff().round(2)
        df[f"{b}_velocity_ma6"] = df[f"{b}_velocity"].rolling(6, min_periods=1).mean().round(2)
        df[f"{b}_signal"] = (df[f"{b}_velocity"] > df[f"{b}_velocity_ma6"]).astype(int)

    # Core Re-Mapping for Recession (keeps your primary signal stable)
    mapping = {'recession_us_recession_by_end_of_2026_prob': 'recession_prob'}
    if 'recession_prob' in df.columns:
        for l, sh in mapping.items():
            if l in df.columns: df[sh] = df[sh].fillna(df[l])

    # Dynamic Grouping
    y_cols = ['gold_price', 'oil_wti', 'silver', 'copper_price', 'dxy_index', 'vix_index', 'gold_vix', 'real_yield_proxy', 'gold_miners', 'gld_etf_vol', 'treasury_10y']
    f_cols = ['inflation_expectation', 'yield_curve_spread', 'real_yield_10y', 'fed_balance_sheet', 'credit_stress_spread']
    n_cols = ['news_sentiment_score', 'nyt_gold_hits', 'nyt_recession_hits']
    p_cols = sorted([c for c in df.columns if c not in ['date'] + y_cols + f_cols + n_cols])
    
    df = df[['date'] + y_cols + f_cols + n_cols + p_cols]
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 Dynamic Update Complete. Active Markets Tracked: {len(active_list)}")

if __name__ == "__main__": main()
