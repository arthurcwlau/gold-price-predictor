import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re
import logging
import time

# --- 🛰️ SYSTEM CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 🏦 MODULE 1: FRED (Macro Anchors) ---
def fetch_fred_data(session, api_key):
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE": return {}
    series = {"inflation_expectation":"T10YIE","yield_curve_spread":"T10Y2Y","real_yield_10y":"DFII10","fed_balance_sheet":"WALCL","credit_stress_spread":"BAMLH0A0HYM2"}
    data = {}
    for k, s in series.items():
        try:
            r = session.get(f"https://api.stlouisfed.org/fred/series/observations?series_id={s}&api_key={api_key}&file_type=json&sort_order=desc&limit=1", timeout=10).json()
            if 'observations' in r and r['observations']: data[k] = float(r['observations'][0]['value']) if r['observations'][0]['value'] != "." else None
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
                if k == "dxy_index": data["dxy_vol"] = int(h['Volume'].iloc[-1]) if 'Volume' in h.columns else 0
        except: pass
    return data

# --- 📰 MODULE 3: NEWS & NARRATIVE (AV & NYT) ---
def fetch_news_narrative(session, av_key, nyt_key):
    data = {}
    # Alpha Vantage: Numerical Sentiment Score
    if av_key:
        try:
            url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=GLD&apikey={av_key}"
            resp = session.get(url, timeout=15).json()
            if "feed" in resp and resp["feed"]:
                scores = [float(item['overall_sentiment_score']) for item in resp['feed'][:5]]
                data['news_sentiment_score'] = round(sum(scores) / len(scores), 3)
        except: pass

    # NYT: Narrative Volume (Headline Hits)
    if nyt_key:
        for word in ["gold", "recession"]:
            try:
                url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?q={word}&api-key={nyt_key}"
                resp = session.get(url, timeout=10).json()
                data[f'nyt_{word}_hits'] = resp.get('response', {}).get('meta', {}).get('hits', 0)
                time.sleep(6) # Respecting NYT 10-req/min limit
            except: pass
    return data

# --- 🛰️ MODULE 4: POLYMARKET (Sentiment Anchors) ---
def fetch_polymarket_data(session):
    slugs = {"gold":"gc-settle-jun-2026","oil":"cl-hit-jun-2026","fed":"fed-decision-in-june-825","recession":"us-recession-by-end-of-2026"}
    res = {}
    for p, s in slugs.items():
        try:
            d = session.get(f"https://gamma-api.polymarket.com/events?slug={s}", timeout=10).json()
            for m in d[0]['markets']:
                t = (m.get('groupItemTitle') or m.get('question')).lower()
                c = re.sub(r'_+', '_', re.sub(r'[^a-z0-9]', '_', t).strip('_').replace('$', '').replace('<', 'under_').replace('>', 'over_'))
                pr = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if pr: res[f"{p}_{c}_prob"] = round(float(pr[0]) * 100, 2)
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

# --- 🏗️ ORCHESTRATION ---
def main():
    fn = "gold_investment_pro.csv"
    with requests.Session() as s:
        s.headers.update({"User-Agent": "GoldPro/6.0"})
        logging.info("Initiating Ultimate Intelligence Pulse...")
        
        row = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")}
        row.update(fetch_fred_data(s, os.getenv("FRED_API_KEY")))
        row.update(fetch_yfinance_data())
        row.update(fetch_news_narrative(s, os.getenv("ALPHA_VANTAGE_API_KEY"), os.getenv("NYT_API_KEY")))
        row.update(fetch_polymarket_data(s))

    df = pd.concat([pd.read_csv(fn) if os.path.exists(fn) else pd.DataFrame(), pd.DataFrame([row])], ignore_index=True)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # Signal Math
    for c in [c for c in df.columns if c.endswith('_prob')]:
        b = c.replace('_prob', '')
        df[f"{b}_velocity"] = df[c].diff().round(2)
        df[f"{b}_velocity_ma6"] = df[f"{b}_velocity"].rolling(6).mean().round(2)
        df[f"{b}_signal"] = (df[f"{b}_velocity"] > df[f"{b}_velocity_ma6"]).astype(int)

    # Re-Mapping & Cleanup
    mapping = {'recession_us_recession_by_end_of_2026_prob': 'recession_prob', 'recession_us_recession_by_end_of_2026_velocity': 'recession_velocity', 'recession_us_recession_by_end_of_2026_velocity_ma6': 'recession_velocity_ma6', 'recession_us_recession_by_end_of_2026_signal': 'recession_signal'}
    for l, sh in mapping.items():
        if l in df.columns: df[sh] = df[sh].fillna(df[l])

    # Source-Based Grouping
    y_cols = ['gold_price', 'oil_wti', 'silver', 'copper_price', 'dxy_index', 'vix_index', 'gold_vix', 'real_yield_proxy', 'gold_miners', 'gld_etf_vol', 'dxy_vol', 'treasury_10y']
    f_cols = ['inflation_expectation', 'yield_curve_spread', 'real_yield_10y', 'fed_balance_sheet', 'credit_stress_spread']
    n_cols = ['news_sentiment_score', 'nyt_gold_hits', 'nyt_recession_hits']
    junk = [c for c in df.columns if 'recession_us_recession' in c]
    p_cols = sorted([c for c in df.columns if c not in ['date'] + y_cols + f_cols + n_cols + junk])
    
    df = df[['date'] + y_cols + f_cols + n_cols + p_cols]
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 Ultimate Update Successful. Narrative Columns populated.")

if __name__ == "__main__": main()
