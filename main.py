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
    tickers = {
        "gold_price": "GC=F", "oil_wti": "CL=F", "silver": "SI=F", 
        "copper_price": "HG=F", "usd_etf": "UUP", "vix_index": "^VIX", 
        "gold_vix": "^GVZ", "real_yield_proxy": "TIP", "gold_miners": "GDX", 
        "treasury_10y": "^TNX", "btc_sentiment": "BTC-USD", "geopol_ita": "ITA"
    }
    data = {}
    for key, symbol in tickers.items():
        try:
            t = yf.Ticker(symbol)
            h = t.history(period="5d", interval="1h")
            if not h.empty:
                last_bar = h.iloc[-1]
                data[key] = round(last_bar['Close'], 2)
                # Group metadata for cycles
                prefix = key.replace('_price', '').replace('_index', '').replace('_wti', '')
                data[f"{prefix}_high"] = round(last_bar['High'], 2)
                data[f"{prefix}_low"] = round(last_bar['Low'], 2)
                if 'Volume' in h.columns:
                    # Specific request: usd_volume
                    vol_key = "usd_volume" if key == "usd_etf" else f"{prefix}_volume"
                    data[vol_key] = int(last_bar['Volume'])
                
                if key == "gold_price":
                    g = yf.Ticker("GLD").history(period="5d")
                    if not g.empty: data["gld_etf_vol"] = int(g['Volume'].iloc[-1])
        except: pass
    return data

def fetch_polymarket_data(session):
    # Slug confirmed from user input
    slugs = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825", "recession": "us-recession-by-end-of-2026"}
    res = {}
    for p, s in slugs.items():
        try:
            d = session.get(f"https://gamma-api.polymarket.com/events?slug={s}", timeout=10).json()
            for m in d[0]['markets']:
                t = (m.get('groupItemTitle') or m.get('question')).lower()
                c = re.sub(r'_+', '_', re.sub(r'[^a-z0-9]', '_', t).strip('_'))
                
                # UNIFICATION: Force short names for recession
                prefix = f"{p}_{c}"
                if "us_recession_by_end_of_2026" in prefix: prefix = "recession"
                
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: res[f"{prefix}_prob"] = round(float(prices[0]) * 100, 2)
                res[f"{prefix}_vol"] = round(float(m.get('volume', 0)), 2)
                res[f"{prefix}_liq"] = round(float(m.get('liquidity', 0)), 2)
                res[f"{prefix}_oi"] = round(float(m.get('openInterest', 0)), 2)
                
                tks = m.get('clobTokenIds')
                if tks:
                    tid = tks[0] if isinstance(tks, list) else json.loads(tks)[0]
                    bk = session.get(f"https://clob.polymarket.com/book?token_id={tid}", timeout=10).json()
                    if bk.get('bids') and bk.get('asks'):
                        res[f"{prefix}_spread"] = round(float(bk['asks'][0]['price']) - float(bk['bids'][0]['price']), 4)
                        res[f"{prefix}_depth"] = round(sum([float(x['size']) for x in bk['bids'][:5]]), 2)
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

    if os.path.exists(fn):
        df = pd.read_csv(fn)
        # 1. PURGE JUNK: Delete dotted columns and the long recession columns
        df = df.loc[:, ~df.columns.str.contains(r'\.\d+$')]
        long_recession_cols = [c for c in df.columns if "us_recession_by_end_of_2026" in c]
        df.drop(columns=long_recession_cols, inplace=True, errors='ignore')
    else:
        df = pd.DataFrame()

    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True, sort=False)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # Indicators (Always calculated on the final clean columns)
    if 'gold_price' in df.columns:
        df['gold_log_return'] = np.log(df['gold_price'] / df['gold_price'].shift(1)).round(6)
        
    # Universal Velocity/MA for all Probability columns (including 'recession_prob')
    prob_cols = [c for c in df.columns if c.endswith('_prob')]
    for col in prob_cols:
        base = col.replace('_prob', '')
        df[f"{base}_velocity"] = df[col].diff().round(2)
        df[f"{base}_velocity_ma6"] = df[f"{base}_velocity"].rolling(6, min_periods=1).mean().round(2)
        df[f"{base}_signal"] = (df[f"{base}_velocity"] > df[f"{base}_velocity_ma6"]).astype(int)

    # FINAL NEAT ORDER
    y_core = ['gold_price', 'oil_wti', 'silver', 'usd_etf', 'usd_volume', 'copper_price', 'vix_index', 'gold_vix', 'real_yield_proxy', 'gold_miners', 'gld_etf_vol', 'treasury_10y']
    sent = ['btc_sentiment', 'geopol_ita', 'gold_log_return', 'inflation_expectation', 'yield_curve_spread', 'real_yield_10y', 'fed_balance_sheet', 'credit_stress_spread', 'usd_global_confidence', 'usd_sentiment_index']
    
    p_cols = sorted([c for c in df.columns if c not in ['date'] + y_core + sent])
    
    df = df[['date'] + y_core + sent + p_cols]
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 MASTER PURGE COMPLETE. Long columns deleted. All rows filled.")

if __name__ == "__main__": main()
