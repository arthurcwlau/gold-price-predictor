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
                data[f"{key}_high"] = round(last_bar['High'], 2)
                data[f"{key}_low"] = round(last_bar['Low'], 2)
                
                if key == "usd_etf": data["usd_volume"] = int(last_bar['Volume'])
                else: data[f"{key}_volume"] = int(last_bar['Volume']) if 'Volume' in h.columns else 0
                
                if key == "gold_price":
                    g = yf.Ticker("GLD").history(period="5d")
                    if not g.empty: data["gld_etf_vol"] = int(g['Volume'].iloc[-1])
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
                prefix = f"{p}_{c}"
                if "us_recession" in prefix: prefix = "recession"
                
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: res[f"{prefix}_prob"] = round(float(prices[0]) * 100, 2)
                res[f"{prefix}_vol"] = round(float(m.get('volume', 0)), 2)
                res[f"{prefix}_liq"] = round(float(m.get('liquidity', 0)), 2)
                res[f"{prefix}_oi"] = round(float(m.get('openInterest', 0)), 2)
                
                tks = m.get('clobTokenIds')
                if tks and float(m.get('liquidity', 0)) > 0:
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

    df = pd.read_csv(fn) if os.path.exists(fn) else pd.DataFrame()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True, sort=False)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # --- 🧹 DATA PRUNING ---
    to_delete = [c for c in df.columns if re.search(r'\.\d+$', c) or "us_recession_by_end_of_2026" in c]
    df.drop(columns=[c for c in to_delete if c in df.columns], inplace=True, errors='ignore')

    # --- ⚖️ REFINED FAIR VALUE CALCULATION ---
    tier_midpoints = {
        "gold_under_3_800": 3600, "gold_3_800_4_200": 4000, "gold_4_200_4_600": 4400,
        "gold_4_600_5_000": 4800, "gold_5_000_5_400": 5200, "gold_5_400_5_800": 5600,
        "gold_5_800_6_200": 6000, "gold_over_6_200": 6400
    }
    
    # Calculate weighted average based on probabilities
    weighted_sum = 0
    total_prob = 0
    for key, midpoint in tier_midpoints.items():
        col = f"{key}_prob"
        if col in df.columns:
            weighted_sum += df[col].fillna(0) * midpoint
            total_prob += df[col].fillna(0)
    
    df['fair_value'] = (weighted_sum / total_prob).ffill()

    # Probability Velocity Signals
    prob_cols = [c for c in df.columns if c.endswith('_prob')]
    for col in prob_cols:
        base = col.replace('_prob', '')
        df[f"{base}_velocity"] = df[col].diff().round(2)
        df[f"{base}_velocity_ma6"] = df[f"{base}_velocity"].rolling(6, min_periods=1).mean().round(2)
        df[f"{base}_signal"] = (df[f"{base}_velocity"] > df[f"{base}_velocity_ma6"]).astype(int)

    # --- 🏗️ NEAT SORTING ---
    gold_cols = ['gold_price', 'gold_price_high', 'gold_price_low', 'gold_price_volume', 'fair_value']
    y_core = sorted([c for c in df.columns if any(x in c for x in ['oil_wti', 'silver', 'copper_price', 'usd_etf', 'usd_volume', 'vix_index', 'treasury_10y', 'btc_sentiment', 'geopol_ita'])])
    p_core = sorted([c for c in df.columns if any(c.startswith(p) for p in ['fed_', 'recession_']) or (c.startswith('gold_') and c not in gold_cols)])
    others = sorted([c for c in df.columns if c not in ['date'] + gold_cols + y_core + p_core])
    
    df = df[['date'] + gold_cols + y_core + p_core + others]
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 MASTER UNIFICATION COMPLETE.")

if __name__ == "__main__": main()
