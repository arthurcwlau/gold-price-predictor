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
    """Captures OHLCV for all assets."""
    tickers = {
        "gold": "GC=F", "oil": "CL=F", "silver": "SI=F", "copper": "HG=F",
        "usd_etf": "UUP", "btc_sentiment": "BTC-USD", "geopol_ita": "ITA", 
        "gold_miners": "GDX", "treasury_10y": "^TNX", "vix": "^VIX", "skew": "^SKEW",
        "eastern_bid": "XAUCNY=X"
    }
    data = {}
    for key, symbol in tickers.items():
        try:
            ticker_obj = yf.Ticker(symbol)
            h = ticker_obj.history(period="7d", interval="1h")
            if not h.empty:
                last_bar = h.iloc[-1]
                data[f"{key}_price"] = round(last_bar['Close'], 2)
                data[f"{key}_high"] = round(last_bar['High'], 2)
                data[f"{key}_low"] = round(last_bar['Low'], 2)
                if 'Volume' in h.columns:
                    data[f"{key}_volume"] = int(last_bar['Volume'])
        except: pass
    return data

def fetch_polymarket_data(session):
    """Restores full Orderbook Depth, Spread, and Liquidity."""
    slugs = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825", "recession": "us-recession-by-end-of-2026"}
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
                res[f"{p}_{c}_oi"] = round(float(m.get('openInterest', 0)), 2)
                
                # Orderbook logic (Restored)
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    book = session.get(f"https://clob.polymarket.com/book?token_id={tid}", timeout=10).json()
                    if book.get('bids') and book.get('asks'):
                        res[f"{p}_{c}_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
                        res[f"{p}_{c}_liq"] = round(float(m.get('liquidity', 0)), 2)
                        res[f"{p}_{c}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
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

    df = pd.concat([pd.read_csv(fn) if os.path.exists(fn) else pd.DataFrame(), pd.DataFrame([row])], ignore_index=True)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # --- 🏗️ TRADING ENGINE (ATR, SMA, RSI) ---
    if 'gold_price' in df.columns:
        tr = pd.concat([df['gold_high'] - df['gold_low'], 
                        abs(df['gold_high'] - df['gold_price'].shift(1)), 
                        abs(df['gold_low'] - df['gold_price'].shift(1))], axis=1).max(axis=1)
        df['gold_atr_14'] = tr.rolling(window=14, min_periods=1).mean().round(2)
        df['gold_sma_20'] = df['gold_price'].rolling(window=20, min_periods=1).mean().round(2)
        
        delta = df['gold_price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        df['gold_rsi_14'] = (100 - (100 / (1 + (gain/loss)))).fillna(50).round(2)

    # --- 🌉 THE REPAIR BRIDGE (Syncing old/new names) ---
    mapping = {
        'btc_digital_gold_price': 'btc_sentiment_price', 'btc_digital_gold_high': 'btc_sentiment_high',
        'btc_digital_gold_low': 'btc_sentiment_low', 'btc_digital_gold_volume': 'btc_sentiment_volume',
        'recession_us_recession_by_end_of_2026_prob': 'recession_prob'
    }
    for old, current in mapping.items():
        if old in df.columns:
            df[current] = df[current].fillna(df[old])
            df.drop(columns=[old], inplace=True)

    # --- 🧹 NEAT DISPLAY SORTING ---
    # Group 1: Core yfinance (Price/OHLC/Vol/Technicals)
    yfinance_main = sorted([c for c in df.columns if any(x in c for x in ['gold_', 'oil_', 'silver_', 'copper_'])])
    
    # Group 2: Polymarket (Probabilities/Orderbook)
    # Filter for columns that have prefix matching our slugs
    poly_prefixes = ['gold_', 'oil_', 'fed_', 'recession_']
    polymarket_cols = sorted([c for c in df.columns if any(c.startswith(p) for p in poly_prefixes) and c not in yfinance_main])
    
    # Group 3: All Sentiments & Macro (BTC, SKEW, ITA, USD, FRED)
    sentiment_macro = sorted([c for c in df.columns if any(x in c for x in ['btc_', 'skew', 'geopol_', 'usd_', 'vix', 'treasury_', 'inflation_', 'yield_', 'real_', 'fed_balance', 'credit_']) and c not in yfinance_main + polymarket_cols])
    
    # Re-order final
    ordered = ['date'] + yfinance_main + polymarket_cols + sentiment_macro
    # Add any columns missed (edge cases)
    missed = [c for c in df.columns if c not in ordered]
    df = df[ordered + missed]
    
    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 MASTER PULSE COMPLETE. Columns Grouped Neatly.")

if __name__ == "__main__": main()
