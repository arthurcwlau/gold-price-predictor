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
    """Fetches high-conviction macro data with a fallback for null values."""
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
    """Volume-First OHLC: Captures all market metrics with strict naming."""
    tickers = {
        "gold": "GC=F", "oil": "CL=F", "silver": "SI=F", "copper": "HG=F",
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
                # Price mapping (User preference: 'gold_price', 'oil_wti')
                p_key = "gold_price" if key == "gold" else "oil_wti" if key == "oil" else f"{key}_price"
                data[p_key] = round(last['Close'], 2)
                # Metadata naming (Strict: asset_high, asset_low, asset_volume)
                data[f"{key}_high"] = round(last['High'], 2)
                data[f"{key}_low"] = round(last['Low'], 2)
                if 'Volume' in h.columns: data[f"{key}_volume"] = int(last['Volume'])
                
                # Internal Gold specific volume (GLD)
                if key == "gold":
                    gld = yf.Ticker("GLD").history(period="5d")
                    if not gld.empty: data["gld_etf_vol"] = int(gld['Volume'].iloc[-1])
        except Exception as e:
            logging.error(f"yfinance error for {key}: {e}")
    return data

def fetch_polymarket_data(session):
    """Deep Liquidity Check: Fetches prob, vol, and orderbook stats."""
    slugs = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825", "recession": "us-recession-by-end-of-2026"}
    res = {}
    for p, s in slugs.items():
        try:
            d = session.get(f"https://gamma-api.polymarket.com/events?slug={s}", timeout=10).json()
            if not d or not d[0].get('markets'): continue
            for m in d[0]['markets']:
                t = (m.get('groupItemTitle') or m.get('question')).lower()
                c = re.sub(r'_+', '_', re.sub(r'[^a-z0-9]', '_', t).strip('_'))
                
                # Prices & Volume
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: res[f"{p}_{c}_prob"] = round(float(prices[0]) * 100, 2)
                res[f"{p}_{c}_vol"] = round(float(m.get('volume', 0)), 2)
                res[f"{p}_{c}_oi"] = round(float(m.get('openInterest', 0)), 2)
                
                # Liquidity & Orderbook
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    book = session.get(f"https://clob.polymarket.com/book?token_id={tid}", timeout=10).json()
                    if book.get('bids') and book.get('asks'):
                        res[f"{p}_{c}_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
                        res[f"{p}_{c}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
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

    # 1. LOAD & SYNC
    df = pd.read_csv(fn) if os.path.exists(fn) else pd.DataFrame()
    df_new = pd.DataFrame([row])
    df = pd.concat([df, df_new], ignore_index=True, sort=False)
    df['date'] = pd.to_datetime(df['date'])
    df = df.drop_duplicates('date').sort_values('date')

    # 2. THE REPAIR BRIDGE (Fills gaps from naming mismatches)
    bridge = {
        'copper_price_high': 'copper_high', 'copper_price_low': 'copper_low', 'copper_price_volume': 'copper_volume',
        'gold_price_high': 'gold_high', 'gold_price_low': 'gold_low', 'gold_price_volume': 'gold_volume',
        'oil_price_high': 'oil_high', 'oil_price_low': 'oil_low', 'oil_price_volume': 'oil_volume',
        'silver_price': 'silver', 'recession_us_recession_by_end_of_2026_prob': 'recession_prob'
    }
    for old, target in bridge.items():
        if old in df.columns:
            if target not in df.columns: df[target] = np.nan
            df[target] = df[target].fillna(df[old])

    # 3. INDICATOR CALCULATIONS (On full DataFrame to avoid NaNs)
    if 'gold_price' in df.columns:
        # ATR (Average True Range)
        tr = pd.concat([df['gold_high'] - df['gold_low'], 
                        abs(df['gold_high'] - df['gold_price'].shift(1)), 
                        abs(df['gold_low'] - df['gold_price'].shift(1))], axis=1).max(axis=1)
        df['gold_atr_14'] = tr.rolling(window=14, min_periods=1).mean().round(2)
        df['gold_is_tradable'] = (df['gold_atr_14'] > 5.0).astype(int)
        df['gold_sma_20'] = df['gold_price'].rolling(window=20, min_periods=1).mean().round(2)
        df['gold_log_return'] = np.log(df['gold_price'] / df['gold_price'].shift(1))
        
        # RSI 14
        delta = df['gold_price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        df['gold_rsi_14'] = (100 - (100 / (1 + (gain/loss)))).fillna(50).round(2)

    # 4. NEAT DISPLAY SORTING
    core_market = ['gold_price', 'oil_wti', 'silver', 'copper_price', 'usd_etf', 'vix_index', 'gold_vix', 'real_yield_proxy', 'gold_miners', 'gld_etf_vol', 'treasury_10y']
    market_tech = sorted([c for c in df.columns if any(x in c for x in ['_high', '_low', '_volume', '_atr', '_rsi', '_sma', '_return', '_is_tradable']) and not any(p in c for p in ['gold_', 'oil_', 'fed_', 'recession_'])])
    
    poly_list = sorted([c for c in df.columns if any(c.startswith(p) for p in ['gold_', 'oil_', 'fed_', 'recession_']) and c not in core_market + market_tech])
    macro_sent = sorted(['btc_sentiment', 'geopol_ita', 'skew_index', 'eastern_bid', 'inflation_expectation', 'yield_curve_spread', 'real_yield_10y', 'fed_balance_sheet', 'credit_stress_spread', 'usd_global_confidence', 'usd_sentiment_index'])
    
    # Define Final Order
    final_order = ['date'] + core_market + market_tech + poly_list + [c for c in macro_sent if c in df.columns]
    
    # Cleanup: Drop internal bridge sources
    df.drop(columns=[c for c in bridge.keys() if c in df.columns], inplace=True)
    df = df[[c for c in final_order if c in df.columns]]

    df['date'] = df['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df.to_csv(fn, index=False)
    logging.info(f"🏁 MASTER PULSE COMPLETE. Columns populated and sorted.")

if __name__ == "__main__": main()
