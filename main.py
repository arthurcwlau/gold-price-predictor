import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re
import logging

# --- 🛰️ SYSTEM CONFIGURATION & LOGGING ---
# Standardizes error reporting and tracking in GitHub Actions
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- 🏦 MODULE 1: FRED (Macro Anchors) ---
def fetch_fred_data(session, api_key):
    """Fetches high-conviction macro data from the Federal Reserve."""
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE":
        logging.warning("FRED API Key missing. Skipping Macro module.")
        return {}
        
    fred_series = {
        "inflation_expectation": "T10YIE",
        "yield_curve_spread": "T10Y2Y",
        "real_yield_10y": "DFII10",
        "fed_balance_sheet": "WALCL",
        "credit_stress_spread": "BAMLH0A0HYM2"
    }
    fred_data = {}
    for key, series_id in fred_series.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&sort_order=desc&limit=1"
            resp = session.get(url, timeout=10).json()
            if 'observations' in resp and resp['observations']:
                val = resp['observations'][0]['value']
                fred_data[key] = float(val) if val != "." else None
        except Exception as e:
            logging.error(f"FRED module error ({key}): {e}")
    return fred_data

# --- 📈 MODULE 2: YFINANCE (Institutional Anchors) ---
def fetch_yfinance_data():
    """Fetches real-time price data for institutional assets."""
    tickers = {
        "gold_price": "GC=F", 
        "oil_wti": "CL=F", 
        "silver_price": "SI=F", # Mapped to 'silver' in cleanup
        "copper_price": "HG=F",
        "dxy_index": "DX-Y.NYB", 
        "vix_index": "^VIX", 
        "gold_vix": "^GVZ", 
        "real_yield_proxy": "TIP",
        "gold_miners": "GDX", 
        "treasury_10y": "^TNX"
    }
    y_data = {}
    for key, symbol in tickers.items():
        try:
            ticker_obj = yf.Ticker(symbol)
            h = ticker_obj.history(period="5d")
            if not h.empty:
                y_data[key] = round(h['Close'].iloc[-1], 2)
                
                # Internal Volume Logic
                if key == "gold_price":
                    gld = yf.Ticker("GLD").history(period="5d")
                    if not gld.empty: y_data["gld_etf_vol"] = int(gld['Volume'].iloc[-1])
                if key == "dxy_index":
                    y_data["dxy_vol"] = int(h['Volume'].iloc[-1]) if 'Volume' in h.columns else 0
        except Exception as e:
            logging.error(f"yfinance module error ({symbol}): {e}")
    return y_data

# --- 🛰️ MODULE 3: POLYMARKET (Sentiment Anchors) ---
def fetch_polymarket_data(session):
    """Extracts Whale conviction data from Polymarket CLOB."""
    slugs = {
        "gold": "gc-settle-jun-2026", 
        "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825", 
        "recession": "us-recession-by-end-of-2026"
    }
    results = {}
    for prefix, slug in slugs.items():
        try:
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            data = session.get(url, timeout=10).json()
            if not data or not data[0].get('markets'): continue
            
            for m in data[0]['markets']:
                # Dynamic Clean Naming logic
                title = (m.get('groupItemTitle') or m.get('question')).lower()
                clean = re.sub(r'[^a-z0-9]', '_', title).strip('_')
                clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
                
                # Prices and Probabilities
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: results[f"{prefix}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
                
                # Metadata (Volume, OI, Liquidity)
                results[f"{prefix}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
                results[f"{prefix}_{clean}_liq"] = round(float(m.get('liquidity', 0)), 2)
                results[f"{prefix}_{clean}_oi"] = round(float(m.get('openInterest', 0)), 2)
                
                # Orderbook Depth & Spread
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    book_url = f"https://clob.polymarket.com/book?token_id={tid}"
                    book = session.get(book_url, timeout=10).json()
                    if book.get('bids') and book.get('asks'):
                        results[f"{prefix}_{clean}_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
                        results[f"{prefix}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
        except Exception as e:
            logging.error(f"Polymarket module error ({slug}): {e}")
    return results

# --- 🏗️ ORCHESTRATION & PERSISTENCE ---
def main():
    file_name = "gold_investment_pro.csv"
    fred_key = os.getenv("FRED_API_KEY")
    
    # 1. Atomic Data Collection using Session for maximum speed
    with requests.Session() as session:
        session.headers.update({"User-Agent": "GoldProIntelligence/3.0"})
        logging.info("Initiating Multi-Source Pulse...")
        
        row = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")}
        row.update(fetch_fred_data(session, fred_key))
        row.update(fetch_yfinance_data())
        row.update(fetch_polymarket_data(session))

    # 2. Persistence Engine
    df_new = pd.DataFrame([row])
    if os.path.exists(file_name):
        df_old = pd.read_csv(file_name, low_memory=False)
        df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
    else:
        df_final = df_new

    # 3. Standardization & Signal Math
    df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
    df_final = df_final.dropna(subset=['date']).drop_duplicates(subset=['date']).sort_values('date')
    
    prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
    for col in prob_cols:
        base = col.replace('_prob', '')
        df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
        df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
        df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

    # 4. FIX: Internal Mapping (Cleans empty Silver/Recession columns)
    mapping = {
        'recession_us_recession_by_end_of_2026_prob': 'recession_prob',
        'recession_us_recession_by_end_of_2026_velocity': 'recession_velocity',
        'recession_us_recession_by_end_of_2026_velocity_ma6': 'recession_velocity_ma6',
        'recession_us_recession_by_end_of_2026_signal': 'recession_signal',
        'silver_price': 'silver'
    }
    for long, short in mapping.items():
        if long in df_final.columns:
            # Transfer data from long-name to short-name if short is empty
            df_final[short] = df_final[short].fillna(df_final[long])

    # 5. SOURCE-BASED GROUPING
    yfinance_cols = [
        'gold_price', 'oil_wti', 'silver', 'copper_price', 'dxy_index', 
        'vix_index', 'gold_vix', 'real_yield_proxy', 'gold_miners', 
        'gld_etf_vol', 'dxy_vol', 'treasury_10y'
    ]
    fred_cols = [
        'inflation_expectation', 'yield_curve_spread', 
        'real_yield_10y', 'fed_balance_sheet', 'credit_stress_spread'
    ]
    
    # Identify junk/duplicate columns for deletion
    junk = [c for c in df_final.columns if 'recession_us_recession' in c or c == 'silver_price']
    
    # Categorize Sentiment data (Alphabetical)
    metadata = ['date']
    poly_cols = sorted([c for c in df_final.columns if c not in metadata + yfinance_cols + fred_cols + junk])
    
    # Final Reorder
    final_order = [c for c in metadata + yfinance_cols + fred_cols + poly_cols if c in df_final.columns]
    df_final = df_final[final_order]
    
    # 6. Export
    df_final['date'] = df_final['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    df_final.to_csv(file_name, index=False)
    logging.info(f"🏁 MASTER Update Successful. CSV Organized. Columns: {len(df_final.columns)}")

if __name__ == "__main__":
    main()
