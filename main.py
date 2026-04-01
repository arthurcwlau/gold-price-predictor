import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import os
import json
import re
import logging

# --- CONFIGURATION & LOGGING ---
# A: Enterprise Standard Logging (No more silent failures)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# D: Type Safety - Define explicit types for structural anchors
STRUCTURAL_DTYPES = {
    'gold_price': 'float64',
    'oil_wti': 'float64',
    'inflation_expectation': 'float64',
    'credit_stress_spread': 'float64',
    'recession_prob': 'float64'
}

# --- MODULES ---

# C: Modularity - Specialized fetch functions
def fetch_fred_data(session, api_key):
    """Module to handle Federal Reserve data."""
    if not api_key or api_key == "YOUR_ACTUAL_KEY_HERE":
        logging.warning("FRED API Key missing. Skipping Macro Anchors.")
        return {}

    series_map = {
        "inflation_expectation": "T10YIE",
        "yield_curve_spread": "T10Y2Y",
        "real_yield_10y": "DFII10",
        "fed_balance_sheet": "WALCL",
        "credit_stress_spread": "BAMLH0A0HYM2"
    }
    
    results = {}
    for key, series_id in series_map.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&sort_order=desc&limit=1"
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            val = data['observations'][0]['value']
            results[key] = float(val) if val != "." else None
        except Exception as e:
            logging.error(f"FRED Failure ({series_id}): {e}")
    return results

def fetch_institutional_data(session):
    """Module to handle Yahoo Finance and Market Tickers."""
    tickers = {
        "gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", 
        "vix_index": "^VIX", "gold_vix": "^GVZ", "real_yield_proxy": "TIP",
        "silver_price": "SI=F", "gold_miners": "GDX", "copper_price": "HG=F",
        "treasury_10y": "^TNX"
    }
    
    data = {}
    for key, symbol in tickers.items():
        try:
            ticker = yf.Ticker(symbol)
            h = ticker.history(period="5d")
            if not h.empty:
                data[key] = round(h['Close'].iloc[-1], 2)
                if key == "gold_price":
                    # Proxy for gold interest
                    gld_vol = yf.Ticker("GLD").history(period="5d")['Volume']
                    data["gld_etf_vol"] = int(gld_vol.iloc[-1]) if not gld_vol.empty else 0
                if key == "dxy_index":
                    data["dxy_vol"] = int(h['Volume'].iloc[-1]) if 'Volume' in h.columns else 0
        except Exception as e:
            logging.error(f"Ticker Failure ({symbol}): {e}")
    return data

def fetch_polymarket_data(session):
    """Module to handle Prediction Market Intelligence."""
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
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            events = resp.json()
            if not events or not events[0].get('markets'): continue
            
            for m in events[0]['markets']:
                # Clean naming logic
                title = (m.get('groupItemTitle') or m.get('question')).lower()
                clean = re.sub(r'[^a-z0-9]', '_', title).strip('_')
                clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
                
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: results[f"{prefix}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
                
                # Liquidity & Volume Metrics
                results[f"{prefix}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
                results[f"{prefix}_{clean}_liq"] = round(float(m.get('liquidity', 0)), 2)
                results[f"{prefix}_{clean}_oi"] = round(float(m.get('openInterest', 0)), 2)
                
                # Orderbook Depth
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    book_url = f"https://clob.polymarket.com/book?token_id={tid}"
                    book = session.get(book_url, timeout=10).json()
                    if book.get('bids') and book.get('asks'):
                        results[f"{prefix}_{clean}_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
                        results[f"{prefix}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
        except Exception as e:
            logging.error(f"Polymarket Failure ({slug}): {e}")
            
    return results

# --- ORCHESTRATION ---

def main():
    file_name = "gold_investment_pro.csv"
    fred_key = os.getenv("FRED_API_KEY")
    
    # A: Network Efficiency - Use a Session for all requests
    with requests.Session() as session:
        session.headers.update({"User-Agent": "GoldProIntelligence/2.0"})
        
        logging.info("Starting Data Collection Pulse...")
        
        # C: Execute specialized modules
        row = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M Z")}
        row.update(fetch_fred_data(session, fred_key))
        row.update(fetch_institutional_data(session))
        row.update(fetch_polymarket_data(session))
        
    # Load and Persist
    df_new = pd.DataFrame([row])
    if os.path.exists(file_name):
        # D: Type Safety during load
        df_old = pd.read_csv(file_name, low_memory=False)
        df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
    else:
        df_final = df_new

    # Process Signals
    df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
    df_final = df_final.dropna(subset=['date']).drop_duplicates(subset=['date']).sort_values('date')

    for col in [c for c in df_final.columns if c.endswith('_prob')]:
        base = col.replace('_prob', '')
        df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
        df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
        df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

    # Self-Healing Mapping (The "Bridge")
    mapping = {'recession_us_recession_by_end_of_2026_prob': 'recession_prob'}
    for long, short in mapping.items():
        if long in df_final.columns: df_final[short] = df_final[long]

    # Final Cleanup & Save
    df_final['date'] = df_final['date'].dt.strftime("%Y-%m-%d %H:%M Z")
    junk = [c for c in df_final.columns if 'recession_us_recession' in c]
    df_final.drop(columns=junk, inplace=True, errors='ignore')
    
    df_final.to_csv(file_name, index=False)
    logging.info(f"🏁 Pulse Complete. Row count: {len(df_final)}")

if __name__ == "__main__":
    main()
