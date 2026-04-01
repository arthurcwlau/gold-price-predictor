import pandas as pd
import yfinance as yf
import requests
from fredapi import Fred
import numpy as np
from datetime import datetime
import os

# --- CONFIGURATION ---
FRED_API_KEY = os.getenv('FRED_API_KEY')
CSV_FILE = 'gold_investment_pro (23).csv'
fred = Fred(api_key=FRED_API_KEY)

# Polymarket Slugs to Track
POLY_SLUGS = {
    "gold_10000": "will-gold-gc-hit-high-10000-by-end-of-june",
    "recession": "us-recession-by-end-of-2026",
    # Add other existing slugs here...
}

def get_polymarket_data(slug):
    try:
        url = f"https://gamma-api.polymarket.com/markets?slug={slug}"
        response = requests.get(url).json()
        if not response: return {}
        m = response[0]
        # Calculate approximate depth/liquidity from orderbook or simplified metrics
        return {
            "prob": float(m.get('outcomePrices', [0, 0])[0]) * 100,
            "oi": float(m.get('openInterest', 0)),
            "liq": float(m.get('liquidity', 0)),
            "depth": float(m.get('volume', 0)) # Using volume as a proxy for depth in this context
        }
    except:
        return {"prob": np.nan, "oi": np.nan, "liq": np.nan, "depth": np.nan}

def main():
    # 1. Fetch YFinance Data
    gold = yf.Ticker("GC=F").history(period="1d")['Close'].iloc[-1]
    oil = yf.Ticker("CL=F").history(period="1d")['Close'].iloc[-1]
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    copper = yf.Ticker("HG=F").history(period="1d")['Close'].iloc[-1]
    
    # 2. Fetch FRED Data
    data_row = {
        "date": datetime.utcnow().strftime('%Y-%m-%d %H:%00 Z'),
        "gold_price": gold,
        "oil_wti": oil,
        "vix_index": vix,
        "copper_price": copper,
        "inflation_expectation": fred.get_series('T10YIE').iloc[-1],
        "real_yield_10y": fred.get_series('DFII10').iloc[-1],
        "credit_stress_spread": fred.get_series('STLFSI4').iloc[-1],
        "yield_curve_spread": fred.get_series('T10Y2Y').iloc[-1],
    }

    # 3. Fetch New Polymarket Data ($10k Gold)
    pm_10k = get_polymarket_data(POLY_SLUGS["gold_10000"])
    data_row["gold_10000_prob"] = pm_10k["prob"]
    data_row["gold_10000_oi"] = pm_10k["oi"]
    data_row["gold_10000_liq"] = pm_10k["liq"]
    data_row["gold_10000_depth"] = pm_10k["depth"]

    # 4. Update CSV
    df_new = pd.DataFrame([data_row])
    if os.path.exists(CSV_FILE):
        df_old = pd.read_csv(CSV_FILE)
        # Calculate Velocity for the new 10k target
        if "gold_10000_prob" in df_old.columns:
            data_row["gold_10000_velocity"] = pm_10k["prob"] - df_old["gold_10000_prob"].iloc[-1]
        
        df_final = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_final = df_new

    df_final.to_csv(CSV_FILE, index=False)
    print(f"Data updated successfully at {data_row['date']}")

if __name__ == "__main__":
    main()
