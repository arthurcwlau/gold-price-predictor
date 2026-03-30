import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
import json
import re

def safe_get_json(url):
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 404: return None
        response.raise_for_status()
        return response.json()
    except: return None

def run_recovery_and_scrape():
    print("--- 🚀 2026 SUPER RECOVERY ENGINE: REBUILDING HISTORY ---")
    SLUGS = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825"}
    TICKERS = {"gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", "treasury_10y": "^TNX", "vix_index": "^VIX"}
    
    # 1. Backfill Macro History (Last 7 Days)
    history_dict = {}
    print("Pulling 7 days of hourly Macro data...")
    for key, ticker in TICKERS.items():
        try:
            h = yf.Ticker(ticker).history(period="7d", interval="1h")
            for ts, val in h['Close'].items():
                dt = ts.strftime('%Y-%m-%d %H:00')
                if dt not in history_dict: history_dict[dt] = {"date": dt}
                history_dict[dt][key] = round(val, 2)
        except: pass

    # 2. Backfill Polymarket History (Last 72 Hours)
    print("Identifying and backfilling Prediction Market tokens...")
    for p, slug in SLUGS.items():
        data = safe_get_json(f"https://gamma-api.polymarket.com/events?slug={slug}")
        if not data or not data[0].get('markets'): continue
        for m in data[0]['markets']:
            clean = re.sub(r'[^a-z0-9]', '_', (m.get('groupItemTitle') or m.get('question')).lower()).strip('_')
            clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
            
            tokens = m.get('clobTokenIds')
            if tokens:
                tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                h_data = safe_get_json(f"https://clob.polymarket.com/prices-history?market={tid}&interval=1h")
                if h_data and h_data.get('history'):
                    for p_point in h_data['history'][-72:]:
                        dt = datetime.fromtimestamp(p_point['t']).strftime("%Y-%m-%d %H:00")
                        if dt not in history_dict: history_dict[dt] = {"date": dt}
                        history_dict[dt][f"{p}_{clean}_prob"] = round(float(p_point['p']) * 100, 2)

    # 3. Create DataFrame and Calculate Signals
    df_final = pd.DataFrame(list(history_dict.values()))
    df_final = df_final.dropna(subset=['date']).sort_values('date')

    # Calculate Velocity and Signals
    prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
    for col in prob_cols:
        base = col.replace('_prob', '')
        df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
        df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
        df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

    # Save to CSV
    df_final.to_csv("gold_investment_pro.csv", index=False)
    print(f"🏁 RECOVERY COMPLETE! Total hourly rows recovered: {len(df_final)}")

run_recovery_and_scrape()
