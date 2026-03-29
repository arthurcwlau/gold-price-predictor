import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re

def get_strategic_volume_data():
    print("--- 🏦 2026 Gold Intelligence: Volume & Probability Active ---")
    
    # STRATEGIC EVENT SLUGS
    GOLD_SETTLE_SLUG = "gc-settle-jun-2026"
    OIL_HIT_SLUG = "cl-hit-jun-2026"
    FED_JUNE_SLUG = "fed-decision-in-june-825"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0
    }

    # 1. Macro Data
    try:
        gold = yf.Ticker("GC=F").history(period="7d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1]
        oil = yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1]
        entry.update({"gold_price": round(gold, 2), "dxy_index": round(dxy, 2), "oil_wti": round(oil, 2)})
    except Exception as e:
        print(f"!! Macro Data Error: {e}")

    # 2. Polymarket Data Fetcher
    def fetch_event_markets(slug):
        try:
            r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            return r[0]['markets'] if r else []
        except: return []

    def process_strategic_curve(slug, prefix, entry_dict):
        markets = fetch_event_markets(slug)
        for m in markets:
            title = m.get('groupItemTitle') or m.get('question') or ""
            
            # Tidy column name (e.g. "<$3,800" -> "under_3800")
            clean_name = title.replace('$', '').replace('+', '').replace(',', '').replace('<', 'under_').replace('>', 'over_').strip().lower()
            clean_name = re.sub(r'[^a-z0-9]', '_', clean_name).replace('__', '_')
            
            # Probability
            prices = m.get('outcomePrices')
            if isinstance(prices, str): prices = json.loads(prices)
            entry_dict[f"{prefix}_{clean_name}_prob"] = round(float(prices[0]) * 100, 2)
            
            # Volume (Total USD traded in this specific bracket)
            vol = float(m.get('volume', 0))
            entry_dict[f"{prefix}_{clean_name}_vol"] = round(vol, 2)

    # Execute strategic fetches
    process_strategic_curve(GOLD_SETTLE_SLUG, "gold_settle", entry)
    process_strategic_curve(OIL_HIT_SLUG, "oil_hit", entry)
    process_strategic_curve(FED_JUNE_SLUG, "fed_june", entry)

    return entry

# --- Save & Tidy Logic ---
new_row = get_strategic_volume_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # Align to current strategic columns (Discarding any old scrapped data)
    df_combined = pd.concat([df_old, df_new], sort=False)
    df_combined = df_combined[df_new.columns]
    df_combined = df_combined.drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Dashboard Updated: {len(df_new.columns)} metrics (Prob + Vol) recorded.")
