import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re

def get_full_strategic_data():
    print("--- 🏦 2026 June Strategic Dashboard: Full Curves Active ---")
    
    # STRATEGIC EVENT SLUGS (As confirmed by your snippets)
    GOLD_HIT_SLUG = "gc-hit-jun-2026"
    OIL_HIT_SLUG = "cl-hit-jun-2026"
    FED_JUNE_SLUG = "fed-decision-in-june-825"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0
    }

    # 1. Macro Data (The Physical Realities)
    try:
        # Weekend-safe logic (looks back 7 days for the latest close)
        gold = yf.Ticker("GC=F").history(period="7d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1]
        oil = yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1]
        entry.update({"gold_price": round(gold, 2), "dxy_index": round(dxy, 2), "oil_wti": round(oil, 2)})
        print(f"📈 Macro: Gold ${entry['gold_price']} | Oil ${entry['oil_wti']} | DXY {entry['dxy_index']}")
    except Exception as e:
        print(f"!! Macro Data Error: {e}")

    # 2. Polymarket Data Fetcher
    def fetch_event_markets(slug):
        try:
            r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            return r[0]['markets'] if r else []
        except: return []

    def process_curve(slug, prefix, entry_dict):
        markets = fetch_event_markets(slug)
        for m in markets:
            title = m.get('groupItemTitle') or m.get('question') or ""
            
            # Clean title for CSV columns (e.g. "$10,000" -> "10000", "50+ bps" -> "50bps")
            clean_name = title.replace('$', '').replace('+', '').replace(',', '').strip().lower()
            clean_name = re.sub(r'[^a-z0-9]', '_', clean_name)
            
            key = f"{prefix}_{clean_name}_prob"
            
            # Parse Price Probability
            prices = m.get('outcomePrices')
            if isinstance(prices, str): prices = json.loads(prices)
            entry_dict[key] = round(float(prices[0]) * 100, 2)

    # A. Retrieve the Gold Hit Curve ($5k - $10k)
    process_curve(GOLD_HIT_SLUG, "gold", entry)

    # B. Retrieve the Oil Hit Curve ($110 - $200)
    process_curve(OIL_HIT_SLUG, "oil", entry)

    # C. Retrieve the Fed June Decision Curve (All entries)
    process_curve(FED_JUNE_SLUG, "fed_june", entry)

    return entry

# --- Save & Tidy Logic ---
new_row = get_full_strategic_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # Align columns: discard old "Daily" or "Past" columns no longer in the script
    df_combined = pd.concat([df_old, df_new], sort=False)
    # Keep it tidy: only use columns present in the new strategic row
    df_combined = df_combined[df_new.columns]
    df_combined = df_combined.drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Strategic Data Saved. Captured {len(df_new.columns)} strategic data points.")
