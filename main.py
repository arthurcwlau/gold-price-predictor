import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_pro_investment_data():
    print("--- 🏦 2026 Strategic Dashboard: Full-Curve Mode ---")
    
    # EVENT SLUGS (Top-level categories)
    OIL_EVENT = "cl-hit-jun-2026"
    GOLD_JUNE_EVENT = "gc-settle-jun-2026"
    GOLD_DAILY_EVENT = "xauusd-up-or-down-on-march-30-2026"
    FED_EVENT = "fed-rate-cut-by-629"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0,
        "market_spread": 0.0
    }

    # 1. Macro Data
    try:
        gold_h = yf.Ticker("GC=F").history(period="7d")
        dxy_h = yf.Ticker("DX-Y.NYB").history(period="7d")
        oil_h = yf.Ticker("CL=F").history(period="7d")
        entry.update({
            "gold_price": round(gold_h['Close'].iloc[-1], 2),
            "dxy_index": round(dxy_h['Close'].iloc[-1], 2),
            "oil_wti": round(oil_h['Close'].iloc[-1], 2)
        })
    except: print("!! Macro Data Fetch Failed")

    # 2. Polymarket "Curve Crawler"
    def crawl_event(slug, prefix):
        results = {}
        try:
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            resp = requests.get(url).json()
            if resp and len(resp) > 0:
                markets = resp[0].get('markets', [])
                for m in markets:
                    # Clean the title (e.g. "↑ $200" -> "up_200")
                    raw_title = m.get('groupItemTitle') or m.get('question')
                    clean_title = raw_title.replace('↑', 'up').replace('↓', 'down').replace('$', '').replace('<', 'under').replace('>', 'over').strip()
                    clean_title = clean_title.lower().replace(' ', '_').replace('-', '_')
                    
                    key = f"{prefix}_{clean_title}_prob"
                    
                    # Parse Price
                    prices = m.get('outcomePrices')
                    if isinstance(prices, str): prices = json.loads(prices)
                    results[key] = round(float(prices[0]) * 100, 2)
                    
                    # Capture spread for the very first market as a proxy
                    if prefix == "gold_daily" and "market_spread" not in entry:
                        entry["market_spread"] = -1.0
        except Exception as e: print(f"!! Error crawling {slug}: {e}")
        return results

    # Fetching all curves
    entry.update(crawl_event(OIL_EVENT, "oil"))
    entry.update(crawl_event(GOLD_JUNE_EVENT, "gold_june"))
    entry.update(crawl_event(GOLD_DAILY_EVENT, "gold_daily"))
    
    # Special Fed Fetch (Focus on April)
    fed_probs = crawl_event(FED_EVENT, "fed")
    entry["fed_april_prob"] = fed_probs.get("fed_april_meeting_prob", 0.0)

    print(f"✅ Success: Retrieved {len(entry)} data points.")
    return entry

# --- Save & Rolling Log ---
new_row = get_pro_investment_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # This logic handles new columns if brackets are added/removed in the future
    df_combined = pd.concat([df_old, df_new], sort=False).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Strategic File Updated: {file_name}")
