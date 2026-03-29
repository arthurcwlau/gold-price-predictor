import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_strategic_gold_oil_data():
    print("--- 🏦 2026 Strategic Dashboard: Full-Curve Active ---")
    
    # STRATEGIC EVENT SLUGS
    GOLD_HIT_SLUG = "gc-hit-jun-2026"
    OIL_HIT_SLUG = "cl-hit-jun-2026"
    FED_SLUG = "fed-rate-cut-by-629"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0
    }

    # 1. Macro Data (The Physical Realities)
    try:
        # Weekend-safe logic (Finds last close)
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

    def process_hit_curve(slug, prefix, entry_dict):
        markets = fetch_event_markets(slug)
        for m in markets:
            title = m.get('groupItemTitle') or m.get('question') or ""
            # Extract numbers (e.g., "$5,000" -> "5000")
            clean_price = "".join(filter(str.isdigit, title))
            if clean_price:
                key = f"{prefix}_hit_{clean_price}_prob"
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                entry_dict[key] = round(float(prices[0]) * 100, 2)

    # A. Retrieve the Gold Curve ($5k - $10k)
    process_hit_curve(GOLD_HIT_SLUG, "gold", entry)

    # B. Retrieve the Oil Curve ($110 - $200)
    process_hit_curve(OIL_HIT_SLUG, "oil", entry)

    # C. Fed Pivot Sentiment (April Meeting focus)
    fed_markets = fetch_event_markets(FED_SLUG)
    for m in fed_markets:
        if "APRIL" in m.get('groupItemTitle', '').upper():
            p = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            entry["fed_april_pivot_prob"] = round(float(p[0]) * 100, 2)

    return entry

# --- Save & Tidy Logic ---
new_row = get_strategic_gold_oil_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # We force the CSV to only use columns defined in the new data to stay tidy
    df_combined = pd.concat([df_old, df_new], sort=False)
    # We only keep the columns that exist in our newest strategic row
    df_combined = df_combined[df_new.columns]
    df_combined = df_combined.drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Strategic Data Saved. Captured {len(df_new.columns)} alpha-points.")
