import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_gold_strategy_data():
    print("--- 🏦 2026 Gold Breakout Dashboard: Online ---")
    
    # TARGET SLUGS
    GOLD_HIT_SLUG = "gc-hit-jun-2026"  # The new 'Will Gold hit $X,XXX' event
    FED_SLUG = "fed-rate-cut-by-629"
    OIL_SHOCK_SLUG = "cl-hit-jun-2026"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0
    }

    # 1. Macro Data (The Hard Assets)
    try:
        gold = yf.Ticker("GC=F").history(period="7d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1]
        oil = yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1]
        entry.update({"gold_price": round(gold, 2), "dxy_index": round(dxy, 2), "oil_wti": round(oil, 2)})
        print(f"📈 Macro: Gold ${entry['gold_price']} | Oil ${entry['oil_wti']}")
    except: print("!! Macro Fetch Failed")

    # 2. Polymarket Data Fetcher
    def fetch_event_markets(slug):
        try:
            r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            return r[0]['markets'] if r else []
        except: return []

    # A. Gold Hit Curve (Retrieving ALL probabilities)
    gold_hit_markets = fetch_event_markets(GOLD_HIT_SLUG)
    for m in gold_hit_markets:
        title = m.get('groupItemTitle') or m.get('question') or ""
        # Clean title to get the price (e.g., "Will Gold hit $5,000" -> "gold_hit_5000_prob")
        clean_price = "".join(filter(str.isdigit, title))
        if clean_price:
            key = f"gold_hit_{clean_price}_prob"
            prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            entry[key] = round(float(prices[0]) * 100, 2)

    # B. Fed Pivot Sentiment
    fed_markets = fetch_event_markets(FED_SLUG)
    for m in fed_markets:
        if "APRIL" in m.get('groupItemTitle', '').upper():
            p = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            entry["fed_april_pivot_prob"] = round(float(p[0]) * 100, 2)

    # C. Oil Shock Sentiment (Predictive for Gold)
    oil_markets = fetch_event_markets(OIL_SHOCK_SLUG)
    # Focus only on the 'High Alpha' targets for a tidy CSV
    for m in oil_markets:
        title = m.get('groupItemTitle', '')
        if any(target in title for target in ["$150", "$200"]):
            clean_name = f"oil_shock_{title.replace('$', '').strip()}_prob"
            p = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
            entry[clean_name] = round(float(p[0]) * 100, 2)

    return entry

# --- Save Routine (Cleans old empty entries) ---
new_row = get_gold_strategy_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # We combine and align columns, ensuring no 'phantom' columns persist
    df_combined = pd.concat([df_old, df_new], sort=False)
    # Keep it tidy: only columns that are in the NEWest row
    df_combined = df_combined[df_new.columns]
    df_combined = df_combined.drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Dashboard Updated: {len(df_new.columns)} columns recorded.")
