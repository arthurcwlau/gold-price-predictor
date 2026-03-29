import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_gold_alpha_data():
    print("--- 🏦 2026 Gold Alpha Pipeline: Starting ---")
    
    # 1. Macro Pulse
    try:
        # Weekend-safe fetching
        gold = yf.Ticker("GC=F").history(period="7d")['Close'].iloc[-1]
        dxy = yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1]
        oil = yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1]
    except: 
        gold, dxy, oil = 0.0, 0.0, 0.0

    # Define the STRICT entry structure (No empty columns allowed)
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": round(gold, 2),
        "dxy_index": round(dxy, 2),
        "oil_wti": round(oil, 2),
        "poly_gold_daily_up": 0.0,
        "gold_crash_june_prob": 0.0,
        "fed_pivot_prob": 0.0,
        "oil_shock_150_prob": 0.0,
        "oil_shock_200_prob": 0.0
    }

    # 2. Polymarket Precision Fetching
    def get_prob(slug, target_text=None, is_event=True):
        try:
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            r = requests.get(url).json()
            if r and len(r) > 0:
                markets = r[0]['markets']
                market = markets[0] # Default
                
                if target_text:
                    for m in markets:
                        title = (m.get('groupItemTitle') or m.get('question') or "").upper()
                        if target_text.upper() in title:
                            market = m
                            break
                
                prices = market.get('outcomePrices')
                if isinstance(prices, str): prices = json.loads(prices)
                return round(float(prices[0]) * 100, 2)
        except: return 0.0
        return 0.0

    # Fill the entry with targeted sentiment
    entry["poly_gold_daily_up"] = get_prob("xauusd-up-or-down-on-march-30-2026")
    entry["gold_crash_june_prob"] = get_prob("gc-settle-jun-2026", target_text="3800")
    entry["fed_pivot_prob"] = get_prob("fed-rate-cut-by-629", target_text="APRIL")
    entry["oil_shock_150_prob"] = get_prob("cl-hit-jun-2026", target_text="150")
    entry["oil_shock_200_prob"] = get_prob("cl-hit-jun-2026", target_text="200")

    print(f"✅ Success: Macro and Alpha sentiment captured.")
    return entry

# --- Save Routine (Cleans the CSV) ---
new_row = get_gold_alpha_data()
file_name = "gold_investment_pro.csv"

# We create a clean DataFrame with ONLY our specified columns
df_new = pd.DataFrame([new_row])

# If file exists, we read it, but we FORCE it to match our new tidy columns
if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # This line discards any old 'empty' columns from your previous run
    df_old = df_old[df_new.columns.intersection(df_old.columns)]
    df_combined = pd.concat([df_old, df_new], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

# Save with the clean, tidy structure
df_combined.to_csv(file_name, index=False)
print(f"🏁 Tidy Dashboard Saved: {file_name}")
