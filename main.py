import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re
import time

def get_pro_institutional_data():
    print("--- 🧠 2026 Gold Intelligence: Institutional Pipeline Active ---")
    
    # STRATEGIC EVENT SLUGS
    SLUGS = {
        "gold": "gc-settle-jun-2026",
        "oil": "cl-hit-jun-2026",
        "fed": "fed-decision-in-june-825"
    }
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0
    }

    # 1. Macro Data (Weekend-Safe)
    try:
        gold_h = yf.Ticker("GC=F").history(period="7d")
        oil_h = yf.Ticker("CL=F").history(period="7d")
        dxy_h = yf.Ticker("DX-Y.NYB").history(period="7d")
        
        entry["gold_price"] = round(gold_h['Close'].iloc[-1], 2)
        entry["oil_wti"] = round(oil_h['Close'].iloc[-1], 2)
        entry["dxy_index"] = round(dxy_h['Close'].iloc[-1], 2)
        print(f"📈 Macro: Gold ${entry['gold_price']} | Oil ${entry['oil_wti']}")
    except Exception as e:
        print(f"!! Macro Data Error: {e}")

    # 2. Polymarket Data Fetchers
    def get_clob_metrics(token_id):
        try:
            if not token_id: return 0.0, 0.0
            time.sleep(0.2) # Avoid rate limiting
            r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}").json()
            bids = r.get('bids', [])
            asks = r.get('asks', [])
            
            spread = 0.0
            if bids and asks:
                spread = round(float(asks[0]['price']) - float(bids[0]['price']), 4)
            
            depth = sum([float(b['size']) for b in bids[:5]]) + sum([float(a['size']) for a in asks[:5]])
            return spread, round(depth, 2)
        except: return 0.0, 0.0

    def get_reliable_oi(market_id):
        try:
            # 2026 Data API specific fetch
            r = requests.get(f"https://data-api.polymarket.com/oi?market_id={market_id}").json()
            return round(float(r.get('openInterest', 0)), 2)
        except: return 0.0

    def process_institutional_curve(slug, prefix):
        print(f"🔍 Fetching {slug}...")
        try:
            r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            if not r: return
            markets = r[0]['markets']
            
            for m in markets:
                title = m.get('groupItemTitle') or m.get('question') or ""
                # Clean name for CSV
                clean_name = re.sub(r'[^a-z0-9]', '_', title.replace('$', '').replace('<', 'under_').lower()).strip('_')
                
                # A. Probability & Volume (Gamma)
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                entry[f"{prefix}_{clean_name}_prob"] = round(float(prices[0]) * 100, 2)
                entry[f"{prefix}_{clean_name}_vol"] = round(float(m.get('volume', 0)), 2)
                
                # B. Open Interest (Data API)
                # Using the specific Market ID for OI
                entry[f"{prefix}_{clean_name}_oi"] = get_reliable_oi(m['id'])
                
                # C. Spread & Depth (CLOB API)
                tokens = m.get('clobTokenIds')
                if isinstance(tokens, str): tokens = json.loads(tokens)
                if tokens:
                    s, d = get_clob_metrics(tokens[0])
                    entry[f"{prefix}_{clean_name}_spread"] = s
                    entry[f"{prefix}_{clean_name}_depth"] = d
        except Exception as e:
            print(f"!! Error processing {slug}: {e}")

    # Run the processors
    process_institutional_curve(SLUGS["gold"], "gold")
    process_institutional_curve(SLUGS["oil"], "oil")
    process_institutional_curve(SLUGS["fed"], "fed")

    return entry

# --- Execution & Save Logic ---
# This name must match the function definition above!
new_row_data = get_pro_institutional_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row_data])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_combined = pd.concat([df_old, df_new], sort=False).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"🏁 Strategic Data Saved for {new_row_data['date']}")
