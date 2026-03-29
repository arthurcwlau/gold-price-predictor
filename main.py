import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re
import time

def get_gold_alpha_dashboard():
    print("--- 🛡️ 2026 Gold Alpha Dashboard: Final Resilient Build ---")
    
    SLUGS = {
        "gold": "gc-settle-jun-2026",
        "oil": "cl-hit-jun-2026",
        "fed": "fed-decision-in-june-825"
    }
    
    entry = {"date": datetime.now().strftime("%Y-%m-%d"), "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0}

    # 1. Macro Pulse
    try:
        gh = yf.Ticker("GC=F").history(period="7d")
        entry.update({
            "gold_price": round(gh['Close'].iloc[-1], 2),
            "oil_wti": round(yf.Ticker("CL=F").history(period="7d")['Close'].iloc[-1], 2),
            "dxy_index": round(yf.Ticker("DX-Y.NYB").history(period="7d")['Close'].iloc[-1], 2)
        })
    except: print("!! Macro Data Offline")

    # 2. Polymarket Institutional Helpers
    def get_clob(token_id):
        try:
            if not token_id: return 0.0, 0.0
            time.sleep(0.1) # Protect API limits
            r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}").json()
            b, a = r.get('bids', []), r.get('asks', [])
            if not b or not a: return 0.0, 0.0
            spread = round(float(a[0]['price']) - float(b[0]['price']), 4)
            depth = sum([float(x['size']) for x in b[:5] + a[:5]])
            return spread, round(depth, 2)
        except: return 0.0, 0.0

    def get_oi_triple_check(m):
        # Fallback 1: Direct Gamma Metadata
        oi = m.get('openInterest')
        if oi and float(oi) > 0: return round(float(oi), 2)
        
        # Fallback 2: Data API via Market ID
        try:
            r = requests.get(f"https://data-api.polymarket.com/oi?market_id={m['id']}").json()
            if r.get('openInterest'): return round(float(r['openInterest']), 2)
        except: pass
        
        # Fallback 3: Data API via Condition ID
        try:
            cid = m.get('conditionId')
            if cid:
                r = requests.get(f"https://data-api.polymarket.com/oi?condition_id={cid}").json()
                if r.get('openInterest'): return round(float(r['openInterest']), 2)
        except: pass
        return 0.0

    # 3. Execution
    for prefix, slug in SLUGS.items():
        try:
            resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            if not resp: continue
            for m in resp[0]['markets']:
                # Naming Logic: "Gold <$3,800" -> "gold_under_3800"
                raw_t = m.get('groupItemTitle') or m.get('question') or ""
                clean = re.sub(r'[^a-z0-9]', '_', raw_t.replace('$', '').replace('<', 'under_').replace('>', 'over_').lower()).strip('_')
                clean = re.sub(r'_+', '_', clean) # Kill double underscores
                
                # Metrics
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                entry[f"{prefix}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
                entry[f"{prefix}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
                entry[f"{prefix}_{clean}_oi"] = get_oi_triple_check(m)
                
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    s, d = get_clob(tid)
                    entry[f"{prefix}_{clean}_spread"], entry[f"{prefix}_{clean}_depth"] = s, d
        except: print(f"!! Failed slug: {slug}")

    return entry

# --- Save & Alignment ---
final_row = get_gold_alpha_dashboard()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([final_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # Keep ONLY columns present in the newest script to prevent NaN ghosting
    df_old = df_old[df_old.columns.intersection(df_new.columns)]
    df_combined = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=['date'], keep='last')
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print("🏁 Final Clean Strategic Run Complete.")
