import pandas as pd
import yfinance as yf
import requests, os, re, json
from datetime import datetime

def get_live_market_data():
    print("--- 🛰️ 2026 Pulse: High-Efficiency Pro Mode ---")
    session = requests.Session()
    
    SLUGS = {
        "gold": "gc-settle-jun-2026", 
        "oil": "cl-hit-jun-2026", 
        "fed": "fed-decision-in-june-825",
        "recession": "us-recession-by-end-of-2026"
    }
    
    tickers = {
        "gold_price": "GC=F", "oil_wti": "CL=F", "dxy_index": "DX-Y.NYB", 
        "vix_index": "^VIX", "gold_vix": "^GVZ", "real_yield_proxy": "TIP",
        "silver_price": "SI=F", "gold_miners": "GDX", "copper_price": "HG=F"
    }

    entry = {"date": datetime.now().strftime("%Y-%m-%d %H:%M")}

    # 1. BATCH YFINANCE (Download everything in one call)
    try:
        all_tickers = list(tickers.values()) + ["GLD"]
        raw = yf.download(all_tickers, period="1d", progress=False)
        
        for key, sym in tickers.items():
            if not raw['Close'][sym].empty:
                entry[key] = round(raw['Close'][sym].iloc[-1], 2)
        
        # Capture specific volumes
        entry["gld_etf_vol"] = int(raw['Volume']['GLD'].iloc[-1])
        entry["dxy_vol"] = int(raw['Volume']['DX-Y.NYB'].iloc[-1])
    except Exception as e:
        print(f"YFinance Error: {e}")

    # 2. DEEP PREDICTION PULSE (Polymarket Session)
    for p, slug in SLUGS.items():
        try:
            resp = session.get(f"https://gamma-api.polymarket.com/events?slug={slug}", timeout=10)
            data = resp.json()
            if not data or not data[0].get('markets'): continue
            
            for m in data[0]['markets']:
                # KEEPING YOUR ORIGINAL CLEANING LOGIC
                raw_title = (m.get('groupItemTitle') or m.get('question')).lower()
                clean = re.sub(r'[^a-z0-9]', '_', raw_title).strip('_')
                clean = re.sub(r'_+', '_', clean.replace('$', '').replace('<', 'under_').replace('>', 'over_'))
                
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                if prices: entry[f"{p}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
                
                entry[f"{p}_{clean}_vol"] = round(float(m.get('volume', 0)), 2)
                entry[f"{p}_{clean}_oi"] = round(float(m.get('openInterest', 0)), 2)
                entry[f"{p}_{clean}_liq"] = round(float(m.get('liquidity', 0)), 2)
                
                # Book Depth & Spread
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    book = session.get(f"https://clob.polymarket.com/book?token_id={tid}").json()
                    if book.get('bids') and book.get('asks'):
                        entry[f"{p}_{clean}_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
                        entry[f"{p}_{clean}_depth"] = round(sum([float(x['size']) for x in book['bids'][:5]]), 2)
                    
                    last_p = session.get(f"https://clob.polymarket.com/price?token_id={tid}").json()
                    if last_p.get('price'):
                        entry[f"{p}_{clean}_last_price"] = round(float(last_p['price']) * 100, 2)
        except: continue
        
    return entry

# --- PERSISTENCE & PREDICTOR ENGINE ---
file_name = "gold_investment_pro.csv"
live_row = get_live_market_data()
df_new = pd.DataFrame([live_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name, low_memory=False)
    df_final = pd.concat([df_old, df_new], ignore_index=True, sort=False)
else:
    df_final = df_new

# UNALTERED PREDICTOR LOGIC
df_final['date'] = pd.to_datetime(df_final['date'])
df_final = df_final.drop_duplicates(subset=['date']).sort_values('date')

prob_cols = [c for c in df_final.columns if c.endswith('_prob')]
for col in prob_cols:
    base = col.replace('_prob', '')
    # Velocity calculation
    df_final[f"{base}_velocity"] = df_final[col].diff().round(2)
    df_final[f"{base}_velocity_ma6"] = df_final[f"{base}_velocity"].rolling(window=6, min_periods=1).mean().round(2)
    # Signal generation
    df_final[f"{base}_signal"] = (df_final[f"{base}_velocity"] > df_final[f"{base}_velocity_ma6"]).astype(int)

df_final.to_csv(file_name, index=False)
print(f"🏁 Update Successful. Tracked {len(df_final.columns)} indicators.")
