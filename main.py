import pandas as pd
import yfinance as yf
import requests, os, re, json
from datetime import datetime

def get_live_market_data():
    # 1. Configuration & Batch Download
    tickers = {"gold": "GC=F", "dxy": "DX-Y.NYB", "vix": "^VIX", "silver": "SI=F", "copper": "HG=F"}
    raw = yf.download(list(tickers.values()), period="1d", progress=False)['Close'].iloc[-1]
    
    entry = {"date": datetime.now().strftime("%Y-%m-%d %H:%M")}
    entry.update({k: round(raw[v], 2) for k, v in tickers.items()})
    entry["au_cu_ratio"] = round(entry["gold"] / entry["copper"], 2)

    # 2. Polymarket Recession Sentiment (Macro Proxy)
    try:
        url = "https://gamma-api.polymarket.com/events?slug=us-recession-by-end-of-2026"
        m_data = requests.get(url, timeout=10).json()[0]['markets']
        prices = json.loads(m_data[0]['outcomePrices'])
        entry["recession_prob"] = round(float(prices[0]) * 100, 2)
    except: entry["recession_prob"] = None
    return entry

# --- Persistence & Divergence Engine ---
file_name = "gold_investment_pro.csv"
new_row = get_live_market_data()
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df = pd.read_csv(file_name)
    # LEGACY PATCH: Automatically rename old columns to new short format
    rename_map = {"gold_price": "gold", "dxy_index": "dxy", "vix_index": "vix", "copper_price": "copper"}
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df = pd.concat([df, df_new], ignore_index=True)
else:
    df = df_new

# 3. Alpha Divergence Logic (Requires 10+ days of data)
if len(df) > 10:
    def z_score(s): return (s - s.rolling(20).mean()) / (s.rolling(20).std() + 1e-9)
    df['z_gold'] = z_score(df['gold'])
    df['z_fear'] = z_score(df['recession_prob'].ffill())
    df['divergence'] = (df['z_gold'] - df['z_fear']).round(2)
    # Signal: 1 = Underpriced Gold, -1 = Overpriced Gold
    df['signal'] = 0
    df.loc[df['divergence'] > 1.5, 'signal'] = -1
    df.loc[df['divergence'] < -1.5, 'signal'] = 1

# Deduplicate and Save
df = df.drop_duplicates(subset=['date']).sort_values('date')
df.to_csv(file_name, index=False)
print(f"✅ Success. Divergence: {df['divergence'].iloc[-1] if 'divergence' in df else 'Calculating...'}")
