import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_pro_investment_data():
    print("--- 🏦 2026 Strategic Investment Dashboard: Online ---")
    
    # EXACT SLUGS FROM YOUR CONFIRMED SNIPPETS
    GOLD_DAILY_SLUG = "xauusd-up-or-down-on-march-30-2026"
    FED_EVENT_SLUG = "fed-rate-cut-by-629"
    JUNE_GOLD_CRASH_SLUG = "gc-settle-below-3800-jun-2026"
    OIL_200_EVENT_SLUG = "cl-hit-jun-2026"
    OIL_200_MARKET_SLUG = "will-crude-oil-cl-hit-high-200-by-end-of-june-677"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0,
        "daily_up_prob": 0.0, "fed_april_cut_prob": 0.0,
        "june_gold_crash_prob": 0.0, "oil_200_june_prob": 0.0,
        "market_spread": 0.0
    }

    # 1. Macro Data: Gold, Dollar, and Oil (WTI)
    try:
        # Weekend-safe logic (looks back 7 days for Friday's close)
        gold_h = yf.Ticker("GC=F").history(period="7d")
        dxy_h = yf.Ticker("DX-Y.NYB").history(period="7d")
        oil_h = yf.Ticker("CL=F").history(period="7d")
        
        entry["gold_price"] = round(gold_h['Close'].iloc[-1], 2)
        entry["dxy_index"] = round(dxy_h['Close'].iloc[-1], 2)
        entry["oil_wti"] = round(oil_h['Close'].iloc[-1], 2)
        print(f"📈 Macro: Gold ${entry['gold_price']} | Oil ${entry['oil_wti']} | DXY {entry['dxy_index']}")
    except: print("!! Macro Data Fetch Failed")

    # 2. Polymarket Logic (The "Hunter" Function)
    def fetch_poly_data(slug, is_event=True):
        try:
            endpoint = "events" if is_event else "markets"
            url = f"https://gamma-api.polymarket.com/{endpoint}?slug={slug}"
            resp = requests.get(url).json()
            data = resp[0] if isinstance(resp, list) and len(resp) > 0 else resp
            
            if is_event:
                markets = data.get('markets', [])
                market = markets[0]
                # Filter for April in Fed list
                if "fed" in slug:
                    for m in markets:
                        if "APRIL" in m.get('groupItemTitle', '').upper():
                            market = m; break
                # Filter for $200 in Oil list
                if "cl-hit" in slug:
                    for m in markets:
                        if "$200" in m.get('groupItemTitle', ''):
                            market = m; break
            else:
                market = data

            prices = market.get('outcomePrices')
            if isinstance(prices, str): prices = json.loads(prices)
            prob = round(float(prices[0]) * 100, 2)
            return prob, market.get('clobTokenIds', [None])[0]
        except: return 0.0, None

    # Execute Fetches
    entry["daily_up_prob"], daily_token = fetch_poly_data(GOLD_DAILY_SLUG, is_event=True)
    entry["fed_april_cut_prob"], _ = fetch_poly_data(FED_EVENT_SLUG, is_event=True)
    entry["june_gold_crash_prob"], _ = fetch_poly_data(JUNE_GOLD_CRASH_SLUG, is_event=False)
    
    # Fetch Oil $200 (Trying Event slug first, then Market slug fallback)
    entry["oil_200_june_prob"], _ = fetch_poly_data(OIL_200_EVENT_SLUG, is_event=True)
    if entry["oil_200_june_prob"] == 0.0:
        entry["oil_200_june_prob"], _ = fetch_poly_data(OIL_200_MARKET_SLUG, is_event=False)

    # 3. Spread Logic
    if daily_token:
        try:
            book = requests.get(f"https://clob.polymarket.com/book?token_id={daily_token}").json()
            if book.get('bids') and book.get('asks'):
                entry["market_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
            else: entry["market_spread"] = -1.0
        except: entry["market_spread"] = -1.0

    print(f"🎯 Sentiment: Daily {entry['daily_up_prob']}% | Gold Crash {entry['june_gold_crash_prob']}% | Oil $200 {entry['oil_200_june_prob']}%")
    return entry

# --- Save & Append Logic ---
new_row = get_pro_investment_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    # Merges and keeps the rolling 30-day window
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"✅ Strategic Data Saved: {file_name}")
