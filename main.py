import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json

def get_pro_investment_data():
    print("--- 🏦 2026 Gold Investment Dashboard: Online ---")
    
    # EXACT SLUGS FROM YOUR CONFIRMED SNIPPETS
    GOLD_DAILY_SLUG = "xauusd-up-or-down-on-march-30-2026"
    FED_EVENT_SLUG = "fed-rate-cut-by-629"
    JUNE_CRASH_MARKET_SLUG = "gc-settle-below-3800-jun-2026"
    
    entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "gold_price": 0.0, "dxy_index": 0.0, "oil_wti": 0.0,
        "daily_up_prob": 0.0, "fed_april_cut_prob": 0.0,
        "june_crash_prob": 0.0, "market_spread": 0.0
    }

    # 1. Macro Data: Gold, Dollar, and Oil (WTI)
    try:
        # Weekend-safe logic (looks back 7 days to find Friday's close)
        gold_h = yf.Ticker("GC=F").history(period="7d")
        dxy_h = yf.Ticker("DX-Y.NYB").history(period="7d")
        oil_h = yf.Ticker("CL=F").history(period="7d")
        
        entry["gold_price"] = round(gold_h['Close'].iloc[-1], 2)
        entry["dxy_index"] = round(dxy_h['Close'].iloc[-1], 2)
        entry["oil_wti"] = round(oil_h['Close'].iloc[-1], 2)
        print(f"📈 Macro: Gold ${entry['gold_price']} | Oil ${entry['oil_wti']} | DXY {entry['dxy_index']}")
    except: print("!! Macro Data Fetch Failed")

    # 2. Polymarket "Hunter" Logic
    def fetch_poly_data(slug, is_event=True):
        try:
            endpoint = "events" if is_event else "markets"
            url = f"https://gamma-api.polymarket.com/{endpoint}?slug={slug}"
            resp = requests.get(url).json()
            
            # If searching events, we get a list of events. If markets, a list of markets.
            data = resp[0] if isinstance(resp, list) and len(resp) > 0 else resp
            
            # If it's an event, we need to pick a market inside it
            if is_event:
                markets = data.get('markets', [])
                market = markets[0]
                # Special logic for Fed: Find the 'April' option
                if "fed" in slug:
                    for m in markets:
                        if "APRIL" in m.get('groupItemTitle', '').upper():
                            market = m; break
            else:
                market = data

            # Decode price into probability (0.14 -> 14.0)
            prices = market.get('outcomePrices')
            if isinstance(prices, str): prices = json.loads(prices)
            prob = round(float(prices[0]) * 100, 2)
            return prob, market.get('clobTokenIds', [None])[0]
        except: return 0.0, None

    # Execute Strategic Fetches
    entry["daily_up_prob"], daily_token = fetch_poly_data(GOLD_DAILY_SLUG, is_event=True)
    entry["fed_april_cut_prob"], _ = fetch_poly_data(FED_EVENT_SLUG, is_event=True)
    # The June Crash is a specific Market inside a multi-outcome event
    entry["june_crash_prob"], _ = fetch_poly_data(JUNE_CRASH_MARKET_SLUG, is_event=False)

    # 3. Conviction Check (Spread)
    if daily_token:
        try:
            book = requests.get(f"https://clob.polymarket.com/book?token_id={daily_token}").json()
            if book.get('bids') and book.get('asks'):
                entry["market_spread"] = round(float(book['asks'][0]['price']) - float(book['bids'][0]['price']), 4)
            else: entry["market_spread"] = -1.0 # Closed/No liquidity
        except: entry["market_spread"] = -1.0

    print(f"🎯 Sentiment: Daily {entry['daily_up_prob']}% | June Crash {entry['june_crash_prob']}% | Fed {entry['fed_april_cut_prob']}%")
    return entry

# --- Save & Append Logic ---
new_row = get_pro_investment_data()
file_name = "gold_investment_pro.csv"
df_new = pd.DataFrame([new_row])

if os.path.exists(file_name):
    df_old = pd.read_csv(file_name)
    df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['date'], keep='last').tail(30)
else:
    df_combined = df_new

df_combined.to_csv(file_name, index=False)
print(f"✅ Strategic Data Saved for {new_row['date']}")
