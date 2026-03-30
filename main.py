import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
import os
import json
import re
import time

def get_max_alpha_data():
    print("--- 🚀 2026 Gold Alpha: Max-Intelligence Build ---")
    
    SLUGS = {"gold": "gc-settle-jun-2026", "oil": "cl-hit-jun-2026", "fed": "fed-decision-in-june-825"}
    entry = {"date": datetime.now().strftime("%Y-%m-%d"), "gold_price": 0.0, "oil_wti": 0.0}

    # 1. Price History Helper (Velocity)
    def get_price_velocity(token_id):
        try:
            # Fetches the last 24 hours of price action
            r = requests.get(f"https://clob.polymarket.com/prices-history?token_id={token_id}&interval=6h").json()
            prices = r.get('history', [])
            if len(prices) > 1:
                change = float(prices[-1]['p']) - float(prices[0]['p'])
                return round(change * 100, 2) # Returns % change over 24h
        except: pass
        return 0.0

    # 2. Whale Concentration Helper (Whale Tracking)
    def get_whale_concentration(market_id):
        try:
            r = requests.get(f"https://data-api.polymarket.com/holders?market_id={market_id}").json()
            holders = r.get('holders', [])
            if holders:
                top_3_total = sum([float(h['weight']) for h in holders[:3]])
                return round(top_3_total * 100, 2) # % of market held by top 3
        except: pass
        return 0.0

    # 3. Main Processor
    def process_max_curve(slug, prefix):
        try:
            resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}").json()
            for m in resp[0]['markets']:
                raw_t = m.get('groupItemTitle') or m.get('question') or ""
                clean = re.sub(r'[^a-z0-9]', '_', raw_t.replace('$', '').replace('<', 'under_').lower()).strip('_')
                
                # Basic Metrics
                prices = json.loads(m['outcomePrices']) if isinstance(m['outcomePrices'], str) else m['outcomePrices']
                entry[f"{prefix}_{clean}_prob"] = round(float(prices[0]) * 100, 2)
                
                # DEEP ALPHA: Concentration & Velocity
                entry[f"{prefix}_{clean}_whale_pct"] = get_whale_concentration(m['id'])
                
                tokens = m.get('clobTokenIds')
                if tokens:
                    tid = tokens[0] if isinstance(tokens, list) else json.loads(tokens)[0]
                    entry[f"{prefix}_{clean}_velocity"] = get_price_velocity(tid)
        except: pass

    process_max_curve(SLUGS["gold"], "gold")
    process_max_curve(SLUGS["oil"], "oil")
    return entry

# --- Logic to save ---
# (Standard save logic here...)
