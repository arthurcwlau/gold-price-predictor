import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
import re

def generate_multi_factor_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing. Please ensure the filename matches.")
        return

    # 1. Load and De-fragment Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()

    # 2. Calculate Base Sentiment (Polymarket Weighted Average)
    tier_midpoints = {
        "gold_3_800_prob": 3600.0,
        "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0,
        "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0,
        "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0,
        "gold_6_200_prob": 6400.0,
    }
    
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    if not active_tiers:
        print("⚠️ No Gold tiers found.")
        return

    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    df['base_pred'] = (weighted_sum / total_prob).replace([np.inf, -np.inf], np.nan).ffill()

    # 3. MULTI-FACTOR REFINEMENT (The Accuracy Engine)
    # We adjust the crowd sentiment using the Macro reality in your CSV
    macro_multiplier = 1.0

    # A. The Dollar Drag (Inverse relationship with USD ETF)
    if 'usd_etf' in df.columns:
        usd_returns = df['usd_etf'].pct_change().fillna(0)
        # If USD goes up, we lower our Gold expectation
        macro_multiplier -= (usd_returns * 0.5) 

    # B. The Fear Boost (Positive relationship with VIX)
    if 'vix_index' in df.columns:
        vix_returns = df['vix_index'].pct_change().fillna(0)
        # If Fear spikes, we raise our Gold expectation
        macro_multiplier += (vix_returns * 0.1)

    # C. The Yield Pressure (Treasury 10Y)
    if 'treasury_10y' in df.columns:
        yield_change = df['treasury_10y'].diff().fillna(0)
        # Higher yields are usually bad for gold
        macro_multiplier -= (yield_change * 0.02)

    # Calculate the Refined Composite Prediction
    df['refined_pred'] = (df['base_pred'] * macro_multiplier).ffill()

    # 4. BACKTEST ALIGNMENT (24-Hour Lead)
    df_h = df[['gold_price', 'refined_pred']].resample('h').mean().ffill()
    horizon = 24
    df_h['forecast_lead'] = df_h['refined_pred'].shift(horizon)
    df_h['error'] = df_h['gold_price'] - df_h['forecast_lead']

    # Performance Stats
    valid = df_h.dropna(subset=['forecast_lead', 'gold_price'])
    if not valid.empty:
        mae = valid['error'].abs().mean()
        # Directional Accuracy (Did we guess Up/Down correctly?)
        prev_p = valid['gold_price'].shift(horizon)
        hit_rate = ((valid['forecast_lead'] > prev_p) == (valid['gold_price'] > prev_p)).mean()
    else:
        mae, hit_rate = 0, 0

    # 5. Professional Dashboard Plotting
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True, 
                                   gridspec_kw={'height_ratios': [3, 1]})

    # Top Plot: Reality vs Multi-Factor Forecast
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Gold Price', color='#FFD700', lw=4, zorder=5)
    ax1.plot(df_h.index, df_h['forecast_lead'], label=f'Refined {horizon}h Forecast', 
             color='#00BFFF', lw=2, ls='--', alpha=0.8)

    ax1.set_title("Gold Multi-Factor Backtest: Macro + Sentiment Fusion", fontsize=18, pad=20)
    ax1.legend(loc='upper left', frameon=True, facecolor='black', edgecolor='white')
    ax1.grid(alpha=0.1)

    # Bottom Plot: The "Edge" (Error/Bias)
    ax2.fill_between(df_h.index, df_h['error'], 0, color='#00BFFF', alpha=0.2)
    ax2.axhline(0, color='white', lw=1, alpha=0.5)
    ax2.set_ylabel("Error (USD)")

    # Performance Metadata
    stats = (f"📈 ACCURACY DASHBOARD\n\n"
             f"Factor Fusion: Tiers + DXY + VIX\n"
             f"Horizon: {horizon} Hours\n"
             f"Directional Acc: {hit_rate:.1%}\n"
             f"Avg Error: ${mae:.2f}")
    
    plt.gcf().text(0.86, 0.5, stats, fontsize=11, color='white', verticalalignment='center',
                   bbox=dict(facecolor='#121212', alpha=0.9, edgecolor='#FFD700', boxstyle='round,pad=1'))

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.tight_layout()
    plt.subplots_adjust(right=0.85)
    
    plt.savefig("gold_multi_factor_backtest.png", dpi=300)
    print("🏁 Multi-factor backtest report generated.")

if __name__ == "__main__":
    generate_multi_factor_backtest()
