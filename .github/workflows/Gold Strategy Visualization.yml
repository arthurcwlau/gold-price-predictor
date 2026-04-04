import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
import re

def generate_backtest_report(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')

    # 2. Dynamic Tier Mapping (Calculates Fair Value from your 8 Gold Tiers)
    # This logic matches the column names in your (41).csv exactly.
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
        print("⚠️ No Gold tiers found. Skipping visualization.")
        return

    # Weighted Average Calculation (Market-Implied Fair Value)
    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    df['pred_now'] = (weighted_sum / total_prob).replace([np.inf, -np.inf], np.nan).ffill()

    # 3. Create Hourly DataFrame for Backtesting
    # We select specific columns first to prevent 'Series' object errors
    df_h = df[['gold_price', 'pred_now']].copy()
    df_h = df_h.resample('1H').mean().ffill()

    # 4. Generate Backtest Metrics (Comparing past predictions to current reality)
    lags = [12, 24, 48]
    metrics = {}
    
    for lag in lags:
        # What we predicted X hours ago for "Right Now"
        df_h[f'forecast_{lag}h'] = df_h['pred_now'].shift(lag)
        # Difference between reality and that past forecast
        df_h[f'error_{lag}h'] = df_h['gold_price'] - df_h[f'forecast_{lag}h']
        
        # Performance Stats
        valid = df_h.dropna(subset=[f'forecast_{lag}h', 'gold_price'])
        if not valid.empty:
            mae = valid[f'error_{lag}h'].abs().mean()
            # Directional Hit Rate (Did it guess correctly if price would move Up or Down?)
            prev_price = valid['gold_price'].shift(lag)
            hit_rate = ((valid[f'forecast_{lag}h'] > prev_price) == (valid['gold_price'] > prev_price)).mean()
            metrics[lag] = {'mae': mae, 'hit_rate': hit_rate}

    # 5. Create the Visualization (Two-Panel Dashboard)
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True, 
                                   gridspec_kw={'height_ratios': [3, 1]})

    # Top Plot: Spot Price vs. Historical Predictions
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', color='#FFD700', lw=4, zorder=5)
    colors = {12: '#00BFFF', 24: '#FF69B4', 48: '#ADFF2F'}
    for lag in lags:
        ax1.plot(df_h.index, df_h[f'forecast_{lag}h'], label=f'Forecast ({lag}h ago)', 
                 color=colors[lag], alpha=0.6, ls='--')

    ax1.set_title("Gold Market Prediction Backtest: Spot vs. Market Consensus", fontsize=18, pad=20)
    ax1.legend(loc='upper left', frameon=True, facecolor='black', edgecolor='white')
    ax1.grid(alpha=0.1)

    # Bottom Plot: Prediction Error (The "Bias")
    for lag in lags:
        ax2.fill_between(df_h.index, df_h[f'error_{lag}h'], 0, color=colors[lag], alpha=0.15)
    ax2.axhline(0, color='white', lw=1, alpha=0.5)
    ax2.set_ylabel("Error (USD)", fontsize=12)
    ax2.set_title("Forecast Bias (Actual - Past Prediction)", fontsize=12)

    # Performance Overlay Dashboard
    stats_text = "📊 BACKTEST PERFORMANCE"
    for lag, m in metrics.items():
        stats_text += f"\n\nHorizon: {lag}h\n Direction Acc: {m['hit_rate']:.1%}\n Avg Bias: ${m['mae']:.2f}"
    
    plt.gcf().text(0.86, 0.5, stats_text, fontsize=10, color='white', verticalalignment='center',
                   bbox=dict(facecolor='#121212', alpha=0.9, edgecolor='#FFD700', boxstyle='round,pad=1'))

    # Final Formatting
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.tight_layout()
    plt.subplots_adjust(right=0.85)
    
    plt.savefig("gold_performance_backtest.png", dpi=300)
    print("🏁 Backtest report generated: gold_performance_backtest.png")

if __name__ == "__main__":
    generate_backtest_report()
