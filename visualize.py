import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
import numpy as np
import os

def generate_normalized_overlay(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Setup Aesthetics
    sns.set_theme(style="darkgrid")
    plt.rcParams['figure.facecolor'] = '#121212'
    plt.rcParams['axes.facecolor'] = '#1e1e1e'

    # 2. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()
    df = df.replace(0, np.nan).ffill().bfill()

    # 3. Calculate Fair Value (Expected Value)
    # Using your main.py logic for consistency
    tier_midpoints = {
        "gold_under_3_800": 3600, "gold_3_800_4_200": 4000, "gold_4_200_4_600": 4400,
        "gold_4_600_5_000": 4800, "gold_5_000_5_400": 5200, "gold_5_400_5_800": 5600,
        "gold_5_800_6_200": 6000, "gold_over_6_200": 6400
    }
    weighted_sum = 0
    total_prob = 0
    for key, midpoint in tier_midpoints.items():
        col = f"{key}_prob"
        if col in df.columns:
            weighted_sum += df[col].fillna(0) * midpoint
            total_prob += df[col].fillna(0)
    
    df['fair_value'] = (weighted_sum / total_prob).ffill()
    df_h = df.resample('h').mean().ffill().copy()

    # 4. NORMALIZATION: Align the scales
    # We calculate the average "Gap" (Basis) between prediction and actual price
    basis = (df_h['fair_value'] - df_h['gold_price']).mean()

    horizons = [2, 6, 12, 24, 48]
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF', 24: '#FF00FF', 48: '#BF00FF'}

    # 5. Plotting (Single Axis)
    fig, ax = plt.subplots(figsize=(15, 9))
    fig.patch.set_facecolor('#121212')

    # Plot Sentiment Leads (Normalized to the Price Scale)
    for h in horizons:
        # We take the forecast from H hours ago and subtract the Basis 
        # so it overlaps with the current price
        normalized_forecast = df_h['fair_value'].shift(h) - basis
        ax.plot(df_h.index, normalized_forecast, label=f'{h}h Sentiment Lead', 
                color=colors[h], lw=1.1, alpha=0.8)

    # Plot Actual Gold Price (Thick Gold Line)
    ax.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', 
            color='#FFD700', lw=4, zorder=10)

    # 6. Formatting
    ax.set_title("Combined Axis Backtest: Normalized Sentiment vs. Spot Price", color='white', fontsize=16, pad=20)
    ax.set_ylabel("Price (Normalized USD)", color='white', fontsize=12)
    
    # Zoom in on the current price action (+/- 30 USD)
    ax.set_ylim(df_h['gold_price'].min() - 30, df_h['gold_price'].max() + 30)

    # Legend and Grid
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.15), ncol=3, frameon=True, facecolor='#121212', edgecolor='white')
    ax.tick_params(colors='white')
    ax.grid(color='#333333', linestyle='--', alpha=0.5)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(color='white')

    plt.tight_layout()
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300, facecolor=fig.get_facecolor())
    print("🏁 Combined-axis normalized chart generated.")

if __name__ == "__main__":
    generate_normalized_overlay()
