import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
import numpy as np
import os

def generate_unified_chart(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. High-End "Clean" Aesthetics
    sns.set_theme(style="darkgrid")
    plt.rcParams['figure.facecolor'] = '#121212'
    plt.rcParams['axes.facecolor'] = '#121212'
    plt.rcParams['grid.color'] = '#222222'

    # 2. Load Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()
    df = df.replace(0, np.nan).ffill().bfill()

    # 3. Calculate Polymarket Fair Value
    tier_midpoints = {
        "gold_under_3_800": 3600, "gold_3_800_4_200": 4000, "gold_4_200_4_600": 4400,
        "gold_4_600_5_000": 4800, "gold_5_000_5_400": 5200, "gold_5_400_5_800": 5600,
        "gold_5_800_6_200": 6000, "gold_over_6_200": 6400
    }
    active_cols = [f"{k}_prob" for k in tier_midpoints.keys() if f"{k}_prob" in df.columns]
    weighted_sum = sum(df[c].fillna(0) * tier_midpoints[c.replace('_prob','')] for c in active_cols)
    total_prob = df[active_cols].sum(axis=1)
    df['fair_value'] = (weighted_sum / total_prob).ffill()

    # 4. Resample & Define Horizons
    df_h = df.resample('h').mean().ffill().copy()
    horizons = [2, 6, 12, 24, 48]
    # Neon palette for leads, Gold for reality
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF', 24: '#FF00FF', 48: '#BF00FF'}

    # 5. ONE SINGLE GRAPH LOGIC
    fig, ax = plt.subplots(figsize=(16, 10))
    fig.patch.set_facecolor('#121212')

    # --- THE NORMALIZATION KEY ---
    # We force the Sentiment line to start at the same price as Gold
    # This allows us to see the 'Move' relative to each other on ONE axis.
    offset = df_h['fair_value'].iloc[0] - df_h['gold_price'].iloc[0]

    # Plot Sentiment Leads (Thin lines)
    for h in horizons:
        # Subtract the offset so the $2700 prediction sits on top of the $2400 price
        normalized_sentiment = df_h['fair_value'].shift(h) - offset
        ax.plot(df_h.index, normalized_sentiment, label=f'{h}h Lead Sentiment', 
                color=colors[h], lw=1.2, alpha=0.7)

    # Plot Actual Gold Spot (Thick Yellow Line)
    ax.plot(df_h.index, df_h['gold_price'], label='ACTUAL GOLD SPOT', 
            color='#FFD700', lw=4, zorder=10)

    # 6. Final Formatting
    ax.set_title("Unified Tactical Backtest: One Graph Trend Analysis", 
                 color='white', fontsize=18, pad=25, fontweight='bold')
    
    # Standardize the Y-axis to show the Gold Price range clearly
    ax.set_ylim(df_h['gold_price'].min() - 20, df_h['gold_price'].max() + 20)

    # Legend & Grid
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.18), ncol=3, 
               frameon=True, facecolor='#121212', edgecolor='white', fontsize=11)
    
    ax.tick_params(colors='white', which='both')
    ax.yaxis.label.set_color('white')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    
    # Shade Weekends/Market Closures
    is_weekend = (df_h.index.weekday >= 5) | \
                 ((df_h.index.weekday == 4) & (df_h.index.hour >= 17)) | \
                 ((df_h.index.weekday == 6) & (df_h.index.hour < 18))
    is_holiday = df_h.index.strftime('%Y-%m-%d').isin(['2026-04-03', '2026-04-04'])
    closed = df_h.index[is_weekend | is_holiday]
    if not closed.empty:
        diff = pd.Series(closed).diff() > pd.Timedelta(hours=1)
        for _, group in pd.Series(closed).groupby(diff.cumsum()):
            ax.axvspan(group.iloc[0], group.iloc[-1], color='#1a1a1a', alpha=1.0, zorder=1)

    plt.tight_layout()
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300, facecolor='#121212')
    print("🏁 SUCCESS: Single-axis unified chart generated.")

if __name__ == "__main__":
    generate_unified_chart()
