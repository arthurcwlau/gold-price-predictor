import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
import numpy as np
import os

def generate_unified_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Aesthetics - Clean & Professional Dark Theme
    sns.set_theme(style="darkgrid")
    plt.rcParams['figure.facecolor'] = '#121212'
    plt.rcParams['axes.facecolor'] = '#121212'
    plt.rcParams['grid.color'] = '#222222'

    # 2. Load and Clean Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()
    # Fill gaps to ensure smooth lines
    df = df.replace(0, np.nan).ffill().bfill()

    # 3. Resample and Define Horizons
    df_h = df.resample('h').mean().ffill().copy()
    horizons = [2, 6, 12, 24, 48]
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF', 24: '#FF00FF', 48: '#BF00FF'}

    # 4. ONE SINGLE GRAPH PLOTTING
    fig, ax = plt.subplots(figsize=(16, 10))
    fig.patch.set_facecolor('#121212')

    # --- THE NORMALIZATION KEY ---
    # Since predictions are ~$2700 and price is ~$2400, we anchor them together 
    # at the start of the data so we can see the relative "lead/lag" moves.
    offset = df_h['fair_value'].iloc[0] - df_h['gold_price'].iloc[0]

    # Plot Sentiment Leads (Thin lines)
    for h in horizons:
        # Shift the sentiment and subtract the offset to overlap with the gold line
        normalized_lead = df_h['fair_value'].shift(h) - offset
        ax.plot(df_h.index, normalized_lead, label=f'{h}h Lead Sentiment', 
                color=colors[h], lw=1.2, alpha=0.7)

    # Plot Actual Gold Spot (Thick Yellow Line)
    ax.plot(df_h.index, df_h['gold_price'], label='ACTUAL GOLD SPOT', 
            color='#FFD700', lw=4, zorder=10)

    # 5. Final Formatting
    ax.set_title("Unified Tactical Backtest: One-Graph Sentiment Lead Analysis", 
                 color='white', fontsize=18, pad=25, fontweight='bold')
    
    # Zoom in strictly on the Price Volatility
    ax.set_ylim(df_h['gold_price'].min() - 25, df_h['gold_price'].max() + 25)

    # Legend & Axis Styling
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.18), ncol=3, 
               frameon=True, facecolor='#121212', edgecolor='white', fontsize=11)
    
    ax.tick_params(colors='white', which='both')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    
    # Shade Market Closures (Good Friday / Weekends)
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
    print("🏁 SUCCESS: Unified one-graph chart generated.")

if __name__ == "__main__":
    generate_unified_chart()
