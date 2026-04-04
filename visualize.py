
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
import numpy as np
import os

def generate_combined_axis_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Aesthetics - Clean, High-Contrast Dark Theme
    sns.set_theme(style="darkgrid")
    plt.rcParams['figure.facecolor'] = '#121212'
    plt.rcParams['axes.facecolor'] = '#121212'
    plt.rcParams['grid.color'] = '#333333'

    # 2. Load Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()
    df = df.replace(0, np.nan).ffill().bfill()

    # 3. Calculate Fair Value (Sentiment)
    # Using your existing logic to stay consistent with main.py
    tier_midpoints = {
        "gold_under_3_800": 3600, "gold_3_800_4_200": 4000, "gold_4_200_4_600": 4400,
        "gold_4_600_5_000": 4800, "gold_5_000_5_400": 5200, "gold_5_400_5_800": 5600,
        "gold_5_800_6_200": 6000, "gold_over_6_200": 6400
    }
    active_cols = [f"{k}_prob" for k in tier_midpoints.keys() if f"{k}_prob" in df.columns]
    weighted_sum = sum(df[c].fillna(0) * tier_midpoints[c.replace('_prob','')] for c in active_cols)
    total_prob = df[active_cols].sum(axis=1)
    df['fair_value'] = (weighted_sum / total_prob).ffill()

    # 4. Create Horizons (2h to 48h)
    df_h = df.resample('h').mean().ffill().copy()
    horizons = [2, 6, 12, 24, 48]
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF', 24: '#FF00FF', 48: '#BF00FF'}

    # 5. SINGLE AXIS PLOTTING
    # Note: No (ax_top, ax_bot) here. Just one ax.
    fig, ax = plt.subplots(figsize=(16, 10))
    fig.patch.set_facecolor('#121212')

    # --- NORMALIZATION LOGIC ---
    # We find the 'Basis' (the gap) at the very start of the visible data 
    # to anchor the sentiment lines to the price.
    initial_price = df_h['gold_price'].iloc[0]
    initial_sentiment = df_h['fair_value'].iloc[0]
    basis_offset = initial_sentiment - initial_price

    # Plot Sentiment Leads (Thin, Solid)
    for h in horizons:
        # We shift the sentiment and subtract the offset so it sits on the gold line
        normalized_lead = df_h['fair_value'].shift(h) - basis_offset
        ax.plot(df_h.index, normalized_lead, label=f'{h}h Sentiment Lead', 
                color=colors[h], lw=1.2, alpha=0.8)

    # Plot Actual Gold Price (The Ground Truth - Thick Gold Line)
    ax.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', 
            color='#FFD700', lw=4, zorder=10)

    # 6. Final Polish
    ax.set_title("Tactical Backtest: Sentiment Leads vs. Price (Combined Axis)", 
                 color='white', fontsize=18, pad=25)
    
    # Zoom in strictly on the Price Volatility (~$2,380 - $2,450 range)
    # This makes the leads/lags extremely visible.
    ax.set_ylim(df_h['gold_price'].min() - 25, df_h['gold_price'].max() + 25)

    # Legend and Axis Styling
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, -0.18), ncol=3, 
               frameon=True, facecolor='#121212', edgecolor='white', fontsize=11)
    
    ax.tick_params(colors='white', which='both', labelsize=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=0)

    # Shading for Market Closures (Good Friday/Weekends)
    is_weekend = (df_h.index.weekday >= 5) | \
                 ((df_h.index.weekday == 4) & (df_h.index.hour >= 17)) | \
                 ((df_h.index.weekday == 6) & (df_h.index.hour < 18))
    is_holiday = df_h.index.strftime('%Y-%m-%d').isin(['2026-04-03', '2026-04-04'])
    is_closed = is_weekend | is_holiday
    
    closed_indices = df_h.index[is_closed]
    if not closed_indices.empty:
        diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
        for _, group in pd.Series(closed_indices).groupby(diff.cumsum()):
            ax.axvspan(group.iloc[0], group.iloc[-1], color='#1a1a1a', alpha=1.0, zorder=1)

    plt.tight_layout()
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300, facecolor='#121212')
    print("🏁 Single-axis precision chart generated.")

if __name__ == "__main__":
    generate_combined_axis_backtest()
