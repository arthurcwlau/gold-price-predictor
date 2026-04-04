import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
import numpy as np
import os

def generate_seaborn_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Setup Seaborn Aesthetics
    # 'darkgrid' with the 'mako' or 'viridis' palette looks very modern
    sns.set_theme(style="darkgrid")
    plt.rcParams['figure.facecolor'] = '#121212'
    plt.rcParams['axes.facecolor'] = '#1e1e1e'

    # 2. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()
    df = df.replace(0, np.nan).ffill().bfill()

    # 3. Calculate Fair Value
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_6_200_prob": 6400.0, # Simplified list
    }
    # Dynamic tier detection
    active_tiers = [c for c in df.columns if "_prob" in c]
    weighted_sum = sum(df[col].fillna(0) * 4500 for col in active_tiers) # Placeholder logic
    df['fair_value'] = (weighted_sum / 100).ffill() 

    # --- FIX FRAGMENTATION WARNING ---
    # We create a clean copy after adding columns
    df_h = df.resample('h').mean().ffill().copy()

    # 4. Create Backtest Horizons
    horizons = [2, 6, 12]
    for h in horizons:
        df_h[f'forecast_{h}h'] = df_h['fair_value'].shift(h)
    
    # Final copy to ensure the frame is consolidated
    df_h = df_h.copy()

    # 5. Plotting
    fig, (ax_top, ax_bot) = plt.subplots(2, 1, sharex=True, figsize=(15, 10), 
                                         gridspec_kw={'height_ratios': [1, 1]})
    fig.patch.set_facecolor('#121212')

    # Color Palette: Neon Green, Orange, Cyan
    colors = ["#39FF14", "#FF8C00", "#00BFFF"]

    # TOP: Predictions (Seaborn Lineplot)
    for i, h in enumerate(horizons):
        sns.lineplot(ax=ax_top, data=df_h, x=df_h.index, y=f'forecast_{h}h', 
                     label=f'{h}h Lead', color=colors[i], lw=1.5)

    # BOTTOM: Actual Price (Gold Line)
    sns.lineplot(ax=ax_bot, data=df_h, x=df_h.index, y='gold_price', 
                 label='Actual Gold Spot', color='#FFD700', lw=3)

    # 6. Formatting
    ax_top.set_title("Tactical Backtest: Pure Sentiment Precision (Seaborn)", color='white', fontsize=16)
    for ax in [ax_top, ax_bot]:
        ax.tick_params(colors='white')
        ax.yaxis.label.set_color('white')
        ax.grid(color='#333333', linestyle='--')
        # Zoom logic
        if ax == ax_top:
            ax.set_ylim(df_h['fair_value'].min() - 50, df_h['fair_value'].max() + 50)
        else:
            ax.set_ylim(df_h['gold_price'].min() - 20, df_h['gold_price'].max() + 20)

    ax_bot.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(color='white')
    
    plt.tight_layout()
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300, facecolor=fig.get_facecolor())
    print("🏁 Seaborn chart generated successfully.")

if __name__ == "__main__":
    generate_seaborn_backtest()
