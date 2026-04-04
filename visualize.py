import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
from pandas.tseries.holiday import USFederalHolidayCalendar

def generate_tactical_dual_axis(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()

    # 2. Calculate Market Fair Value (Polymarket Sentiment)
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    
    # Calculate weighted average with safety check for 'drops'
    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    
    # Only calculate where total probability exists (prevents drops to zero)
    df['raw_prediction'] = np.where(total_prob > 0.1, (weighted_sum / total_prob), np.nan)
    df['raw_prediction'] = df['raw_prediction'].ffill().interpolate()

    # 3. Time-Shift for Backtesting (Aligning previous guesses with 'Now')
    horizons = [2, 6, 12]
    for h in horizons:
        df[f'forecast_{h}h'] = df['raw_prediction'].shift(h)

    # 4. Market Hours Logic (Shading)
    df_h = df.resample('h').mean().ffill()
    cal = USFederalHolidayCalendar()
    is_weekend = (df_h.index.weekday >= 5) | \
                 ((df_h.index.weekday == 4) & (df_h.index.hour >= 17)) | \
                 ((df_h.index.weekday == 6) & (df_h.index.hour < 18))
    is_holiday = df_h.index.strftime('%Y-%m-%d').isin(['2026-04-03', '2026-04-04'])
    is_closed = is_weekend | is_holiday

    # 5. Plotting (Aesthetic Overhaul)
    plt.style.use('dark_background')
    fig, ax1 = plt.subplots(figsize=(15, 8))
    
    # Dual Axis: ax1 is Price (Left), ax2 is Forecast (Right)
    ax2 = ax1.twinx()

    # --- LEFT AXIS: ACTUAL PRICE ---
    price_color = '#FFD700' # Gold
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', color=price_color, lw=4, zorder=5)
    ax1.set_ylabel("Actual Spot Price (USD)", color=price_color, fontsize=12, fontweight='bold')
    ax1.tick_params(axis='y', labelcolor=price_color)

    # --- RIGHT AXIS: PREDICTIONS ---
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF'} # Neon Green, Orange, Blue
    for h in horizons:
        ax2.plot(df_h.index, df_h[f'forecast_{h}h'], label=f'Predicted {h}h ago', 
                 color=colors[h], lw=2, ls='--', alpha=0.8)
    
    ax2.set_ylabel("Polymarket June-2026 Forecast (USD)", color='#00BFFF', fontsize=12, fontweight='bold')
    ax2.tick_params(axis='y', labelcolor='#00BFFF')

    # --- SHADING & STYLING ---
    closed_indices = df_h.index[is_closed]
    if not closed_indices.empty:
        diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
        for _, group in pd.Series(closed_indices).groupby(diff.cumsum()):
            ax1.axvspan(group.iloc[0], group.iloc[-1], color='#222222', alpha=0.7, zorder=1)

    ax1.set_title("Tactical Gold Backtest: Price (Left) vs. Sentiment Forecasts (Right)", fontsize=16, pad=20)
    
    # Combined Legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', ncol=2, frameon=True, facecolor='black')

    ax1.grid(alpha=0.1)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    
    # Dashboard Info
    plt.gcf().text(0.15, 0.02, "💡 Note: Left Axis tracks current Spot. Right Axis tracks the June 2026 Polymarket target.", 
                   fontsize=10, color='gray', style='italic')

    plt.tight_layout()
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300)
    print("🏁 Aesthetic dual-axis backtest generated.")

if __name__ == "__main__":
    generate_tactical_dual_axis()
