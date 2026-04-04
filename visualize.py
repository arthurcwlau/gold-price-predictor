import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
from pandas.tseries.holiday import USFederalHolidayCalendar

def generate_honest_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Clean Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')
    
    # FIX: Remove any '0' values that cause "Big Drops" and fill gaps
    df = df.replace(0, np.nan).ffill().copy()

    # 2. Calculate Market Fair Value
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    
    # Generate Prediction and ensure no gaps
    df['raw_prediction'] = (weighted_sum / total_prob).replace([np.inf, -np.inf], np.nan).ffill()

    # 3. Time-Shift for Backtesting (2h, 6h, 12h)
    horizons = [2, 6, 12]
    for h in horizons:
        df[f'forecast_{h}h'] = df['raw_prediction'].shift(h)

    # 4. Resample & Identify Market Hours (For accurate shading)
    df_h = df.resample('h').mean().ffill()
    
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=df_h.index.min(), end=df_h.index.max())
    is_weekend = (df_h.index.weekday >= 5) | \
                 ((df_h.index.weekday == 4) & (df_h.index.hour >= 17)) | \
                 ((df_h.index.weekday == 6) & (df_h.index.hour < 18))
    is_holiday = df_h.index.strftime('%Y-%m-%d').isin(['2026-04-03', '2026-04-04'])
    is_closed = is_weekend | is_holiday

    # 5. Plotting (Single Scale - No Twin Axis)
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(15, 9))

    # Actual Price (Bottom of the scale)
    ax.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', color='#FFD700', lw=4, zorder=10)
    
    # Predictions (Top of the scale)
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF'}
    for h in horizons:
        ax.plot(df_h.index, df_h[f'forecast_{h}h'], label=f'Predicted {h}h ago', 
                 color=colors[h], lw=2, ls='--', alpha=0.9)

    # 6. Formatting & Shading
    closed_indices = df_h.index[is_closed]
    if not closed_indices.empty:
        diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
        for _, group in pd.Series(closed_indices).groupby(diff.cumsum()):
            ax.axvspan(group.iloc[0], group.iloc[-1], color='#1a1a1a', alpha=1.0, zorder=1)

    # Title & Legend
    ax.set_title("Tactical Gold Backtest: Shared Absolute Scale", fontsize=16, pad=20)
    ax.set_ylabel("Price / Forecast Value (USD)", fontsize=12)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), frameon=True, facecolor='black')
    
    # Grid and Dates
    ax.grid(alpha=0.05)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    
    # Explanation Text
    plt.gcf().text(0.15, 0.02, "💡 Note: The vertical gap represents the 'Expectation Premium' between current Spot and the June 2026 target.", 
                   fontsize=10, color='gray', style='italic')

    plt.tight_layout()
    plt.subplots_adjust(right=0.85) # Make room for the legend on the right
    
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300)
    print("🏁 Honesty-scaled backtest generated.")

if __name__ == "__main__":
    generate_honest_backtest()
