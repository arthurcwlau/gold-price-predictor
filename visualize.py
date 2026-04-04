import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
from pandas.tseries.holiday import USFederalHolidayCalendar

def generate_pure_sentiment_split_axis(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Clean Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()
    # Remove zeros and gaps to keep lines smooth
    df = df.replace(0, np.nan).ffill().bfill()

    # 2. Calculate PURE Polymarket Fair Value (No Macro Noise)
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    df['fair_value'] = (weighted_sum / total_prob).ffill()

    # 3. Backtest Horizons (2h, 6h, 12h)
    horizons = [2, 6, 12]
    for h in horizons:
        df[f'forecast_{h}h'] = df['fair_value'].shift(h)

    # 4. Market Hours Logic (Shading Gaps)
    df_h = df.resample('h').mean().ffill()
    is_weekend = (df_h.index.weekday >= 5) | \
                 ((df_h.index.weekday == 4) & (df_h.index.hour >= 17)) | \
                 ((df_h.index.weekday == 6) & (df_h.index.hour < 18))
    is_holiday = df_h.index.strftime('%Y-%m-%d').isin(['2026-04-03', '2026-04-04'])
    is_closed = is_weekend | is_holiday

    # 5. Plotting (Split-Axis Precision)
    plt.style.use('dark_background')
    fig, (ax_top, ax_bot) = plt.subplots(2, 1, sharex=True, figsize=(15, 10), 
                                         gridspec_kw={'height_ratios': [1, 1]})
    fig.subplots_adjust(hspace=0.05)

    # --- TOP AXIS: PURE SENTIMENT ---
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF'}
    for h in horizons:
        ax_top.plot(df_h.index, df_h[f'forecast_{h}h'], label=f'{h}h Sentiment Lead', 
                    color=colors[h], lw=1.2, ls='-')

    # --- BOTTOM AXIS: ACTUAL PRICE ---
    ax_bot.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', 
                color='#FFD700', lw=3.5, zorder=10)

    # 6. Formatting & Shading
    for ax in [ax_top, ax_bot]:
        closed_indices = df_h.index[is_closed]
        if not closed_indices.empty:
            diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
            for _, group in pd.Series(closed_indices).groupby(diff.cumsum()):
                ax.axvspan(group.iloc[0], group.iloc[-1], color='#1a1a1a', alpha=1.0, zorder=1)
        ax.grid(alpha=0.1)

    # Precision Zooming
    ax_top.set_ylim(df_h['fair_value'].min() - 30, df_h['fair_value'].max() + 30)
    ax_bot.set_ylim(df_h['gold_price'].min() - 15, df_h['gold_price'].max() + 15)

    # Broken Axis Styling
    ax_top.spines['bottom'].set_visible(False)
    ax_bot.spines['top'].set_visible(False)
    ax_bot.xaxis.tick_bottom()

    # Titles & Legend
    ax_top.set_title("Pure Sentiment Backtest: Predictive Lead (Top) vs. Spot Price (Bottom)", fontsize=16, pad=20)
    ax_bot.legend(loc='lower center', bbox_to_anchor=(0.5, -0.2), ncol=4)

    ax_bot.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300, bbox_inches='tight')
    print("🏁 Pure sentiment precision chart generated.")

if __name__ == "__main__":
    generate_pure_sentiment_split_axis()
