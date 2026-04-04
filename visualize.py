import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
from pandas.tseries.holiday import USFederalHolidayCalendar

def generate_fused_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Clean Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()
    df = df.replace(0, np.nan).ffill().bfill()

    # 2. Calculate BASE Polymarket Fair Value
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    df['poly_fair_value'] = (weighted_sum / total_prob).ffill()

    # 3. FUSION ENGINE: Add External Factors (Confirmations)
    # We create a 'Macro Multiplier' - Neutral is 1.0
    macro_multiplier = 1.0
    
    # Factor A: US Dollar Index (DXY) - Gold's Enemy
    if 'dxy_index' in df.columns:
        # If DXY goes UP, Gold goes DOWN (Inverse relationship)
        dxy_change = df['dxy_index'].pct_change().fillna(0)
        macro_multiplier -= (dxy_change * 2.0) # Weight of 2.0x
        
    # Factor B: VIX Index - Fear is Gold's Friend
    if 'vix_index' in df.columns:
        vix_change = df['vix_index'].pct_change().fillna(0)
        macro_multiplier += (vix_change * 0.5) # Fear boosts Gold

    # Factor C: News Sentiment (Alpha Vantage / NYT)
    if 'news_sentiment_score' in df.columns:
        # Sentiment is usually -1.0 to 1.0. We add a small boost/drag.
        macro_multiplier += (df['news_sentiment_score'] * 0.05)

    # Calculate FUSED Prediction
    df['fused_prediction'] = df['poly_fair_value'] * macro_multiplier

    # 4. Backtest Horizons (2h, 6h, 12h)
    horizons = [2, 6, 12]
    for h in horizons:
        df[f'forecast_{h}h'] = df['fused_prediction'].shift(h)

    # 5. Resample and Plotting
    df_h = df.resample('h').mean().ffill()
    is_closed = (df_h.index.weekday >= 5) | (df_h.index.strftime('%Y-%m-%d').isin(['2026-04-03', '2026-04-04']))

    plt.style.use('dark_background')
    fig, (ax_top, ax_bot) = plt.subplots(2, 1, sharex=True, figsize=(15, 10), 
                                         gridspec_kw={'height_ratios': [1, 1]})
    fig.subplots_adjust(hspace=0.05)

    # TOP: FUSED PREDICTIONS
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF'}
    for h in horizons:
        ax_top.plot(df_h.index, df_h[f'forecast_{h}h'], label=f'{h}h Fused Lead', 
                    color=colors[h], lw=1.2)

    # BOTTOM: ACTUAL PRICE
    ax_bot.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', 
                color='#FFD700', lw=3.5, zorder=10)

    # Styling & Shading
    for ax in [ax_top, ax_bot]:
        ax.axvspan(df_h.index[is_closed][0], df_h.index[is_closed][-1], color='#1a1a1a', alpha=1.0, zorder=1)
        ax.grid(alpha=0.1)

    ax_top.set_title("Multi-Factor Fusion: Sentiment + Macro (DXY/VIX/News)", fontsize=16, pad=20)
    ax_bot.set_ylim(df_h['gold_price'].min() - 20, df_h['gold_price'].max() + 20)
    ax_bot.legend(loc='lower center', bbox_to_anchor=(0.5, -0.2), ncol=4)

    plt.savefig("gold_multi_horizon_backtest.png", dpi=300, bbox_inches='tight')
    print("🏁 Multi-factor fused chart generated.")

if __name__ == "__main__":
    generate_fused_backtest()
