import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
from pandas.tseries.holiday import USFederalHolidayCalendar

def generate_full_backtest_suite(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()

    # 2. Base Sentiment Prediction (Weighted Tiers)
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    df['base_pred'] = (weighted_sum / total_prob).replace([np.inf, -np.inf], np.nan).ffill()

    # 3. Multi-Factor Refinement
    # Incorporates USD and VIX from your CSV to sharpen the Polymarket guess
    macro_multiplier = 1.0
    if 'usd_etf' in df.columns: 
        macro_multiplier -= (df['usd_etf'].pct_change().fillna(0) * 0.5)
    if 'vix_index' in df.columns: 
        macro_multiplier += (df['vix_index'].pct_change().fillna(0) * 0.1)
    
    df['refined_pred'] = (df['base_pred'] * macro_multiplier).ffill()

    # 4. Market Closure Detection (Fixing Apr 3-4 shading)
    df_h_raw = df[['gold_price', 'refined_pred']].resample('h').mean()
    
    # Identify Closures: Standard Weekends + Good Friday (Apr 3, 2026)
    # We also check if the price was physically missing in the original data
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=df_h_raw.index.min(), end=df_h_raw.index.max())
    
    is_weekend = (df_h_raw.index.weekday >= 5) | \
                 ((df_h_raw.index.weekday == 4) & (df_h_raw.index.hour >= 17)) | \
                 ((df_h_raw.index.weekday == 6) & (df_h_raw.index.hour < 18))
    
    # Explicitly add Good Friday 2026
    is_holiday = df_h_raw.index.normalize().isin(holidays) | \
                 (df_h_raw.index.strftime('%Y-%m-%d') == '2026-04-03')
                 
    is_closed = is_weekend | is_holiday | df_h_raw['gold_price'].isna()

    # 5. Mult-Horizon Shifts (6h, 12h, 24h, 48h)
    df_h = df_h_raw.ffill()
    horizons = [6, 12, 24, 48]
    colors = {6: '#00BFFF', 12: '#00FFFF', 24: '#FF00FF', 48: '#32CD32'}
    metrics = {}

    for h in horizons:
        col_name = f'forecast_{h}h'
        df_h[col_name] = df_h['refined_pred'].shift(h)
        
        # Calculate Stats for valid (open market) hours
        valid = df_h[~is_closed].dropna(subset=[col_name, 'gold_price'])
        if not valid.empty:
            mae = (valid['gold_price'] - valid[col_name]).abs().mean()
            prev_p = valid['gold_price'].shift(h)
            hit_rate = ((valid[col_name] > prev_p) == (valid['gold_price'] > prev_p)).mean()
            metrics[h] = {'mae': mae, 'hit': hit_rate}
        else:
            metrics[h] = {'mae': 0, 'hit': 0}

    # 6. Plotting
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 11), sharex=True, 
                                   gridspec_kw={'height_ratios': [3, 1]})

    # Shade Market Closures (Grey blocks)
    closed_indices = df_h.index[is_closed]
    if not closed_indices.empty:
        # Find contiguous blocks of closed time to draw spans
        diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
        group_ids = diff.cumsum()
        for i, group in pd.Series(closed_indices).groupby(group_ids):
            start, end = group.iloc[0], group.iloc[-1]
            ax1.axvspan(start, end, color='#2c2c2c', alpha=0.7, label='Market Closed' if i == 0 else "")
            ax2.axvspan(start, end, color='#2c2c2c', alpha=0.7)

    # Actual Price
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', color='#FFD700', lw=5, zorder=10)
    
    # Plot all 4 Forecast Lines
    for h in horizons:
        ax1.plot(df_h.index, df_h[f'forecast_{h}h'], label=f'{h}h Horizon', 
                 color=colors[h], lw=1.5, ls='--', alpha=0.8)

    ax1.set_title("Gold Multi-Horizon Backtest: Reality vs. Multi-Factor Conviction", fontsize=18, pad=20)
    ax1.legend(loc='upper left', ncol=2, frameon=True, facecolor='black', edgecolor='white')
    ax1.grid(alpha=0.1)

    # Error Plot (Showing 24h bias by default)
    ax2.fill_between(df_h.index, (df_h['gold_price'] - df_h['forecast_24h']), 0, 
                     color='#FF00FF', alpha=0.2, label='24h Prediction Bias')
    ax2.axhline(0, color='white', lw=1, alpha=0.5)
    ax2.set_ylabel("Error (USD)")

    # Performance Dashboard Overlay
    stats_text = "📊 STRATEGY DASHBOARD"
    for h in horizons:
        m = metrics[h]
        stats_text += f"\n\n【{h}H Window】\n Acc: {m['hit']:.1%}\n Bias: ${m['mae']:.2f}"
    
    plt.gcf().text(0.86, 0.5, stats_text, fontsize=10, color='white', verticalalignment='center',
                   bbox=dict(facecolor='#121212', alpha=0.9, edgecolor='#FFD700', boxstyle='round,pad=1'))

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.tight_layout()
    plt.subplots_adjust(right=0.85)
    
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300)
    print("🏁 Multi-horizon backtest report generated.")

if __name__ == "__main__":
    generate_full_backtest_suite()
