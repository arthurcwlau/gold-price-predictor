import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os

def generate_multi_factor_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing. Please ensure the file is in the root directory.")
        return

    # 1. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()

    # 2. Base Sentiment Prediction (Polymarket Tiers)
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, 
        "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, 
        "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, 
        "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, 
        "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    
    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    df['base_pred'] = (weighted_sum / total_prob).replace([np.inf, -np.inf], np.nan).ffill()

    # 3. Multi-Factor Refinement (Accuracy Engine)
    macro_multiplier = 1.0
    if 'usd_etf' in df.columns: 
        macro_multiplier -= (df['usd_etf'].pct_change().fillna(0) * 0.5)
    if 'vix_index' in df.columns: 
        macro_multiplier += (df['vix_index'].pct_change().fillna(0) * 0.1)
    
    df['refined_pred'] = (df['base_pred'] * macro_multiplier).ffill()

    # 4. Create Hourly DF & Identify "Closed" Gaps
    df_h_raw = df[['gold_price', 'refined_pred']].resample('h').mean()
    
    # Mark where market was closed (missing original price)
    is_closed = df_h_raw['gold_price'].isna()
    
    df_h = df_h_raw.ffill()
    horizon = 24
    df_h['forecast_lead'] = df_h['refined_pred'].shift(horizon)
    df_h['error'] = df_h['gold_price'] - df_h['forecast_lead']

    # Performance Stats
    valid = df_h.dropna(subset=['forecast_lead', 'gold_price'])
    mae = valid['error'].abs().mean() if not valid.empty else 0
    hit_rate = 0
    if not valid.empty:
        # Compare forecast direction vs reality direction
        prev_p = valid['gold_price'].shift(horizon)
        hit_rate = ((valid['forecast_lead'] > prev_p) == (valid['gold_price'] > prev_p)).mean()

    # 5. Plotting
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True, 
                                   gridspec_kw={'height_ratios': [3, 1]})

    # --- MARKING MARKET CLOSED PERIODS ---
    closed_indices = df_h.index[is_closed]
    if len(closed_indices) > 0:
        diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
        group_ids = diff.cumsum()
        for i, group in pd.Series(closed_indices).groupby(group_ids):
            start, end = group.iloc[0], group.iloc[-1]
            ax1.axvspan(start, end, color='#333333', alpha=0.5, label='Market Closed' if i == 0 else "")
            ax2.axvspan(start, end, color='#333333', alpha=0.5)

    # Reality vs Forecast
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Gold Price', color='#FFD700', lw=4, zorder=5)
    ax1.plot(df_h.index, df_h['forecast_lead'], label=f'Refined {horizon}h Forecast', 
             color='#00BFFF', lw=2, ls='--', alpha=0.8)

    ax1.set_title("Gold Multi-Factor Backtest: Reality vs. Forecast (Market Hours Highlighted)", fontsize=18, pad=20)
    ax1.legend(loc='upper left', frameon=True, facecolor='black', edgecolor='white')
    ax1.grid(alpha=0.1)

    # Error Plot (Bottom Chart)
    ax2.fill_between(df_h.index, df_h['error'], 0, color='#00BFFF', alpha=0.2)
    ax2.axhline(0, color='white', lw=1, alpha=0.5)
    ax2.set_ylabel("Error (USD)")

    # Accuracy Dashboard Overlay
    stats = (f"📈 ACCURACY DASHBOARD\n\n"
             f"Shaded = Market Closed\n"
             f"Horizon: {horizon} Hours\n"
             f"Directional Acc: {hit_rate:.1%}\n"
             f"Avg Error: ${mae:.2f}")
    
    plt.gcf().text(0.86, 0.5, stats, fontsize=11, color='white', verticalalignment='center',
                   bbox=dict(facecolor='#121212', alpha=0.9, edgecolor='#FFD700', boxstyle='round,pad=1'))

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.tight_layout()
    plt.subplots_adjust(right=0.85)
    
    plt.savefig("gold_multi_factor_backtest.png", dpi=300)
    print("🏁 Multi-factor backtest generated successfully.")

if __name__ == "__main__":
    generate_multi_factor_backtest()
