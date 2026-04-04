import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os

def generate_multi_factor_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()

    # 2. Base Sentiment Prediction
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
    macro_multiplier = 1.0
    if 'usd_etf' in df.columns: macro_multiplier -= (df['usd_etf'].pct_change().fillna(0) * 0.5)
    if 'vix_index' in df.columns: macro_multiplier += (df['vix_index'].pct_change().fillna(0) * 0.1)
    
    df['refined_pred'] = (df['base_pred'] * macro_multiplier).ffill()

    # 4. Create Hourly DF & Identify "Original" Gaps (Before ffill)
    df_h_raw = df[['gold_price', 'refined_pred']].resample('h').mean()
    
    # Identify where the market is closed (where original price was missing)
    # This captures weekends and the daily 1-hour settlement break.
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
        prev_p = valid['gold_price'].shift(horizon)
        hit_rate = ((valid['forecast_lead'] > prev_p) == (valid['gold_price'] > prev_p)).mean()

    # 5. Plotting
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True, 
                                   gridspec_kw={'height_ratios': [3, 1]})

    # --- MARKING MARKET CLOSED PERIODS ---
    # We find contiguous blocks of True in is_closed and shade them
    closed_indices = df_h.index[is_closed]
    if len(closed_indices) > 0:
        # Create groups of consecutive timestamps
        diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
        group_ids = diff.cumsum()
        for _, group in pd.Series(closed_indices).groupby(group_ids):
            start, end = group.iloc[0], group.iloc[-1]
            # Shade both top and bottom charts
            ax1.axvspan(start, end, color='#333333', alpha=0.5, label='Market Closed' if _ == 0 else "")
            ax2.axvspan(start, end, color='#333333', alpha=0.5)

    # Reality vs Forecast
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Gold Price', color='#FFD700', lw=4, zorder=5)
    ax1.plot(df_h.index, df_h['forecast_lead'], label=f'Refined {horizon}h Forecast', 
             color='#0
