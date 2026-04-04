import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
from pandas.tseries.holiday import USFederalHolidayCalendar

def generate_truth_map(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date').copy()

    # 2. Calculate the "Prediction" (Market Fair Value)
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    weighted_sum = sum(df[col].fillna(0) * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    
    # This is the 'Fair Value' generated every hour
    df['raw_prediction'] = (weighted_sum / total_prob).replace([np.inf, -np.inf], np.nan).ffill()

    # 3. Time-Shift Logic (The Backtest)
    # We take the prediction from X hours ago and align it with the price NOW
    horizons = [2, 6, 12]
    for h in horizons:
        df[f'forecast_{h}h'] = df['raw_prediction'].shift(h)

    # 4. Market Status Detection (For Accurate Stats)
    df_h = df.resample('h').mean()
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(start=df_h.index.min(), end=df_h.index.max())
    is_weekend = (df_h.index.weekday >= 5) | \
                 ((df_h.index.weekday == 4) & (df_h.index.hour >= 17)) | \
                 ((df_h.index.weekday == 6) & (df_h.index.hour < 18))
    is_holiday = df_h.index.normalize().isin(holidays) | \
                 (df_h.index.strftime('%Y-%m-%d').isin(['2026-04-03', '2026-04-04']))
    is_closed = is_weekend | is_holiday | df_h['gold_price'].isna()

    # 5. Accuracy Metrics (Open Market History Only)
    metrics = {}
    for h in horizons:
        # Check direction: Did the move in 'Fair Value' match the move in 'Price'?
        valid = df_h[~is_closed].dropna(subset=[f'forecast_{h}h', 'gold_price'])
        if not valid.empty:
            # We compare the change in price vs. the change suggested by the forecast
            price_prev = df_h['gold_price'].shift(h).loc[valid.index]
            actual_move = valid['gold_price'] - price_prev
            pred_move = valid[f'forecast_{h}h'] - price_prev
            
            hit_rate = (np.sign(actual_move) == np.sign(pred_move)).mean()
            avg_error = (valid['gold_price'] - valid[f'forecast_{h}h']).abs().mean()
            metrics[h] = {'mae': avg_error, 'hit': hit_rate}
        else:
            metrics[h] = {'mae': 0, 'hit': 0.5}

    # 6. Plotting
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 11), sharex=True, gridspec_kw={'height_ratios': [3, 1]})

    # Shade Closed Periods
    closed_indices = df_h.index[is_closed]
    if not closed_indices.empty:
        diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
        for _, group in pd.Series(closed_indices).groupby(diff.cumsum()):
            ax1.axvspan(group.iloc[0], group.iloc[-1], color='#222222', alpha=0.9, label='Market Closed' if _ == 0 else "")
            ax2.axvspan(group.iloc[0], group.iloc[-1], color='#222222', alpha=0.9)

    # Actual Gold Price
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot (Reality)', color='#FFD700', lw=5, zorder=10)
    
    # Plot the Predictions made 2, 6, and 12 hours ago
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF'}
    for h in horizons:
        ax1.plot(df_h.index, df_h[f'forecast_{h}h'], label=f'Predicted {h}h ago', color=colors[h], lw=2, ls='--')

    ax1.set_title("Gold Truth-Map: Actual Price vs. Previous Predictions", fontsize=18, pad=20)
    ax1.legend(loc='upper left', ncol=4, frameon=True, facecolor='black', edgecolor='white')
    ax1.grid(alpha=0.1)

    # Residual Plot (2h Forecast Error)
    ax2.fill_between(df_h.index, (df_h['gold_price'] - df_h['forecast_2h']).fillna(0), 0, color='#39FF14', alpha=0.2)
    ax2.set_ylabel("Error ($)")
    ax2.axhline(0, color='white', lw=1, alpha=0.5)

    # Stats Dashboard
    stats_text = "📊 BACKTEST PERFORMANCE\n(Last 100 Active Hours)"
    for h in horizons:
        m = metrics[h]
        stats_text += f"\n\n【{h}H Lead】\n Acc: {m['hit']:.1%}\n Err: ${m['mae']:.0f}"
    
    plt.gcf().text(0.86, 0.5, stats_text, fontsize=11, color='white', verticalalignment='center',
                   bbox=dict(facecolor='#121212', alpha=0.9, edgecolor='#FFD700', boxstyle='round,pad=1'))

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.tight_layout()
    plt.subplots_adjust(right=0.85)
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300)
    print("🏁 Truth-map chart generated.")

if __name__ == "__main__":
    generate_truth_map()
