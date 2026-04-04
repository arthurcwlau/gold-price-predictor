import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
from pandas.tseries.holiday import USFederalHolidayCalendar

def generate_tactical_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load Data
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
    df['fair_value'] = (weighted_sum / total_prob).ffill()

    # 3. Market Hours & Filtering
    df_h = df[['gold_price', 'fair_value']].resample('h').mean().ffill()
    cal = USFederalHolidayCalendar()
    is_weekend = (df_h.index.weekday >= 5) | \
                 ((df_h.index.weekday == 4) & (df_h.index.hour >= 17)) | \
                 ((df_h.index.weekday == 6) & (df_h.index.hour < 18))
    is_holiday = df_h.index.strftime('%Y-%m-%d').isin(['2026-04-03', '2026-04-04'])
    is_closed = is_weekend | is_holiday

    # 4. Accuracy Logic: Predicted Change vs. Actual Change
    horizons = [2, 6, 12]
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF'}
    metrics = {}

    for h in horizons:
        # Forecast is what Fair Value was H hours ago
        df_h[f'forecast_{h}h'] = df_h['fair_value'].shift(h)
        
        # We only judge during open market hours
        open_data = df_h[~is_closed].copy()
        
        # THE FIX: Compare MOVES, not absolute prices
        # Actual Move: Price(Now) - Price(H hours ago)
        # Predicted Move: FairValue(H hours ago) - Price(H hours ago)
        actual_move = open_data['gold_price'] - open_data['gold_price'].shift(h)
        predicted_move = open_data[f'forecast_{h}h'] - open_data['gold_price'].shift(h)
        
        valid = (actual_move.dropna().index).intersection(predicted_move.dropna().index)
        if not valid.empty:
            # Hit Rate: Did they move in the same direction?
            hits = (np.sign(actual_move.loc[valid]) == np.sign(predicted_move.loc[valid])).mean()
            # Residual: Average distance between predicted move and actual move
            error = (actual_move.loc[valid] - predicted_move.loc[valid]).abs().mean()
            metrics[h] = {'mae': error, 'hit': hits}
        else:
            metrics[h] = {'mae': 0, 'hit': 0.5}

    # 5. Plotting
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 11), sharex=True, gridspec_kw={'height_ratios': [3, 1]})

    # Shading
    closed_indices = df_h.index[is_closed]
    if not closed_indices.empty:
        diff = pd.Series(closed_indices).diff() > pd.Timedelta(hours=1)
        for _, group in pd.Series(closed_indices).groupby(diff.cumsum()):
            ax1.axvspan(group.iloc[0], group.iloc[-1], color='#222222', alpha=0.8)
            ax2.axvspan(group.iloc[0], group.iloc[-1], color='#222222', alpha=0.8)

    # Lines
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Gold Spot', color='#FFD700', lw=4, zorder=10)
    for h in horizons:
        # To make the graph readable, we normalize the forecast to the current price scale
        # We show "Price + Predicted Move"
        display_line = df_h['gold_price'].shift(h) + (df_h[f'forecast_{h}h'] - df_h['gold_price'].shift(h))
        ax1.plot(df_h.index, display_line, label=f'{h}h Lead', color=colors[h], lw=2, ls='--')

    ax1.set_title("Tactical Backtest: Directional Change Accuracy", fontsize=16)
    ax1.legend(loc='upper left', ncol=4)

    # Bottom Residual Plot (2h Error)
    res_2h = (df_h['gold_price'] - df_h['gold_price'].shift(2)) - (df_h['forecast_2h'] - df_h['gold_price'].shift(2))
    ax2.fill_between(df_h.index, res_2h.fillna(0), 0, color='#39FF14', alpha=0.2)
    ax2.set_ylabel("Move Residual")

    # Stats Box
    stats_text = "📊 CHANGE-BASED STATS"
    for h in horizons:
        m = metrics[h]
        stats_text += f"\n\n【{h}H Horizon】\n Acc: {m['hit']:.1%}\n Error: ${m['mae']:.2f}"
    
    plt.gcf().text(0.86, 0.5, stats_text, fontsize=11, color='white', verticalalignment='center',
                   bbox=dict(facecolor='#121212', alpha=0.9, edgecolor='#FFD700', boxstyle='round,pad=1'))

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.tight_layout()
    plt.subplots_adjust(right=0.85)
    plt.savefig("gold_multi_horizon_backtest.png", dpi=300)
    print("🏁 Logic-corrected backtest generated.")

if __name__ == "__main__":
    generate_tactical_backtest()
