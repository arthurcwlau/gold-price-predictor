import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os

def generate_backtest_report(file_name="gold_investment_pro (40).csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')

    # 2. Calculate Market Implied Fair Value
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    weighted_sum = sum(df[col] * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    df['pred_now'] = weighted_sum / total_prob

    # 3. Regularize Time Series (Hourly)
    df_h = df[['gold_price', 'pred_now']].resample('1H').mean().ffill()

    # 4. BACKTEST LOGIC: Compare Today's Price vs. Prediction from X hours ago
    # This aligns the 'Target' with the 'Forecast' made in the past
    lags = [12, 24, 48]
    metrics = {}

    for lag in lags:
        # What we predicted X hours ago for 'Right Now'
        df_h[f'forecast_{lag}h'] = df_h['pred_now'].shift(lag)
        # Error = Actual Price - What we predicted back then
        df_h[f'error_{lag}h'] = df_h['gold_price'] - df_h[f'forecast_{lag}h']
        
        # Stats for Dashboard
        mae = df_h[f'error_{lag}h'].abs().mean()
        # Hit Rate: Did the prediction correctly guess if price would be > or < than start?
        # (Requires comparing forecast to price AT the time of forecast)
        price_at_forecast = df_h['gold_price'].shift(lag)
        correct_dir = ((df_h[f'forecast_{lag}h'] > price_at_forecast) == 
                       (df_h['gold_price'] > price_at_forecast)).mean()
        metrics[lag] = {'mae': mae, 'hit_rate': correct_dir}

    # 5. Plotting (2 Subplots: Price & Error)
    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True, 
                                   gridspec_kw={'height_ratios': [3, 1]})

    # --- TOP PLOT: Price vs Forecasts ---
    ax1.plot(df_h.index, df_h['gold_price'], label='Actual Spot Price', color='#FFD700', lw=3, zorder=5)
    colors = {12: '#00BFFF', 24: '#FF69B4', 48: '#ADFF2F'}
    
    for lag in lags:
        ax1.plot(df_h.index, df_h[f'forecast_{lag}h'], 
                 label=f'{lag}h Lead Forecast', color=colors[lag], alpha=0.7, ls='--')

    ax1.set_title("Gold Price vs. Market-Implied Predictions (Backtest)", fontsize=16, pad=20)
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(alpha=0.2)

    # --- BOTTOM PLOT: Forecast Error (Bias) ---
    for lag in lags:
        ax2.fill_between(df_h.index, df_h[f'error_{lag}h'], 0, 
                         color=colors[lag], alpha=0.2, label=f'{lag}h Error')
    
    ax2.axhline(0, color='white', lw=1, ls='-')
    ax2.set_ylabel("Error (USD)", fontsize=12)
    ax2.set_title("Prediction Bias (Actual - Forecast)", fontsize=12)

    # --- PERFORMANCE DASHBOARD ---
    stats_text = "📊 MODEL PERFORMANCE\n"
    for lag, m in metrics.items():
        stats_text += f"\n【{lag}h Horizon】\n Avg Error: ${m['mae']:.2f}\n Hit Rate: {m['hit_rate']:.1%}"
    
    plt.gcf().text(0.85, 0.5, stats_text, fontsize=11, color='white', 
                   bbox=dict(facecolor='#1f1f1f', alpha=0.8, edgecolor='#FFD700'))

    # Formatting
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.tight_layout()
    plt.subplots_adjust(right=0.83) # Make room for the dashboard
    
    plt.savefig("gold_performance_backtest.png", dpi=300)
    print("📈 Backtest report saved as gold_performance_backtest.png")

if __name__ == "__main__":
    generate_backtest_report()
