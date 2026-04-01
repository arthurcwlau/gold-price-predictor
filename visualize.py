import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def generate_advanced_accuracy_chart(file_name="gold_investment_pro (26).csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')

    # 2. Calculate Market Implied Fair Value (The Prediction)
    tier_midpoints = {
        "gold_3_800_prob": 3600.0, "gold_3_800_4_200_prob": 4000.0,
        "gold_4_200_4_600_prob": 4400.0, "gold_4_600_5_000_prob": 4800.0,
        "gold_5_000_5_400_prob": 5200.0, "gold_5_400_5_800_prob": 5600.0,
        "gold_5_800_6_200_prob": 6000.0, "gold_6_200_prob": 6400.0,
    }
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    
    # Weighted average calculation
    weighted_sum = sum(df[col] * tier_midpoints[col] for col in active_tiers)
    total_prob = df[active_tiers].sum(axis=1)
    df['pred_now'] = weighted_sum / total_prob

    # 3. Regularize Time Series (Hourly) to enable accurate shifts
    # We use 'mean' to aggregate multiple snapshots within the same hour
    df_hourly = df[['gold_price', 'pred_now']].resample('1H').mean().ffill()

    # 4. Generate Lags (The "Past" Predictions)
    df_hourly['pred_6h'] = df_hourly['pred_now'].shift(6)
    df_hourly['pred_12h'] = df_hourly['pred_now'].shift(12)
    df_hourly['pred_24h'] = df_hourly['pred_now'].shift(24)
    df_hourly['pred_48h'] = df_hourly['pred_now'].shift(48)

    # 5. Create the Plot
    plt.style.use('dark_background')
    plt.figure(figsize=(14, 8))
    
    # Actual Price (The Benchmark)
    plt.plot(df_hourly.index, df_hourly['gold_price'], label='Actual Spot Price', 
             color='#FFD700', linewidth=3, zorder=5)
    
    # Lagged Predictions
    plt.plot(df_hourly.index, df_hourly['pred_6h'], label='Prediction (6h ago)', color='#00BFFF', alpha=0.8)
    plt.plot(df_hourly.index, df_hourly['pred_12h'], label='Prediction (12h ago)', color='#ADFF2F', alpha=0.7)
    plt.plot(df_hourly.index, df_hourly['pred_24h'], label='Prediction (24h ago)', color='#FF69B4', alpha=0.6)
    plt.plot(df_hourly.index, df_hourly['pred_48h'], label='Prediction (48h ago)', color='#F08080', alpha=0.5, linestyle='--')

    # Formatting
    plt.title("Gold Price vs. Historical Market Conviction", fontsize=16, fontweight='bold', pad=20)
    plt.ylabel("USD per Ounce", fontsize=12)
    plt.legend(frameon=True, facecolor='black', edgecolor='white')
    plt.grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.3)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    
    plt.tight_layout()
    plt.savefig("gold_lag_analysis.png", dpi=300)
    print("📈 Lagged prediction chart saved as gold_lag_analysis.png")

if __name__ == "__main__":
    generate_advanced_accuracy_chart()
