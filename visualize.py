import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def generate_accuracy_chart(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load and Prepare Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # 2. Calculate Market Implied Fair Value (The Prediction)
    # Midpoints based on your tiering logic
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

    # Find which columns exist in your CSV
    active_tiers = [c for c in tier_midpoints.keys() if c in df.columns]
    
    # Calculate weighted average price for each row
    def calculate_fair_value(row):
        total_prob = row[active_tiers].sum()
        if total_prob == 0: return None
        weighted_sum = sum(row[col] * tier_midpoints[col] for col in active_tiers)
        return weighted_sum / total_prob

    df['predicted_price'] = df.apply(calculate_fair_value, axis=1)

    # 3. Create the Accuracy Plot
    plt.switch_backend('Agg') # Headless mode for GitHub Actions
    plt.figure(figsize=(12, 7))
    
    # Plot Actual Gold Price
    plt.plot(df['date'], df['gold_price'], label='Actual Gold Price (Spot)', 
             color='#FFD700', linewidth=3, zorder=3)
    
    # Plot Predicted Gold Price (Sentiment Implied)
    plt.plot(df['date'], df['predicted_price'], label='Market Implied Prediction (Polymarket)', 
             color='#00BFFF', linestyle='--', linewidth=2, zorder=2)

    # Shade the gap (The Error/Inaccuracy)
    plt.fill_between(df['date'], df['gold_price'], df['predicted_price'], 
                     color='gray', alpha=0.15, label='Prediction Gap')

    # Formatting
    plt.title("Gold Price vs. Market Prediction Accuracy", fontsize=16, fontweight='bold')
    plt.ylabel("Price (USD)")
    plt.xlabel("Time (UTC)")
    plt.legend(loc='best')
    plt.grid(alpha=0.3)
    
    # Clean up X-Axis (Date format)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=30)
    
    # Save Output
    plt.tight_layout()
    plt.savefig("gold_accuracy_report.png", dpi=300)
    print("📈 Accuracy chart generated: gold_accuracy_report.png")

if __name__ == "__main__":
    generate_accuracy_chart()
