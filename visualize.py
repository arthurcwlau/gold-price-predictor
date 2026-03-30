import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def generate_momentum_chart(file_name="gold_investment_pro.csv"):
    # 1. Check if the data file exists first
    if not os.path.exists(file_name):
        print(f"❌ Error: {file_name} not found. Run the scraper first!")
        return

    # 2. Load and prepare data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    
    # Take the last 7 days (168 hours)
    df_recent = df.tail(168)
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    plt.subplots_adjust(hspace=0.3)

    # --- TOP PLOT: Probabilities ---
    prob_cols = [c for c in df.columns if c.endswith('_prob')]
    for col in prob_cols:
        label = col.replace('_prob', '').replace('_', ' ').title()
        ax1.plot(df_recent['date'], df_recent[col], label=f"{label} %", linewidth=2)
    
    ax1.set_title("Macro Event Probabilities (Last 7 Days)", fontsize=14, fontweight='bold')
    ax1.set_ylabel("Probability (%)")
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(alpha=0.3)

    # --- BOTTOM PLOT: Gold Velocity ---
    # Find the gold velocity column dynamically
    gold_cols = [c for c in df.columns if 'gold' in c and 'velocity' in c and 'ma6' not in c]
    
    if gold_cols:
        gold_vel_col = gold_cols[0]
        gold_ma_col = gold_vel_col + "_ma6"
        
        ax2.fill_between(df_recent['date'], df_recent[gold_vel_col], color='gold', alpha=0.3, label="Gold Velocity")
        if gold_ma_col in df_recent.columns:
            ax2.plot(df_recent['date'], df_recent[gold_ma_col], color='darkgoldenrod', linestyle='--', label="6hr Moving Avg")
        
        # Highlight signals
        signal_col = gold_vel_col.replace('velocity', 'signal')
        if signal_col in df_recent.columns:
            ax2.fill_between(df_recent['date'], df_recent[gold_vel_col].min(), df_recent[gold_vel_col].max(), 
                             where=(df_recent[signal_col] == 1), color='green', alpha=0.1, label="Bullish Momentum")

    ax2.set_title("Gold Prediction Momentum (Velocity vs MA6)", fontsize=14, fontweight='bold')
    ax2.set_ylabel("Velocity Delta")
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(alpha=0.3)

    # Formatting dates
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    plt.xticks(rotation=45)
    
    # Save the output
    plt.savefig("momentum_report.png", dpi=300, bbox_inches='tight')
    print("📈 Momentum report generated: momentum_report.png")

if __name__ == "__main__":
    generate_momentum_chart()
