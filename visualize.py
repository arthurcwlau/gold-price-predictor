import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def generate_momentum_chart(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df_recent = df.tail(48) # 48 hours is better for a "Pulse" view

    plt.switch_backend('Agg') # Headless mode for GitHub
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    plt.subplots_adjust(hspace=0.3)

    # --- TOP PLOT: Filter for meaningful probabilities (>5%) ---
    prob_cols = [c for c in df.columns if c.endswith('_prob')]
    for col in prob_cols:
        if df_recent[col].max() > 5: # Only plot if there's a >5% chance
            label = col.replace('_prob', '').replace('_', ' ').title()
            ax1.plot(df_recent['date'], df_recent[col], label=f"{label} %", linewidth=2)
    
    ax1.set_title("Key Macro Probabilities (Last 48 Hours)", fontsize=14, fontweight='bold')
    ax1.set_ylabel("Probability (%)")
    ax1.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=8) # Legend outside
    ax1.grid(alpha=0.3)

    # --- BOTTOM PLOT: Gold Momentum ---
    # Pick the bin with the highest current probability to track velocity
    current_probs = {c: df_recent[c].iloc[-1] for c in prob_cols if 'gold' in c}
    top_gold_bin = max(current_probs, key=current_probs.get).replace('_prob', '')
    
    vel_col = f"{top_gold_bin}_velocity"
    ma_col = f"{top_gold_bin}_velocity_ma6"
    sig_col = f"{top_gold_bin}_signal"

    if vel_col in df.columns:
        ax2.fill_between(df_recent['date'], df_recent[vel_col], color='gold', alpha=0.3, label=f"{top_gold_bin} Vel")
        ax2.plot(df_recent['date'], df_recent[ma_col], color='darkgoldenrod', linestyle='--', label="6hr Avg")
        
        # Bullish Highlight
        ax2.fill_between(df_recent['date'], df_recent[vel_col].min(), df_recent[vel_col].max(), 
                         where=(df_recent[sig_col] == 1), color='green', alpha=0.1)

    ax2.set_title(f"Momentum: {top_gold_bin.replace('_', ' ').title()}", fontsize=14, fontweight='bold')
    ax2.set_ylabel("Velocity")
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(alpha=0.3)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.xticks(rotation=45)
    plt.savefig("momentum_report.png", dpi=300, bbox_inches='tight')
    print("📈 Fresh chart saved as PNG.")

if __name__ == "__main__":
    generate_momentum_chart()
