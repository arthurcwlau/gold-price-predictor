import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

def generate_plotly_backtest(file_name="gold_investment_pro.csv"):
    if not os.path.exists(file_name):
        print(f"❌ {file_name} missing.")
        return

    # 1. Load Data
    df = pd.read_csv(file_name)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').replace(0, pd.NA).ffill()

    # 2. Math (Same as before)
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

    # 3. Create the Plotly Figure
    # We use subplots to separate the 'Sky-High' predictions from the 'Floor' price
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03,
                        subplot_titles=("June 2026 Sentiment Lead", "Actual Gold Spot Price"))

    # Add Prediction Lines (Top)
    colors = {2: '#39FF14', 6: '#FF8C00', 12: '#00BFFF'}
    for h in [2, 6, 12]:
        fig.add_trace(go.Scatter(x=df['date'], y=df['fair_value'].shift(h),
                                 name=f'{h}h Sentiment',
                                 line=dict(color=colors[h], width=1.5)), row=1, col=1)

    # Add Actual Price (Bottom)
    fig.add_trace(go.Scatter(x=df['date'], y=df['gold_price'],
                             name='Actual Spot',
                             line=dict(color='#FFD700', width=3)), row=2, col=1)

    # 4. Styling (The "Pleasing" Part)
    fig.update_layout(
        template="plotly_dark",
        title_text="Tactical Gold Backtest: Pure Sentiment Precision",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=80, b=20),
        height=800
    )

    # Standardize the Y-Axes to zoom in on the action
    fig.update_yaxes(title_text="USD", row=1, col=1)
    fig.update_yaxes(title_text="USD", row=2, col=1)

    # 5. Save for GitHub
    # This saves a static image for your README
    fig.write_image("gold_multi_horizon_backtest.png", scale=2)
    
    # Optional: Save as interactive HTML (GitHub Pages can host this!)
    # fig.write_html("index.html") 

    print("🏁 Plotly 'Premium' chart generated.")

if __name__ == "__main__":
    generate_plotly_backtest()
