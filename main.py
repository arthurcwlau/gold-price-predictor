import requests
import pandas as pd
from datetime import datetime
import os

# Add this near the top of main.py
def get_actual_gold_price():
    # Using a simple public price API for Gold Spot (XAU)
    res = requests.get("https://api.accessprecision.com/v1/gold_spot").json() 
    return res['price']

# When saving to CSV, add the actual price:
new_data = pd.DataFrame([{
    "date": datetime.now().strftime("%Y-%m-%d"),
    "market": title,
    "poly_prob": prob,
    "actual_price": get_actual_gold_price()
}])

# 1. Ask Polymarket for Gold Markets
url = "https://gamma-api.polymarket.com/events?active=true&closed=false&q=Gold"
data = requests.get(url).json()

# 2. Extract the first gold market price (probability)
# This looks for the first "Yes" price it finds
try:
    market = data[0]['markets'][0]
    prob = market['outcomePrices'][0] # Price of 'Yes'
    title = market['question']
except:
    prob = 0
    title = "No active market found"

# 3. Create a new data row
new_data = pd.DataFrame([{
    "date": datetime.now().strftime("%Y-%m-%d"),
    "market": title,
    "probability": prob
}])

# 4. Save to a rolling CSV file
file_name = "gold_data.csv"
if os.path.exists(file_name):
    df = pd.read_csv(file_name)
    df = pd.concat([df, new_data]).tail(30) # Keep only last 30 days
else:
    df = new_data

df.to_csv(file_name, index=False)
print(f"Recorded {title} at {prob}")
