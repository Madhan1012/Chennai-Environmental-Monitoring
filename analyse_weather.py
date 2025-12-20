import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password=os.getenv("DB_PASSWORD"))

# 1. Fetch data from DB
query = "SELECT station_name, pollutant, value, measured_at FROM chennai_measurements"
df = pd.read_sql(query, conn)

# 2. Reshape data so we can compare pollutants side-by-side
# We want columns: [station_name, measured_at, pm25, temperature, humidity]
df_pivot = df.pivot_table(index=['station_name', 'measured_at'], columns='pollutant', values='value').reset_index()

# 3. Create a correlation plot
if 'pm25' in df_pivot.columns and 'humidity' in df_pivot.columns:
    plt.figure(figsize=(10, 6))
    sns.regplot(data=df_pivot, x='humidity', y='pm25', color='teal')
    plt.title("Chennai: Humidity vs PM2.5 (How moisture traps dust)")
    plt.xlabel("Humidity (%)")
    plt.ylabel("PM2.5 (µg/m³)")
    plt.savefig("weather_impact_analysis.png")
    print("📈 Analysis saved as 'weather_impact_analysis.png'")
else:
    print("⚠️ Not enough data yet. Run fetch_data.py a few times to get both weather and AQI.")