import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")

def run_master_analysis():
    try:
        conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password=DB_PASS)
        df_raw = pd.read_sql("SELECT station_name, pollutant, value, measured_at FROM chennai_measurements", conn)
        conn.close()

        if df_raw.empty:
            print("⚠️ No data! Run fetch_data.py first.")
            return

        # Pivot the data to get columns: [pm25, no2, temperature, wind_speed]
        df = df_raw.pivot_table(index=['station_name', 'measured_at'], columns='pollutant', values='value').reset_index()

        # 1. Chart: Temperature vs PM2.5
        if 'pm25' in df.columns and 'temperature' in df.columns:
            plt.figure(figsize=(10, 5))
            sns.regplot(data=df, x='temperature', y='pm25', color='orange')
            plt.title("Chennai: Temperature vs PM2.5")
            plt.savefig("analysis_temp_vs_pm25.png")
            print("✅ Created Temperature Chart")

        # 2. Chart: wind_speed vs PM2.5 (The "Cleansing" Effect)
        if 'pm25' in df.columns and 'wind_speed' in df.columns:
            plt.figure(figsize=(10, 5))
            sns.scatterplot(data=df, x='wind_speed', y='pm25', hue='station_name')
            plt.title("Chennai: How wind_speed Cleans the Air")
            plt.savefig("analysis_wind_vs_pm25.png")
            print("✅ Created wind_speed Chart")

        print("\n--- Summary Statistics ---")
        print(df[['pm25', 'no2', 'temperature', 'wind_speed']].describe().loc[['mean', 'max']])

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_master_analysis()