import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

# 1. Setup & Connection
load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")

def run_analysis():
    try:
        conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password=DB_PASS)
        
        # 2. Fetch the "Aspects"
        # We grab everything and pivot it so we have columns for PM25, Temp, etc.
        query = "SELECT station_name, pollutant, value, measured_at FROM chennai_measurements"
        df_raw = pd.read_sql(query, conn)
        conn.close()

        if df_raw.empty:
            print("⚠️ Database is empty. Run fetch_data.py first!")
            return

        # 3. Data Transformation (Pivoting)
        # This aligns the readings by station and time
        df = df_raw.pivot_table(index=['station_name', 'measured_at'], 
                                columns='pollutant', 
                                values='value').reset_index()

        # 4. Visualization: Temperature vs PM2.5
        if 'pm25' in df.columns and 'temperature' in df.columns:
            plt.figure(figsize=(10, 6))
            sns.set_style("whitegrid")
            
            # Create a regression plot (Shows the trend line)
            sns.regplot(data=df, x='temperature', y='pm25', 
                        scatter_kws={'alpha':0.5}, line_kws={'color':'red'})
            
            plt.title("Chennai Environmental Aspect: Temperature vs PM2.5")
            plt.xlabel("Temperature (°C)")
            plt.ylabel("PM2.5 (µg/m³)")
            
            output_file = "pollution_weather_correlation.png"
            plt.savefig(output_file)
            print(f"📈 Correlation chart saved as '{output_file}'")
        
        # 5. Print a Quick "Environmental Summary"
        print("\n--- Project Summary ---")
        print(f"Total Data Points Collected: {len(df_raw)}")
        if 'windspeed' in df.columns:
            avg_wind = df['windspeed'].mean()
            print(f"Average Wind Speed: {avg_wind:.2f} m/s")
            if avg_wind < 1.0:
                print("🚨 Insight: Low wind speeds are likely trapping pollution in the city.")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")

if __name__ == "__main__":
    run_analysis()