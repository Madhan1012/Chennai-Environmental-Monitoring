import psycopg2
import folium
import json
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")


def calculate_risk_dashboard():
    try:
        conn = psycopg2.connect(
            host="localhost", database="postgres", user="postgres", password=DB_PASS
        )

        # 1. Complex Query: Get PM2.5 and Windspeed per Zone
        # This joins your geographic wards with your atmospheric readings
        query = """
        SELECT 
            w."zone name" as zone_name, 
            AVG(CASE WHEN m.pollutant = 'pm25' THEN m.value END) as avg_pm25,
            AVG(CASE WHEN m.pollutant = 'wind_speed' THEN m.value END) as avg_wind
        FROM chennai_wards w
        JOIN chennai_measurements m ON ST_Contains(w.wkb_geometry, m.geom)
        GROUP BY w."zone name";
        """
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            print("⚠️ No spatial data found. Ensure sensors are inside zone boundaries!")
            return

        # 2. Risk Logic (The Scientific Aspect)
        # We calculate a score out of 100. (PM2.5 of 60 = 100% of Indian Daily Limit)
        df["risk_score"] = (df["avg_pm25"] / 60) * 100

        def get_risk_meta(row):
            score = row["risk_score"]
            wind = row["avg_wind"] if row["avg_wind"] else 0

            if score > 100:
                return {
                    "level": "High",
                    "color": "#d73027",
                    "advice": "Wear N95 masks; limit outdoor exercise.",
                }
            elif score > 50:
                return {
                    "level": "Moderate",
                    "color": "#fee08b",
                    "advice": "Sensitive groups should reduce heavy exertion.",
                }
            else:
                return {
                    "level": "Low",
                    "color": "#1a9850",
                    "advice": "Air quality is good. Enjoy outdoor activities.",
                }

        # Apply the logic to create new columns
        risk_details = df.apply(get_risk_meta, axis=1, result_type="expand")
        df = pd.concat([df, risk_details], axis=1)

        # 3. Build the Map
        m = folium.Map(location=[13.08, 80.27], zoom_start=11, tiles="cartodbpositron")

        with open("chennai_wards.geojson", "r") as f:
            geo_data = json.load(f)

        # 4. Add the Risk Layer
        choropleth = folium.Choropleth(
            geo_data=geo_data,
            data=df,
            columns=["zone_name", "risk_score"],
            key_on="feature.properties.Zone Name",
            fill_color="RdYlGn_r",  # Red-Yellow-Green (Reversed so Red = High Risk)
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name="Health Risk Score (Based on Indian AQI Standards)",
        ).add_to(m)

        # 5. Add Advanced Tooltips
        # Convert df to dictionary for faster lookups in the GeoJSON loop
        data_dict = df.set_index("zone_name").to_dict("index")

        for feature in choropleth.geojson.data["features"]:
            name = feature["properties"]["Zone Name"]
            if name in data_dict:
                info = data_dict[name]
                feature["properties"]["Risk"] = (
                    f"{info['level']} ({info['risk_score']:.1f}%)"
                )
                feature["properties"]["PM25"] = f"{info['avg_pm25']:.1f} µg/m³"
                feature["properties"]["Wind"] = (
                    f"{info['avg_wind']:.1f} m/s" if info["avg_wind"] else "No Data"
                )
                feature["properties"]["Action"] = info["advice"]
            else:
                feature["properties"]["Risk"] = "Unknown (No Sensors)"
                feature["properties"]["PM25"] = "N/A"
                feature["properties"]["Action"] = "Monitor installation recommended."

        folium.GeoJsonTooltip(
            fields=["Zone Name", "Risk", "PM25", "Wind", "Action"],
            aliases=[
                "Zone:",
                "Risk Level:",
                "Avg PM2.5:",
                "Avg Wind:",
                "Recommendation:",
            ],
            style="background-color: #f9f9f9; border: 2px solid black; border-radius: 3px; font-weight: bold;",
        ).add_to(choropleth.geojson)

        m.save("chennai_risk_assessment.html")
        print("✅ Master Risk Map created: chennai_risk_assessment.html")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    calculate_risk_dashboard()

